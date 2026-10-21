"""管理設定の保存先と、本番起動時の秘密・権限境界を確認する。"""

import importlib.util
import json
from pathlib import Path
import stat
import sys
import warnings

import pytest
from werkzeug.security import check_password_hash


ROOT = Path(__file__).resolve().parents[1]
PASSWORD = "テスト専用管理パスワード-42"


def load_script(name):
    spec = importlib.util.spec_from_file_location(
        f"runtime_config_test_{name}", ROOT / "scripts" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def configure():
    return load_script("configure_admin")


@pytest.fixture
def runner():
    return load_script("run_server")


def supply_credentials(monkeypatch, configure, path, passwords):
    answers = iter(passwords)
    monkeypatch.setattr(sys, "argv", ["configure_admin.py", "--config", str(path)])
    monkeypatch.setattr("builtins.input", lambda _prompt: "管理者")
    monkeypatch.setattr(configure.getpass, "getpass", lambda _prompt: next(answers))


def run_configure(configure):
    # 対話入力の警告ポリシーを、ほかのテストへ持ち越さない。
    with warnings.catch_warnings():
        configure.main()


def test_configure_saves_private_hashed_credentials_without_printing_secrets(
    configure, tmp_path, monkeypatch, capsys
):
    path = tmp_path / "settings" / "admin.json"
    assert not path.parent.exists()
    supply_credentials(monkeypatch, configure, path, [PASSWORD, PASSWORD])

    run_configure(configure)

    raw = path.read_text(encoding="utf-8")
    config = json.loads(raw)
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert config["SHOGI_ADMIN_USER"] == "管理者"
    assert check_password_hash(config["SHOGI_ADMIN_PASSWORD_HASH"], PASSWORD)
    assert len(bytes.fromhex(config["SHOGI_SECRET_KEY"])) >= 32
    assert PASSWORD not in raw
    captured = capsys.readouterr()
    output = captured.out + captured.err
    for secret in (PASSWORD, config["SHOGI_ADMIN_PASSWORD_HASH"], config["SHOGI_SECRET_KEY"]):
        assert secret not in output


def test_configure_rejects_shared_directory_without_changing_its_permissions(
    configure, tmp_path, monkeypatch, capsys
):
    path = tmp_path / "shared" / "admin.json"
    path.parent.mkdir(mode=0o755)
    path.parent.chmod(0o755)
    supply_credentials(monkeypatch, configure, path, [PASSWORD, PASSWORD])

    with pytest.raises(SystemExit):
        run_configure(configure)

    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o755
    assert not path.exists()
    captured = capsys.readouterr()
    assert PASSWORD not in captured.out + captured.err


def test_configure_does_not_overwrite_existing_file_or_prompt(
    configure, tmp_path, monkeypatch
):
    path = tmp_path / "admin.json"
    original = b'{"existing": "must remain unchanged"}\n'
    path.write_bytes(original)
    monkeypatch.setattr(sys, "argv", ["configure_admin.py", "--config", str(path)])
    monkeypatch.setattr("builtins.input", lambda _prompt: pytest.fail("既存設定では入力しない"))
    monkeypatch.setattr(configure.getpass, "getpass", lambda _prompt: pytest.fail("既存設定では入力しない"))

    with pytest.raises(SystemExit):
        run_configure(configure)

    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "passwords", [("short", "short"), (PASSWORD, PASSWORD + "不一致")]
)
def test_configure_rejects_bad_passwords_before_creating_file(
    configure, tmp_path, monkeypatch, capsys, passwords
):
    path = tmp_path / "settings" / "admin.json"
    supply_credentials(monkeypatch, configure, path, passwords)

    with pytest.raises(SystemExit):
        run_configure(configure)

    assert not path.exists()
    captured = capsys.readouterr()
    assert all(password not in captured.out + captured.err for password in passwords)


def write_config(tmp_path):
    path = tmp_path / "admin.json"
    config = {
        "SHOGI_ADMIN_USER": "管理者",
        "SHOGI_ADMIN_PASSWORD_HASH": "test-only-hash-kept-out-of-command-line",
        "SHOGI_SECRET_KEY": "test-only-session-secret-never-use-in-production-42",
    }
    path.write_text(json.dumps(config), encoding="utf-8")
    path.chmod(0o600)
    return path, config


@pytest.mark.parametrize("check", [False, True])
def test_runner_uses_private_loopback_worker_and_secrets_only_in_environment(
    runner, tmp_path, monkeypatch, check
):
    path, config = write_config(tmp_path)
    arguments = ["run_server.py", "--config", str(path)]
    if check:
        arguments.append("--check")
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setenv("GUNICORN_CMD_ARGS", "--bind 0.0.0.0:9000 --workers 7")
    monkeypatch.setenv("FLASK_DEBUG", "1")
    for key in config:
        monkeypatch.setenv(key, "stale-inherited-value")
    calls = []
    monkeypatch.setattr(runner.os, "execve", lambda *args: calls.append(args))

    runner.main()

    assert len(calls) == 1
    executable, command, environment = calls[0]
    assert executable == sys.executable
    assert command[:3] == [sys.executable, "-m", "gunicorn"]
    assert command[-1] == "app:app"
    for option, expected in {
        "--config": "/dev/null",
        "--chdir": str(ROOT),
        "--bind": "127.0.0.1:5000",
        "--workers": "1",
        "--worker-class": "sync",
        "--threads": "1",
    }.items():
        assert command[command.index(option) + 1] == expected
    assert ("--check-config" in command) is check
    assert "GUNICORN_CMD_ARGS" not in environment
    assert "FLASK_DEBUG" not in environment
    assert {key: environment[key] for key in config} == config
    assert all(value not in " ".join(command) for value in config.values())


@pytest.mark.parametrize(
    "invalid",
    ["missing_file", "world_readable", "missing_user", "missing_hash", "missing_secret", "short_secret"],
)
def test_runner_rejects_insecure_or_incomplete_config_before_exec(
    runner, tmp_path, monkeypatch, invalid
):
    path, config = write_config(tmp_path)
    if invalid == "missing_file":
        path.unlink()
    elif invalid == "world_readable":
        path.chmod(0o644)
    else:
        if invalid == "short_secret":
            config["SHOGI_SECRET_KEY"] = "too-short"
        else:
            key = {
                "missing_user": "SHOGI_ADMIN_USER",
                "missing_hash": "SHOGI_ADMIN_PASSWORD_HASH",
                "missing_secret": "SHOGI_SECRET_KEY",
            }[invalid]
            del config[key]
        path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["run_server.py", "--config", str(path)])
    monkeypatch.setattr(runner.os, "execve", lambda *_args: pytest.fail("不正な設定では起動しない"))

    with pytest.raises(SystemExit):
        runner.main()
