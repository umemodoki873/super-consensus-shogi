"""管理設定を読み込み、localhost限定のGunicornで起動する。"""

import argparse
import json
import os
from pathlib import Path
import stat
import sys


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = Path.home() / ".config" / "super-consensus-shogi" / "admin.json"
CONFIG_KEYS = ("SHOGI_ADMIN_USER", "SHOGI_ADMIN_PASSWORD_HASH", "SHOGI_SECRET_KEY")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--check", action="store_true", help="設定とアプリの読み込みだけを検証")
    args = parser.parse_args()
    path = args.config.expanduser().absolute()
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise SystemExit("管理設定は実行ユーザー所有の通常ファイルにし、権限を600にしてください。")
        with path.open(encoding="utf-8") as stream:
            config = json.load(stream)
    except (OSError, ValueError):
        raise SystemExit("管理設定を読み込めません。先に scripts/configure_admin.py を実行してください。") from None
    if not isinstance(config, dict) or any(
        not isinstance(config.get(key), str) or not config[key] for key in CONFIG_KEYS
    ):
        raise SystemExit("管理設定が不足しています。設定ファイルの内容をチャットには貼らないでください。")
    if len(config["SHOGI_SECRET_KEY"]) < 32:
        raise SystemExit("管理セッション用の秘密が短すぎます。設定を作り直してください。")

    environment = os.environ.copy()
    environment.update({key: config[key] for key in CONFIG_KEYS})
    environment.pop("GUNICORN_CMD_ARGS", None)
    environment.pop("FLASK_DEBUG", None)
    command = [
        sys.executable, "-m", "gunicorn",
        "--config", "/dev/null",
        "--chdir", str(ROOT),
        "--bind", "127.0.0.1:5000",
        "--workers", "1",
        "--worker-class", "sync",
        "--threads", "1",
        "--timeout", "30",
        "--forwarded-allow-ips", "127.0.0.1",
        "--access-logfile", "-",
        "--error-logfile", "-",
        "--access-logformat", '%(h)s %(t)s "%(m)s %(U)s" %(s)s %(b)s',
    ]
    if args.check:
        command.append("--check-config")
    command.append("app:app")
    os.execve(sys.executable, command, environment)


if __name__ == "__main__":
    main()
