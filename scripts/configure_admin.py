"""管理者の秘密を対話入力し、リポジトリの外へ保存する。初回のみ実行。"""

import argparse
import getpass
import json
import os
from pathlib import Path
import secrets
import warnings

from werkzeug.security import generate_password_hash


DEFAULT_CONFIG = Path.home() / ".config" / "super-consensus-shogi" / "admin.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    path = args.config.expanduser().absolute()
    if path.exists() or path.is_symlink():
        raise SystemExit("管理設定は既にあります。既存の設定を上書きせず終了しました。")

    warnings.simplefilter("error", getpass.GetPassWarning)
    try:
        username = input("管理ユーザー名 [admin]: ").strip() or "admin"
        if len(username) > 128 or ":" in username or any(ord(c) < 32 for c in username):
            raise SystemExit("ユーザー名は128文字以内で、コロンや制御文字を含めないでください。")
        password = getpass.getpass("新しい管理パスワード（12文字以上・画面には表示されません）: ")
        repeated = getpass.getpass("確認のため、もう一度入力: ")
    except (EOFError, KeyboardInterrupt, getpass.GetPassWarning):
        raise SystemExit("非表示での入力を完了できませんでした。対話SSH画面から実行してください。") from None
    if not 12 <= len(password) <= 1024:
        raise SystemExit("パスワードは12〜1024文字にしてください。設定は保存していません。")
    if not secrets.compare_digest(password.encode("utf-8"), repeated.encode("utf-8")):
        raise SystemExit("入力が一致しません。設定は保存していません。")

    config = {
        "SHOGI_ADMIN_USER": username,
        "SHOGI_ADMIN_PASSWORD_HASH": generate_password_hash(password),
        "SHOGI_SECRET_KEY": secrets.token_hex(32),
    }
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.stat().st_mode & 0o077:
        raise SystemExit("保存先には、実行ユーザーだけがアクセスできるディレクトリを指定してください。")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(config, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(f"管理設定を保存しました: {path}")
    print(f"管理ユーザー名: {username}")
    print("ファイル権限: 600。パスワードや設定ファイルの内容を共有する必要はありません。")


if __name__ == "__main__":
    main()
