"""管理画面の境界と、匿名投票を維持するための回帰テスト。"""

import base64
from datetime import datetime, timedelta
from html.parser import HTMLParser
from http.cookies import SimpleCookie
from pathlib import Path
import sqlite3
import sys

import pytest
import shogi
from werkzeug.security import generate_password_hash

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as shogi_app


BASE_URL = "https://shogi.test"
ADMIN_USER = "管理者"
ADMIN_PASSWORD = "テスト専用の十分に長いパスワード-42"
NOW = datetime(2026, 10, 22, 12, 0, 0)


def basic_header(user=ADMIN_USER, password=ADMIN_PASSWORD):
    credentials = base64.b64encode(f"{user}:{password}".encode("utf-8"))
    return "Basic " + credentials.decode("ascii")


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setattr(shogi_app, "DB_PATH", str(tmp_path / "shogi.db"))
    monkeypatch.setattr(shogi_app, "now_jst", lambda: NOW)
    # 本番の強度設定には触れず、繰り返し行うテストだけを軽くする。
    password_hash = generate_password_hash(
        ADMIN_PASSWORD, method="pbkdf2:sha256:1000"
    )
    for key, value in {
        "TESTING": True,
        "ADMIN_USER": ADMIN_USER,
        "ADMIN_PASSWORD_HASH": password_hash,
        "SECRET_KEY": "test-only-fixed-secret-key-never-use-in-production-42",
    }.items():
        monkeypatch.setitem(shogi_app.app.config, key, value)
    return shogi_app.app


def admin_request(client, method="GET", *, auth=basic_header(), data=None):
    headers = {} if auth is None else {"Authorization": auth}
    return client.open(
        "/admin", method=method, base_url=BASE_URL, headers=headers, data=data
    )


def assert_admin_headers(response):
    assert "no-store" in response.headers.get("Cache-Control", "").lower()
    assert response.headers.get("X-Frame-Options", "").upper() == "DENY"


class CsrfInputs(HTMLParser):
    def __init__(self):
        super().__init__()
        self.values = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "input" and attrs.get("name") == "csrf_token":
            self.values.append(attrs.get("value", ""))


def csrf_token_from(response):
    parser = CsrfInputs()
    parser.feed(response.get_data(as_text=True))
    assert len(parser.values) == 3, "すべての管理フォームにCSRFトークンが必要"
    assert len(set(parser.values)) == 1
    token = parser.values[0]
    assert len(token) >= 32
    return token


def seed_vote(app, *, overdue=False):
    """期限越えの票なら、通常のリクエストで実際に自動集計される。"""
    with app.app_context():
        shogi_app.init_db()
        game_id = shogi_app.get_active_game_id()
        move = next(iter(shogi.Board().legal_moves)).usi()
        assert shogi_app.register_vote(game_id, 0, move, "seed-voter", "127.0.0.1")
        if overdue:
            old_date = (NOW - timedelta(days=2)).isoformat(timespec="seconds")
            db = shogi_app.get_db()
            db.execute("UPDATE votes SET created_at = ?", (old_date,))
            db.commit()
    return game_id, move


def db_snapshot():
    with sqlite3.connect(shogi_app.DB_PATH) as db:
        return tuple(db.iterdump())


def query(sql, parameters=()):
    with sqlite3.connect(shogi_app.DB_PATH) as db:
        return db.execute(sql, parameters).fetchall()


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize(
    "missing_key,missing_value",
    [("ADMIN_USER", ""), ("ADMIN_PASSWORD_HASH", ""), ("SECRET_KEY", None)],
)
def test_missing_configuration_fails_closed_before_creating_db(
    app, monkeypatch, method, missing_key, missing_value
):
    monkeypatch.setitem(app.config, missing_key, missing_value)
    response = admin_request(
        app.test_client(), method, data={"action": "new_game"} if method == "POST" else None
    )
    assert response.status_code == 503
    assert_admin_headers(response)
    assert not Path(shogi_app.DB_PATH).exists()


def test_missing_configuration_cannot_finalize_overdue_votes(app, monkeypatch):
    seed_vote(app, overdue=True)
    before = db_snapshot()
    monkeypatch.setitem(app.config, "ADMIN_PASSWORD_HASH", "")
    response = admin_request(
        app.test_client(), "POST", data={"action": "new_game"}
    )
    assert response.status_code == 503
    assert_admin_headers(response)
    assert db_snapshot() == before


@pytest.mark.parametrize("method", ["GET", "HEAD", "POST"])
def test_anonymous_admin_requests_do_not_create_database(app, method):
    response = admin_request(app.test_client(), method, auth=None)
    assert response.status_code == 401
    assert response.headers.get("WWW-Authenticate", "").lower().startswith("basic ")
    assert_admin_headers(response)
    assert not Path(shogi_app.DB_PATH).exists()


@pytest.mark.parametrize(
    "auth",
    [
        None,
        "Bearer invalid-token",
        "Basic not-valid-base64!",
        "Basic " + base64.b64encode(b"missing-colon").decode("ascii"),
        basic_header(password="違うパスワード"),
        basic_header(user="別の管理者"),
    ],
)
def test_rejected_authentication_cannot_mutate_or_finalize_game(app, auth):
    seed_vote(app, overdue=True)
    before = db_snapshot()
    response = admin_request(
        app.test_client(), "POST", auth=auth, data={"action": "new_game"}
    )
    assert response.status_code == 401
    assert_admin_headers(response)
    assert db_snapshot() == before


def test_authenticated_get_sets_scoped_secure_cookie_and_all_form_tokens(app):
    response = admin_request(app.test_client())
    assert response.status_code == 200
    assert_admin_headers(response)
    token = csrf_token_from(response)
    cookies = SimpleCookie()
    for header in response.headers.getlist("Set-Cookie"):
        cookies.load(header)
    cookie = cookies["shogi_admin_session"]
    assert cookie["secure"]
    assert cookie["httponly"]
    assert cookie["samesite"].lower() == "lax"
    assert cookie["path"] == "/admin"
    serializer = app.session_interface.get_signing_serializer(app)
    assert serializer.loads(cookie.value)["admin_csrf_token"] == token
    html = response.get_data(as_text=True)
    assert "googletagmanager.com" not in html
    assert ADMIN_PASSWORD not in html


@pytest.mark.parametrize(
    "case", ["missing", "empty", "wrong", "non_ascii", "other_session", "missing_session"]
)
def test_bad_csrf_cannot_mutate_or_finalize_overdue_game(app, case):
    client = app.test_client()
    own_token = csrf_token_from(admin_request(client))
    other_client = app.test_client()
    other_token = csrf_token_from(admin_request(other_client))
    assert own_token != other_token

    # 管理GETによる準備が終わってから期限越えの票を用意する。
    seed_vote(app, overdue=True)
    before = db_snapshot()
    data = {"action": "new_game"}
    if case == "empty":
        data["csrf_token"] = ""
    elif case == "wrong":
        data["csrf_token"] = "not-the-real-token"
    elif case == "non_ascii":
        data["csrf_token"] = "不正なトークン"
    elif case == "other_session":
        data["csrf_token"] = other_token
    elif case == "missing_session":
        client = app.test_client()
        data["csrf_token"] = own_token

    response = admin_request(client, "POST", data=data)
    assert response.status_code == 400
    assert_admin_headers(response)
    assert db_snapshot() == before


@pytest.mark.parametrize("action", ["finalize", "new_game", "save_settings"])
def test_authenticated_csrf_protected_management_actions_still_work(app, action):
    client = app.test_client()
    token = csrf_token_from(admin_request(client))
    game_id, move = seed_vote(app)
    data = {"action": action, "csrf_token": token}
    if action == "save_settings":
        data.update(cutoff_hour="17", cutoff_minute="23")
    response = admin_request(client, "POST", data=data)
    assert response.status_code in (200, 302, 303)
    assert_admin_headers(response)

    if action == "finalize":
        assert query("SELECT ply, usi FROM moves WHERE game_id = ?", (game_id,)) == [(1, move)]
    elif action == "new_game":
        assert query("SELECT status FROM games WHERE id = ?", (game_id,)) == [("finished",)]
        active_games = query("SELECT id FROM games WHERE status = 'active'")
        assert len(active_games) == 1
        assert active_games[0][0] != game_id
    else:
        settings = dict(query("SELECT key, value FROM settings"))
        assert settings["cutoff_hour"] == "17"
        assert settings["cutoff_minute"] == "23"
        assert query("SELECT COUNT(*) FROM moves") == [(0,)]


def test_public_vote_duplicate_protection_history_and_svg_remain_available(app):
    client = app.test_client()
    response = client.get("/", base_url=BASE_URL)
    assert response.status_code == 200
    assert any(header.startswith("voter_token=") for header in response.headers.getlist("Set-Cookie"))
    assert not any(header.startswith("shogi_admin_session=") for header in response.headers.getlist("Set-Cookie"))
    game_id = query("SELECT id FROM games WHERE status = 'active'")[0][0]
    move = next(iter(shogi.Board().legal_moves)).usi()

    response = client.post("/vote", base_url=BASE_URL, data={"move_usi": move})
    assert response.status_code == 302
    assert query("SELECT move_usi FROM votes") == [(move,)]
    response = client.post("/vote", base_url=BASE_URL, data={"move_usi": move})
    assert response.status_code == 302
    assert query("SELECT COUNT(*) FROM votes") == [(1,)]

    assert client.get("/history", base_url=BASE_URL).status_code == 200
    assert client.get(f"/history/{game_id}", base_url=BASE_URL).status_code == 200
    svg = client.get("/share-board.svg", base_url=BASE_URL)
    assert svg.status_code == 200
    assert svg.mimetype == "image/svg+xml"
    assert "<svg" in svg.get_data(as_text=True)


def test_public_request_really_finalizes_the_overdue_fixture(app):
    game_id, move = seed_vote(app, overdue=True)
    assert query("SELECT COUNT(*) FROM moves") == [(0,)]
    response = app.test_client().get("/", base_url=BASE_URL)
    assert response.status_code == 200
    assert query("SELECT ply, usi FROM moves WHERE game_id = ?", (game_id,)) == [(1, move)]
