import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Optional

import shogi
from flask import Flask, g, redirect, render_template, request, url_for, make_response

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "shogi.db")
DEFAULT_CUTOFF_HOUR = 0
DEFAULT_CUTOFF_MINUTE = 0

app = Flask(__name__)


# ----------------------------
# DB helpers
# ----------------------------
def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_exception: Optional[BaseException]) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            finished_at TEXT
        );

        CREATE TABLE IF NOT EXISTS moves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            ply INTEGER NOT NULL,
            usi TEXT NOT NULL,
            kif TEXT NOT NULL,
            voted_count INTEGER NOT NULL DEFAULT 0,
            decided_at TEXT NOT NULL,
            FOREIGN KEY(game_id) REFERENCES games(id)
        );

        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            round_index INTEGER NOT NULL,
            move_usi TEXT NOT NULL,
            voter_token TEXT NOT NULL,
            ip_address TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(game_id, round_index, voter_token),
            FOREIGN KEY(game_id) REFERENCES games(id)
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )

    # default settings
    ensure_setting("cutoff_hour", str(DEFAULT_CUTOFF_HOUR))
    ensure_setting("cutoff_minute", str(DEFAULT_CUTOFF_MINUTE))

    if get_active_game_id() is None:
        now = now_str()
        db.execute(
            "INSERT INTO games(status, created_at) VALUES('active', ?)",
            (now,),
        )
    db.commit()


def ensure_setting(key: str, value: str) -> None:
    db = get_db()
    row = db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if row is None:
        db.execute("INSERT INTO settings(key, value) VALUES(?, ?)", (key, value))


def set_setting(key: str, value: str) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO settings(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    db.commit()


def get_setting_int(key: str, default: int) -> int:
    db = get_db()
    row = db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    try:
        return int(row["value"])
    except (TypeError, ValueError):
        return default


# ----------------------------
# Domain logic
# ----------------------------
def now_jst() -> datetime:
    # MVP: server local time is used. (Raspberry Pi運用を想定)
    return datetime.now()


def now_str() -> str:
    return now_jst().isoformat(timespec="seconds")


def get_active_game_id() -> Optional[int]:
    db = get_db()
    row = db.execute("SELECT id FROM games WHERE status = 'active' ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:
        return None
    return int(row["id"])


def build_board(game_id: int, upto_ply: Optional[int] = None) -> shogi.Board:
    db = get_db()
    board = shogi.Board()
    params = [game_id]
    query = "SELECT usi FROM moves WHERE game_id = ?"
    if upto_ply is not None:
        query += " AND ply <= ?"
        params.append(upto_ply)
    query += " ORDER BY ply ASC"

    rows = db.execute(query, tuple(params)).fetchall()
    for row in rows:
        try:
            board.push_usi(row["usi"])
        except ValueError:
            # 壊れた棋譜があってもMVPでは処理継続
            continue
    return board


def board_to_grid(board: shogi.Board):
    grid = []
    for rank in range(9):
        row = []
        for file in range(9):
            square = (8 - file) + rank * 9
            piece = board.piece_at(square)
            row.append(piece.japanese_symbol() if piece else "・")
        grid.append(row)
    return grid


def side_to_move_label(board: shogi.Board) -> str:
    return "先手" if board.turn == shogi.BLACK else "後手"


def get_round_index(game_id: int) -> int:
    db = get_db()
    row = db.execute("SELECT COUNT(*) AS c FROM moves WHERE game_id = ?", (game_id,)).fetchone()
    return int(row["c"])


def list_legal_moves(board: shogi.Board):
    options = []
    for move in board.legal_moves:
        usi = move.usi()
        kif = move_to_kif(move, board)
        options.append({"usi": usi, "kif": kif})
    return options


def move_to_kif(move: shogi.Move, board: shogi.Board) -> str:
    # python-shogi標準の日本語表記を利用
    try:
        return shogi.KIF.move_to_kif(move, board)
    except Exception:
        return move.usi()


def get_cutoff_datetime(now: datetime) -> datetime:
    hour = get_setting_int("cutoff_hour", DEFAULT_CUTOFF_HOUR)
    minute = get_setting_int("cutoff_minute", DEFAULT_CUTOFF_MINUTE)
    cutoff = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= cutoff:
        cutoff += timedelta(days=1)
    return cutoff


def get_round_deadline(game_id: int) -> datetime:
    # MVP簡略化: 各ラウンドは「次の締切時刻」まで。ラウンド開始時刻は保持しない。
    _ = game_id
    return get_cutoff_datetime(now_jst())


def get_vote_ranking(game_id: int, round_index: int):
    db = get_db()
    rows = db.execute(
        """
        SELECT move_usi, COUNT(*) AS votes
        FROM votes
        WHERE game_id = ? AND round_index = ?
        GROUP BY move_usi
        ORDER BY votes DESC, move_usi ASC
        """,
        (game_id, round_index),
    ).fetchall()
    return [{"move_usi": r["move_usi"], "votes": int(r["votes"])} for r in rows]


def get_client_token() -> str:
    token = request.cookies.get("voter_token")
    if token:
        return token
    return str(uuid.uuid4())


def has_voted(game_id: int, round_index: int, voter_token: str) -> bool:
    db = get_db()
    row = db.execute(
        "SELECT 1 FROM votes WHERE game_id = ? AND round_index = ? AND voter_token = ?",
        (game_id, round_index, voter_token),
    ).fetchone()
    return row is not None


def register_vote(game_id: int, round_index: int, move_usi: str, voter_token: str, ip: str) -> bool:
    db = get_db()
    try:
        db.execute(
            """
            INSERT INTO votes(game_id, round_index, move_usi, voter_token, ip_address, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (game_id, round_index, move_usi, voter_token, ip, now_str()),
        )
        db.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def finalize_round(game_id: int) -> Optional[str]:
    board = build_board(game_id)
    if board.is_game_over():
        return None

    round_index = get_round_index(game_id)
    ranking = get_vote_ranking(game_id, round_index)
    if not ranking:
        return None

    top = ranking[0]
    move_usi = top["move_usi"]
    legal_usi = {m.usi() for m in board.legal_moves}
    if move_usi not in legal_usi:
        return None

    move = shogi.Move.from_usi(move_usi)
    kif = move_to_kif(move, board)
    board.push(move)

    db = get_db()
    db.execute(
        """
        INSERT INTO moves(game_id, ply, usi, kif, voted_count, decided_at)
        VALUES(?, ?, ?, ?, ?, ?)
        """,
        (game_id, round_index + 1, move_usi, kif, int(top["votes"]), now_str()),
    )

    if board.is_game_over():
        db.execute(
            "UPDATE games SET status = 'finished', finished_at = ? WHERE id = ?",
            (now_str(), game_id),
        )

    db.commit()
    return move_usi


def maybe_auto_finalize() -> None:
    game_id = get_active_game_id()
    if game_id is None:
        return

    db = get_db()
    round_index = get_round_index(game_id)
    row = db.execute(
        """
        SELECT MAX(created_at) AS last_vote
        FROM votes
        WHERE game_id = ? AND round_index = ?
        """,
        (game_id, round_index),
    ).fetchone()

    # 投票がない場合は何もしない（空打ちの自動進行はMVPでは未対応）
    if row is None or row["last_vote"] is None:
        return

    now = now_jst()
    cutoff = get_cutoff_datetime(now)
    # 締切を過ぎているかを判定するため、1日前の締切も確認
    prev_cutoff = cutoff - timedelta(days=1)
    last_vote_at = datetime.fromisoformat(row["last_vote"])
    if last_vote_at < prev_cutoff <= now:
        finalize_round(game_id)


def start_new_game() -> int:
    db = get_db()
    db.execute("UPDATE games SET status = 'finished', finished_at = ? WHERE status = 'active'", (now_str(),))
    db.execute("INSERT INTO games(status, created_at) VALUES('active', ?)", (now_str(),))
    row = db.execute("SELECT id FROM games WHERE status = 'active' ORDER BY id DESC LIMIT 1").fetchone()
    db.commit()
    return int(row["id"])


# ----------------------------
# Routes
# ----------------------------
@app.before_request
def setup() -> None:
    init_db()
    maybe_auto_finalize()


@app.route("/")
def index():
    game_id = get_active_game_id()
    if game_id is None:
        game_id = start_new_game()

    board = build_board(game_id)
    round_index = get_round_index(game_id)
    legal_moves = [] if board.is_game_over() else list_legal_moves(board)
    ranking = get_vote_ranking(game_id, round_index)

    ranking_map = {row["move_usi"]: row["votes"] for row in ranking}
    ranking_display = []
    for item in legal_moves:
        ranking_display.append(
            {
                "usi": item["usi"],
                "kif": item["kif"],
                "votes": ranking_map.get(item["usi"], 0),
            }
        )
    ranking_display.sort(key=lambda x: (-x["votes"], x["usi"]))

    voter_token = get_client_token()
    voted = has_voted(game_id, round_index, voter_token)
    cutoff = get_round_deadline(game_id)
    remaining = cutoff - now_jst()
    remaining_seconds = max(int(remaining.total_seconds()), 0)

    resp = make_response(
        render_template(
            "index.html",
            title="超合議制将棋",
            board_grid=board_to_grid(board),
            side_to_move=side_to_move_label(board),
            game_over=board.is_game_over(),
            legal_moves=legal_moves,
            ranking_display=ranking_display,
            voted=voted,
            round_index=round_index + 1,
            remaining_seconds=remaining_seconds,
            cutoff_time=cutoff.strftime("%H:%M"),
        )
    )
    if "voter_token" not in request.cookies:
        resp.set_cookie("voter_token", voter_token, max_age=60 * 60 * 24 * 365)
    return resp


@app.post("/vote")
def vote():
    game_id = get_active_game_id()
    if game_id is None:
        return redirect(url_for("index"))

    board = build_board(game_id)
    if board.is_game_over():
        return redirect(url_for("index"))

    move_usi = request.form.get("move_usi", "")
    legal_usi = {m.usi() for m in board.legal_moves}
    if move_usi not in legal_usi:
        return redirect(url_for("index"))

    round_index = get_round_index(game_id)
    token = get_client_token()
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    register_vote(game_id, round_index, move_usi, token, ip)

    resp = make_response(redirect(url_for("index")))
    if "voter_token" not in request.cookies:
        resp.set_cookie("voter_token", token, max_age=60 * 60 * 24 * 365)
    return resp


@app.route("/history")
def history():
    game_id = get_active_game_id()
    db = get_db()
    moves = []
    boards = []
    if game_id is not None:
        moves = db.execute(
            "SELECT ply, usi, kif, voted_count, decided_at FROM moves WHERE game_id = ? ORDER BY ply ASC",
            (game_id,),
        ).fetchall()
        # 各局面を簡易に閲覧できるように、手数ごとの盤面を準備
        for row in moves:
            ply = int(row["ply"])
            boards.append({"ply": ply, "grid": board_to_grid(build_board(game_id, upto_ply=ply))})

    return render_template("history.html", title="履歴", moves=moves, boards=boards)


@app.route("/admin", methods=["GET", "POST"])
def admin():
    message = ""
    if request.method == "POST":
        action = request.form.get("action")
        if action == "finalize":
            gid = get_active_game_id()
            if gid is not None:
                result = finalize_round(gid)
                message = "集計して1手進めました。" if result else "進行できる投票がありません。"
        elif action == "new_game":
            start_new_game()
            message = "新しい対局を開始しました。"
        elif action == "save_settings":
            hour_raw = request.form.get("cutoff_hour", "0")
            minute_raw = request.form.get("cutoff_minute", "0")
            try:
                hour = max(0, min(23, int(hour_raw)))
                minute = max(0, min(59, int(minute_raw)))
                set_setting("cutoff_hour", str(hour))
                set_setting("cutoff_minute", str(minute))
                message = "設定を保存しました。"
            except ValueError:
                message = "設定値が不正です。"

    hour = get_setting_int("cutoff_hour", DEFAULT_CUTOFF_HOUR)
    minute = get_setting_int("cutoff_minute", DEFAULT_CUTOFF_MINUTE)

    return render_template(
        "admin.html",
        title="管理",
        message=message,
        cutoff_hour=hour,
        cutoff_minute=minute,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
