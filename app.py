import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from html import escape
from typing import Optional
from urllib.parse import urlencode

import shogi
from flask import Flask, Response, g, redirect, render_template, request, url_for, make_response

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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_moves_game_ply_unique ON moves(game_id, ply);

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
    query += " ORDER BY ply ASC, id ASC"

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
            square = file + rank * 9
            piece = board.piece_at(square)
            row.append(piece.japanese_symbol() if piece else "・")
        grid.append(row)
    return grid


def build_board_cells(board: shogi.Board, is_rotated: bool):
    rows = []
    for rank in range(9):
        row = []
        for file in range(9):
            square = file + rank * 9
            piece = board.piece_at(square)
            usi_square = shogi.SQUARE_NAMES[square]
            piece_symbol = piece.japanese_symbol() if piece else ""
            row.append(
                {
                    "usi_square": usi_square,
                    "piece_symbol": piece_symbol,
                    "piece_class": piece_class(piece, is_rotated),
                }
            )
        rows.append(row)

    if is_rotated:
        rows = [list(reversed(row)) for row in reversed(rows)]
    return rows


def board_top_labels(is_rotated: bool):
    return [str(n) for n in range(9, 0, -1)] if not is_rotated else [str(n) for n in range(1, 10)]


def board_side_labels(is_rotated: bool):
    return ["一", "二", "三", "四", "五", "六", "七", "八", "九"] if not is_rotated else ["九", "八", "七", "六", "五", "四", "三", "二", "一"]


def piece_class(piece: Optional[shogi.Piece], is_rotated: bool) -> str:
    if piece is None:
        return ""

    # 盤を回転していないときは後手駒を反転、回転時は先手駒を反転
    if (not is_rotated and piece.color == shogi.WHITE) or (is_rotated and piece.color == shogi.BLACK):
        return "piece-opponent"
    return "piece-own"


def side_to_move_label(board: shogi.Board) -> str:
    return "先手" if board.turn == shogi.BLACK else "後手"


def query_flag(name: str) -> bool:
    return request.args.get(name, "0") == "1"


def get_round_index(game_id: int) -> int:
    db = get_db()
    row = db.execute("SELECT COUNT(*) AS c FROM moves WHERE game_id = ?", (game_id,)).fetchone()
    return int(row["c"])


def list_legal_moves(board: shogi.Board, previous_usi: Optional[str] = None):
    options = []
    for move in board.legal_moves:
        usi = move.usi()
        kif = move_to_kif(move, board)
        ki2 = usi_move_to_ki2(usi, board, previous_usi)
        options.append({"usi": usi, "kif": kif, "ki2": ki2})
    return options


def move_to_kif(move: shogi.Move, board: shogi.Board) -> str:
    # python-shogi標準の日本語表記を利用
    try:
        return shogi.KIF.move_to_kif(move, board)
    except Exception:
        return move.usi()


def hand_piece_name(piece_type: int) -> str:
    names = {
        shogi.PAWN: "歩",
        shogi.LANCE: "香",
        shogi.KNIGHT: "桂",
        shogi.SILVER: "銀",
        shogi.GOLD: "金",
        shogi.BISHOP: "角",
        shogi.ROOK: "飛",
    }
    return names.get(piece_type, "駒")


def hand_piece_usi(piece_type: int) -> str:
    mapping = {
        shogi.PAWN: "P",
        shogi.LANCE: "L",
        shogi.KNIGHT: "N",
        shogi.SILVER: "S",
        shogi.GOLD: "G",
        shogi.BISHOP: "B",
        shogi.ROOK: "R",
    }
    return mapping.get(piece_type, "")


def build_hand_data(board: shogi.Board, color: int, interactive: bool):
    hand = board.pieces_in_hand[color]
    order = [
        shogi.ROOK,
        shogi.BISHOP,
        shogi.GOLD,
        shogi.SILVER,
        shogi.KNIGHT,
        shogi.LANCE,
        shogi.PAWN,
    ]
    result = []
    for piece_type in order:
        count = hand.get(piece_type, 0)
        if count > 0:
            result.append(
                {
                    "name": hand_piece_name(piece_type),
                    "usi": hand_piece_usi(piece_type),
                    "count": count,
                    "interactive": interactive,
                }
            )
    return result


def get_last_move_info(game_id: int) -> str:
    db = get_db()
    row = db.execute(
        "SELECT ply, usi FROM moves WHERE game_id = ? ORDER BY ply DESC LIMIT 1",
        (game_id,),
    ).fetchone()
    if row is None:
        return "開始局面"

    ply = int(row["ply"])
    mark = "▲" if ply % 2 == 1 else "△"
    previous_row = db.execute(
        "SELECT usi FROM moves WHERE game_id = ? AND ply = ?",
        (game_id, ply - 1),
    ).fetchone()
    previous_usi = previous_row["usi"] if previous_row is not None else None
    board_before = build_board(game_id, upto_ply=ply - 1)
    return f"{ply}手目 {mark}{usi_move_to_ki2(row['usi'], board_before, previous_usi)}まで"


def get_previous_move_usi(game_id: int) -> Optional[str]:
    db = get_db()
    row = db.execute(
        "SELECT usi FROM moves WHERE game_id = ? ORDER BY ply DESC LIMIT 1",
        (game_id,),
    ).fetchone()
    if row is None:
        return None
    return str(row["usi"])


def usi_move_to_ki2(move_usi: str, board_before: shogi.Board, previous_usi: Optional[str] = None) -> str:
    move = shogi.Move.from_usi(move_usi)
    destination = move_destination_ki2(move, previous_usi)
    piece_name = move_piece_name(move, board_before)
    suffix = ""
    if move.drop_piece_type is not None:
        suffix = "打"
    elif move.promotion:
        suffix = "成"
    return f"{destination}{piece_name}{suffix}"


def create_share_board_svg(board: shogi.Board, move_usi: str, move_label: str) -> str:
    board_size = 540
    margin = 24
    cell = board_size // 9
    total_width = margin * 2 + board_size
    total_height = margin * 2 + board_size + 72

    highlight_square: Optional[int] = None
    if move_usi:
        try:
            highlight_square = shogi.Move.from_usi(move_usi).to_square
        except ValueError:
            highlight_square = None

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="{total_height}" viewBox="0 0 {total_width} {total_height}">',
        '<rect width="100%" height="100%" fill="#f8f6ef"/>',
        f'<rect x="{margin}" y="{margin}" width="{board_size}" height="{board_size}" fill="#f2d9a6" stroke="#5d4037" stroke-width="3"/>',
    ]

    for idx in range(10):
        pos = margin + idx * cell
        lines.append(f'<line x1="{margin}" y1="{pos}" x2="{margin + board_size}" y2="{pos}" stroke="#7b5b34" stroke-width="1"/>')
        lines.append(f'<line x1="{pos}" y1="{margin}" x2="{pos}" y2="{margin + board_size}" stroke="#7b5b34" stroke-width="1"/>')

    if highlight_square is not None:
        file_idx = highlight_square % 9
        rank_idx = highlight_square // 9
        x = margin + file_idx * cell
        y = margin + rank_idx * cell
        lines.append(
            f'<rect x="{x + 2}" y="{y + 2}" width="{cell - 4}" height="{cell - 4}" fill="#ffeb3b" fill-opacity="0.45"/>'
        )

    for rank in range(9):
        for file in range(9):
            square = file + rank * 9
            piece = board.piece_at(square)
            if piece is None:
                continue
            symbol = piece.japanese_symbol()
            center_x = margin + file * cell + cell / 2
            center_y = margin + rank * cell + cell / 2 + 1
            transform = f' transform="rotate(180 {center_x} {center_y})"' if piece.color == shogi.WHITE else ""
            lines.append(
                f'<text x="{center_x}" y="{center_y}" text-anchor="middle" dominant-baseline="middle" font-size="36"'
                f' font-family="sans-serif" fill="#111"{transform}>{escape(symbol)}</text>'
            )

    safe_move_label = escape(move_label) if move_label else "（未選択）"
    caption_y = margin * 2 + board_size + 36
    lines.append(
        f'<text x="{total_width / 2}" y="{caption_y}" text-anchor="middle" font-size="28" font-family="sans-serif" fill="#111">'
        f'選択した手: {safe_move_label}</text>'
    )
    lines.append("</svg>")
    return "".join(lines)


def move_destination_ki2(move: shogi.Move, previous_usi: Optional[str]) -> str:
    if previous_usi:
        prev = shogi.Move.from_usi(previous_usi)
        if prev.to_square == move.to_square:
            return "同"

    square = shogi.SQUARE_NAMES[move.to_square]
    file_zenkaku = {
        "1": "１",
        "2": "２",
        "3": "３",
        "4": "４",
        "5": "５",
        "6": "６",
        "7": "７",
        "8": "８",
        "9": "９",
    }
    rank_kanji = {
        "a": "一",
        "b": "二",
        "c": "三",
        "d": "四",
        "e": "五",
        "f": "六",
        "g": "七",
        "h": "八",
        "i": "九",
    }
    return f"{file_zenkaku[square[0]]}{rank_kanji[square[1]]}"


def move_piece_name(move: shogi.Move, board: shogi.Board) -> str:
    piece_names = {
        shogi.PAWN: "歩",
        shogi.LANCE: "香",
        shogi.KNIGHT: "桂",
        shogi.SILVER: "銀",
        shogi.GOLD: "金",
        shogi.BISHOP: "角",
        shogi.ROOK: "飛",
        shogi.KING: "玉",
        shogi.PROM_PAWN: "と",
        shogi.PROM_LANCE: "成香",
        shogi.PROM_KNIGHT: "成桂",
        shogi.PROM_SILVER: "成銀",
        shogi.PROM_BISHOP: "馬",
        shogi.PROM_ROOK: "龍",
    }
    if move.drop_piece_type is not None:
        return piece_names.get(move.drop_piece_type, "駒")

    piece = board.piece_at(move.from_square) if move.from_square is not None else None
    if piece is None:
        return "駒"
    return piece_names.get(piece.piece_type, "駒")


def get_cutoff_datetime(now: datetime) -> datetime:
    hour = get_setting_int("cutoff_hour", DEFAULT_CUTOFF_HOUR)
    minute = get_setting_int("cutoff_minute", DEFAULT_CUTOFF_MINUTE)
    cutoff = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= cutoff:
        cutoff += timedelta(days=1)
    return cutoff


def get_round_deadline(game_id: int) -> datetime:
    # MVP簡略化: 投票期間は「次の締切時刻」まで。開始時刻は保持しない。
    _ = game_id
    return get_cutoff_datetime(now_jst())


def get_vote_ranking(game_id: int, round_index: int):
    db = get_db()
    rows = db.execute(
        """
        SELECT move_usi, COUNT(*) AS votes, MAX(id) AS last_vote_id
        FROM votes
        WHERE game_id = ? AND round_index = ?
        GROUP BY move_usi
        ORDER BY votes DESC, last_vote_id DESC, move_usi ASC
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


def get_voted_move_usi(game_id: int, round_index: int, voter_token: str) -> str:
    db = get_db()
    row = db.execute(
        """
        SELECT move_usi
        FROM votes
        WHERE game_id = ? AND round_index = ? AND voter_token = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (game_id, round_index, voter_token),
    ).fetchone()
    if row is None:
        return ""
    return str(row["move_usi"])


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
    db = get_db()
    db.execute("BEGIN IMMEDIATE")
    try:
        board = build_board(game_id)
        if board.is_game_over():
            db.rollback()
            return None

        round_index = get_round_index(game_id)
        already_finalized = db.execute(
            "SELECT 1 FROM moves WHERE game_id = ? AND ply = ? LIMIT 1",
            (game_id, round_index + 1),
        ).fetchone()
        if already_finalized is not None:
            db.rollback()
            return None

        ranking = get_vote_ranking(game_id, round_index)
        if not ranking:
            db.rollback()
            return None

        top = ranking[0]
        move_usi = top["move_usi"]
        legal_usi = {m.usi() for m in board.legal_moves}
        if move_usi not in legal_usi:
            db.rollback()
            return None

        move = shogi.Move.from_usi(move_usi)
        kif = move_to_kif(move, board)
        board.push(move)

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
    except sqlite3.IntegrityError:
        db.rollback()
        return None


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
    previous_usi = get_previous_move_usi(game_id)
    legal_moves = [] if board.is_game_over() else list_legal_moves(board, previous_usi)
    legal_move_usis = [m["usi"] for m in legal_moves]
    legal_move_labels = {m["usi"]: m["ki2"] for m in legal_moves}
    ranking_rows = get_vote_ranking(game_id, round_index)
    legal_move_map = {m["usi"]: m["ki2"] for m in legal_moves}
    ranking_display = []
    for row in ranking_rows:
        move_usi = row["move_usi"]
        if move_usi not in legal_move_map:
            continue
        ranking_display.append(
            {
                "usi": move_usi,
                "ki2": legal_move_map[move_usi],
                "votes": row["votes"],
            }
        )
    top_move = ranking_display[0] if ranking_display else None

    voter_token = get_client_token()
    voted = has_voted(game_id, round_index, voter_token)
    voted_move_usi = get_voted_move_usi(game_id, round_index, voter_token) if voted else ""
    selected_move_usi = request.args.get("move_usi", "")
    if selected_move_usi not in legal_move_usis:
        selected_move_usi = voted_move_usi if voted_move_usi in legal_move_usis else ""
    selected_move_label = legal_move_labels.get(selected_move_usi, "")
    show_share_prompt = query_flag("share") and voted
    cutoff = get_round_deadline(game_id)
    now = now_jst()
    remaining = cutoff - now
    remaining_seconds = max(int(remaining.total_seconds()), 0)
    manual_flip = query_flag("flip")
    auto_rotated = board.turn == shogi.WHITE
    is_rotated = auto_rotated ^ manual_flip

    ogp_title = "超合議制将棋"
    ogp_description = f"{round_index + 1}手目の投票受付中。みんなで次の一手を決めよう。"
    if selected_move_label:
        ogp_title = f"「{selected_move_label}」に投票しました | 超合議制将棋"
        ogp_description = f"{round_index + 1}手目で「{selected_move_label}」に投票しました。あなたも参加しよう。"

    ogp_url = url_for("index", _external=True, move_usi=selected_move_usi) if selected_move_usi else url_for("index", _external=True)
    ogp_image = url_for(
        "share_board_image",
        _external=True,
        move_usi=selected_move_usi,
        v=f"{game_id}-{round_index}",
    )
    share_text = f"#超合議制将棋 {round_index + 1}手目で「{selected_move_label or '（手を選択）'}」に投票しました。"
    share_url_x = "https://twitter.com/intent/tweet?" + urlencode({"text": share_text, "url": ogp_url})
    share_url_facebook = "https://www.facebook.com/sharer/sharer.php?" + urlencode({"u": ogp_url, "quote": share_text})
    share_url_threads = "https://www.threads.net/intent/post?" + urlencode({"text": f"{share_text}\n{ogp_url}"})
    share_url_line = "https://social-plugins.line.me/lineit/share?" + urlencode({"url": f"{ogp_url}\n{share_text}"})

    resp = make_response(
        render_template(
            "index.html",
            title="超合議制将棋",
            board_grid=board_to_grid(board),
            board_cells=build_board_cells(board, is_rotated),
            board_rotated=is_rotated,
            auto_rotated=auto_rotated,
            manual_flip=manual_flip,
            side_to_move=side_to_move_label(board),
            game_over=board.is_game_over(),
            legal_moves=legal_moves,
            legal_move_usis=legal_move_usis,
            legal_move_labels=legal_move_labels,
            selected_move_usi=selected_move_usi,
            ranking_display=ranking_display,
            top_move=top_move,
            voted=voted,
            show_share_prompt=show_share_prompt,
            share_url_x=share_url_x,
            share_url_threads=share_url_threads,
            share_url_facebook=share_url_facebook,
            share_url_line=share_url_line,
            round_index=round_index + 1,
            remaining_seconds=remaining_seconds,
            cutoff_time=cutoff.strftime("%H:%M"),
            cutoff_epoch=int(cutoff.timestamp()),
            server_now_epoch=int(now.timestamp()),
            top_labels=board_top_labels(is_rotated),
            side_labels=board_side_labels(is_rotated),
            black_hand=build_hand_data(board, shogi.BLACK, board.turn == shogi.BLACK),
            white_hand=build_hand_data(board, shogi.WHITE, board.turn == shogi.WHITE),
            last_move_text=get_last_move_info(game_id),
            ogp_title=ogp_title,
            ogp_description=ogp_description,
            ogp_url=ogp_url,
            ogp_image=ogp_image,
        )
    )
    if "voter_token" not in request.cookies:
        resp.set_cookie("voter_token", voter_token, max_age=60 * 60 * 24 * 365)
    return resp


@app.get("/share-board.svg")
def share_board_image():
    game_id = get_active_game_id()
    if game_id is None:
        game_id = start_new_game()

    board = build_board(game_id)
    move_usi = request.args.get("move_usi", "")
    previous_usi = get_previous_move_usi(game_id)
    move_label = ""
    legal_usi = {m.usi() for m in board.legal_moves}
    if move_usi in legal_usi:
        move_label = usi_move_to_ki2(move_usi, board, previous_usi)

    svg = create_share_board_svg(board, move_usi, move_label)
    return Response(svg, mimetype="image/svg+xml")


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
    vote_registered = register_vote(game_id, round_index, move_usi, token, ip)

    redirect_kwargs = {"flip": "1"} if query_flag("flip") else {}
    if vote_registered:
        redirect_kwargs["share"] = "1"
        redirect_kwargs["move_usi"] = move_usi
    resp = make_response(redirect(url_for("index", **redirect_kwargs)))
    if "voter_token" not in request.cookies:
        resp.set_cookie("voter_token", token, max_age=60 * 60 * 24 * 365)
    return resp


@app.route("/history")
def history():
    db = get_db()
    games = db.execute(
        """
        SELECT g.id, g.created_at, g.finished_at, g.status, COUNT(m.id) AS total_moves
        FROM games g
        LEFT JOIN moves m ON m.game_id = g.id
        GROUP BY g.id, g.created_at, g.finished_at, g.status
        ORDER BY g.id DESC
        """
    ).fetchall()
    return render_template("history.html", title="履歴一覧", games=games)


@app.route("/history/<int:game_id>")
def history_game(game_id: int):
    db = get_db()
    game = db.execute(
        "SELECT id, status, created_at, finished_at FROM games WHERE id = ?",
        (game_id,),
    ).fetchone()
    if game is None:
        return redirect(url_for("history"))

    total_moves_row = db.execute("SELECT COUNT(*) AS c FROM moves WHERE game_id = ?", (game_id,)).fetchone()
    total_moves = int(total_moves_row["c"])

    # n手目終了時点の局面を表示し、その局面に対する次の1手の投票ランキングを表示する
    ply = request.args.get("ply", "0")
    try:
        selected_ply = max(0, min(total_moves, int(ply)))
    except ValueError:
        selected_ply = 0

    board = build_board(game_id, upto_ply=selected_ply)
    manual_flip = query_flag("flip")
    is_rotated = manual_flip

    votes_rows = db.execute(
        """
        SELECT move_usi, COUNT(*) AS votes, MAX(id) AS last_vote_id
        FROM votes
        WHERE game_id = ? AND round_index = ?
        GROUP BY move_usi
        ORDER BY votes DESC, last_vote_id DESC, move_usi ASC
        """,
        (game_id, selected_ply),
    ).fetchall()

    previous_row = db.execute(
        "SELECT usi FROM moves WHERE game_id = ? AND ply = ?",
        (game_id, selected_ply),
    ).fetchone()
    previous_usi = previous_row["usi"] if previous_row is not None else None

    ranking_display = []
    for row in votes_rows:
        move_usi = str(row["move_usi"])
        ki2 = usi_move_to_ki2(move_usi, board, previous_usi)
        ranking_display.append(
            {
                "usi": move_usi,
                "ki2": ki2,
                "votes": int(row["votes"]),
            }
        )

    adopted_row = db.execute(
        "SELECT usi FROM moves WHERE game_id = ? AND ply = ?",
        (game_id, selected_ply + 1),
    ).fetchone()
    adopted_usi = adopted_row["usi"] if adopted_row is not None else None

    return render_template(
        "history_game.html",
        title=f"履歴詳細: 対局 {game_id}",
        game=game,
        total_moves=total_moves,
        selected_ply=selected_ply,
        manual_flip=manual_flip,
        board_cells=build_board_cells(board, is_rotated),
        board_rotated=is_rotated,
        top_labels=board_top_labels(is_rotated),
        side_labels=board_side_labels(is_rotated),
        black_hand=build_hand_data(board, shogi.BLACK, False),
        white_hand=build_hand_data(board, shogi.WHITE, False),
        ranking_display=ranking_display,
        adopted_usi=adopted_usi,
    )


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
