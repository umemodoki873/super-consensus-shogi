from pathlib import Path
import sys

import shogi

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as shogi_app


def setup_temp_db(tmp_path: Path):
    shogi_app.DB_PATH = str(tmp_path / "test.db")
    with shogi_app.app.app_context():
        shogi_app.init_db()
        game_id = shogi_app.get_active_game_id()
        assert game_id is not None
        return game_id


def test_get_vote_ranking_prefers_last_voted_move_on_tie(tmp_path):
    game_id = setup_temp_db(tmp_path)
    board = shogi.Board()
    moves = [move.usi() for move in list(board.legal_moves)[:2]]

    with shogi_app.app.app_context():
        assert shogi_app.register_vote(game_id, 0, moves[0], "voter-1", "127.0.0.1")
        assert shogi_app.register_vote(game_id, 0, moves[1], "voter-2", "127.0.0.1")

        ranking = shogi_app.get_vote_ranking(game_id, 0)

    assert [row["move_usi"] for row in ranking[:2]] == [moves[1], moves[0]]
    assert [row["votes"] for row in ranking[:2]] == [1, 1]


def test_finalize_round_adopts_last_voted_move_when_votes_are_tied(tmp_path):
    game_id = setup_temp_db(tmp_path)
    board = shogi.Board()
    moves = [move.usi() for move in list(board.legal_moves)[:2]]

    with shogi_app.app.app_context():
        assert shogi_app.register_vote(game_id, 0, moves[0], "voter-1", "127.0.0.1")
        assert shogi_app.register_vote(game_id, 0, moves[1], "voter-2", "127.0.0.1")

        adopted = shogi_app.finalize_round(game_id)
        db = shogi_app.get_db()
        stored = db.execute(
            "SELECT usi, voted_count FROM moves WHERE game_id = ? AND ply = 1",
            (game_id,),
        ).fetchone()

    assert adopted == moves[1]
    assert stored["usi"] == moves[1]
    assert stored["voted_count"] == 1


def test_board_coordinate_labels_use_numbers_on_top_and_kanji_on_side():
    assert shogi_app.board_top_labels(False) == [str(n) for n in range(9, 0, -1)]
    assert shogi_app.board_side_labels(False) == ["一", "二", "三", "四", "五", "六", "七", "八", "九"]


def test_board_coordinate_labels_reverse_when_rotated():
    assert shogi_app.board_top_labels(True) == [str(n) for n in range(1, 10)]
    assert shogi_app.board_side_labels(True) == ["九", "八", "七", "六", "五", "四", "三", "二", "一"]
