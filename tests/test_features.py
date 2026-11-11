from datetime import UTC, datetime

import polars as pl
import pytest

from collectors.common import write_dimension_changes
from pipeline import features
from pipeline.db import connect

T0 = datetime(2026, 11, 11, 12, tzinfo=UTC)
KEYWORDS = ["beta", "tycoon", "find the", "garden"]

ROBLOX = pl.DataFrame(
    {
        "universe_id": [1, 2, 3, 4, 5, 6, 7],
        "name": [
            "[🎃 UPD 3] Grow a Garden 🌱 x2!",
            "New York RP",
            "🎨UPD🎨Paint Or Seek",
            "(🎃) Dummy Defense",
            "My Cookie Store [BETA]",
            "Find the Markers [HALLOWEEN]",
            "Escape Room Tycoon",
        ],
        "description": ["abc", None, "", "", "", "", ""],
        "genre_l1": ["Simulation", "Roleplay & Avatar Sim", "Survival", "Survival", "",
                     "Puzzle", "Simulation"],
        "genre_l2": ["Tycoon", "Life", "", "", "", "Escape Room", ""],
        "genre_raw": ["All", "Town and City", "Horror", "All", "All", "All", "All"],
        "genre_unified": [None] * 7,
    },
    schema_overrides={"genre_unified": pl.Utf8},
)

FORTNITE = pl.DataFrame(
    {
        "island_code": ["a", "b", "c", "d"],
        "title": ["Tycoon PvP", "BOXFIGHT 1V1", "", "Hoth"],
        "tags": [["pvp", "Tycoon", "just for fun"], ["boxfight", "1v1"], [], ["mystery tag"]],
        "category": [None, None, "Star Wars", "STAR WARS "],
    }
)


@pytest.fixture
def con(tmp_path):
    write_dimension_changes(ROBLOX.clear(), ROBLOX, "universe_id", tmp_path, "roblox", T0, "r")
    write_dimension_changes(FORTNITE.clear(), FORTNITE, "island_code", tmp_path, "fortnite",
                            T0, "f")
    c = connect(tmp_path, features=False)
    created = features.register(c, keywords=KEYWORDS)
    assert created == ["roblox_game_features", "fortnite_game_features",
                       "roblox_games_enriched", "fortnite_games_enriched"]
    return c


def rows(con, sql):
    return con.sql(sql).pl().to_dicts()


def test_roblox_genres(con):
    got = dict(con.sql("select universe_id, genre_unified from roblox_game_features").fetchall())
    assert got == {1: "tycoon", 2: "roleplay", 3: "horror", 4: "survival", 5: "other",
                   6: "escape", 7: "simulator"}


def test_fortnite_genres_priority_and_ip(con):
    got = {r["island_code"]: r for r in rows(con, "select * from fortnite_game_features")}
    assert got["a"]["genre_unified"] == "tycoon"  # específico gana a "pvp"
    assert got["a"]["genres"] == ["tycoon", "shooter", "party_casual"]
    assert got["b"]["genre_unified"] == "pvp_practice"
    assert got["c"]["genre_unified"] == "other" and got["c"]["genres"] == []
    assert got["d"]["genre_unified"] == "other"  # tag desconocida
    assert got["c"]["licensed_ip"] == got["d"]["licensed_ip"] == "STAR WARS"
    assert got["a"]["licensed_ip"] is None


def test_title_features(con):
    got = {r["universe_id"]: r for r in rows(con, "select * from roblox_game_features")}
    g = got[1]
    assert g["bracket_tags"] == ["UPD 3"]
    assert g["has_update_tag"] and g["has_event_tag"] is False
    assert g["n_emojis"] == 2 and g["has_emoji"]
    assert g["has_multiplier"] and g["has_exclamation"] and g["has_number"]
    assert g["starts_with_bracket"]
    assert g["title_clean"] == "Grow a Garden x2!"
    assert g["title_keywords"] == ["garden"]
    assert g["description_len"] == 3
    assert got[2]["has_update_tag"] is False  # "New" fuera de corchetes no es etiqueta
    assert got[2]["has_brackets"] is False and got[2]["description_len"] == 0
    assert got[3]["has_update_tag"]  # "UPD" al inicio entre emojis
    assert got[3]["title_clean"] == "UPD Paint Or Seek"
    assert got[4]["bracket_tags"] == []  # corchete con solo un emoji
    assert got[5]["has_beta_tag"] and got[5]["title_keywords"] == []  # [BETA] no es keyword
    assert got[6]["has_event_tag"] and got[6]["title_keywords"] == ["find the"]
    assert got[7]["title_keywords"] == ["tycoon"]
    assert got[7]["caps_ratio"] == pytest.approx(3 / 16)


def test_enriched_views(con):
    r = rows(con, "select * from roblox_games_enriched where universe_id = 1")[0]
    assert r["name"].startswith("[🎃") and r["genre_unified"] == "tycoon"
    cols = con.sql("select * from roblox_games_enriched").columns
    assert cols.count("genre_unified") == 1 and cols.count("universe_id") == 1
    assert con.sql("select count(*) from fortnite_games_enriched").fetchall()[0][0] == 4


def test_connect_registers_features_with_real_config(tmp_path):
    write_dimension_changes(ROBLOX.clear(), ROBLOX, "universe_id", tmp_path, "roblox", T0, "r")
    c = connect(tmp_path)
    assert c.sql("select count(*) from roblox_games_enriched").fetchall()[0][0] == 7


def test_connect_skips_dimension_without_genre_columns(tmp_path):
    dim = pl.DataFrame({"universe_id": [1], "name": ["a"]})
    write_dimension_changes(dim.clear(), dim, "universe_id", tmp_path, "roblox", T0, "r")
    views = {r[0] for r in connect(tmp_path).sql(
        "select view_name from duckdb_views() where not internal").fetchall()}
    assert "roblox_games" in views and "roblox_game_features" not in views


def test_config_validation(tmp_path):
    cfg = features.load_genres()
    assert set(cfg["genres"]) >= {r["genre"] for r in cfg["roblox"]}
    bad = {**cfg, "roblox": [{"l1": "X", "genre": "nope"}]}
    p = tmp_path / "g.yaml"
    import yaml
    p.write_text(yaml.safe_dump(bad))
    with pytest.raises(ValueError, match="nope"):
        features.load_genres(p)
    dup = {**cfg, "fortnite": [{"genre": "tycoon", "tags": ["x"]},
                               {"genre": "horror", "tags": ["x"]}]}
    p.write_text(yaml.safe_dump(dup))
    with pytest.raises(ValueError, match="'x'"):
        features.load_genres(p)
