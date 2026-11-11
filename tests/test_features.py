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


def test_creator_history(tmp_path):
    from collectors.common import write_partition
    from collectors.roblox_creators import CREATOR_GAMES_SCHEMA

    d = lambda day: datetime(2026, 1, day, tzinfo=UTC)  # noqa: E731
    games = ROBLOX.head(2).with_columns(
        pl.Series("creator_type", ["Group", "User"]), pl.Series("creator_id", [7, 8]),
        pl.Series("created_at", [d(10), d(1)]))
    write_dimension_changes(games.clear(), games, "universe_id", tmp_path, "roblox", T0, "r")
    old = datetime(2026, 11, 1, tzinfo=UTC)
    rows = [  # (snapshot, creator_id, universe_id, created, visits)
        (old, 7, 99, d(2), 1),               # listado viejo: lo pisa el último
        (T0, 7, 1, d(10), 50), (T0, 7, 90, d(2), 2_000_000), (T0, 7, 91, d(5), 300),
        (T0, 7, 92, d(20), 9_000_000),       # posterior al juego 1: no es "previo"
    ]
    cg = pl.DataFrame([{"snapshot_at": s, "creator_type": "Group", "creator_id": c,
                        "universe_id": u, "created_at": cr, "place_visits": v}
                       for s, c, u, cr, v in rows]).cast(
        {k: v for k, v in CREATOR_GAMES_SCHEMA.items() if k in
         ("snapshot_at", "creator_type", "creator_id", "universe_id", "created_at",
          "place_visits")})
    write_partition(cg.filter(pl.col("snapshot_at") == old), tmp_path, "roblox",
                    "creator_games", old, "c0")
    write_partition(cg.filter(pl.col("snapshot_at") == T0), tmp_path, "roblox",
                    "creator_games", T0, "c1")

    fn = pl.DataFrame({
        "island_code": ["i1", "i2", "i3", "j1"],
        "title": ["a", "b", "c", "d"], "tags": [["pvp"]] * 4, "category": [None] * 4,
        "creator_code": ["cre", "cre", "cre", "solo"],
        "first_seen_at": [d(1), d(1), d(9), d(1)],
    })
    write_dimension_changes(fn.clear(), fn, "island_code", tmp_path, "fortnite", T0, "f")
    snaps = pl.DataFrame({"island_code": ["i1", "i1", "i2", "j1"],
                          "interval": ["day", "day", "day", "hour"],
                          "bucket_start": [d(1), d(2), d(1), d(1)],
                          "snapshot_at": [T0] * 4, "peak_ccu": [100, 300, 50, 999]})
    write_partition(snaps, tmp_path, "fortnite", "snapshots", T0, "s")

    con = connect(tmp_path, features=False)
    created = features.register(con, keywords=KEYWORDS)
    assert "roblox_creator_history" in created and "fortnite_creator_history" in created

    r = {x["universe_id"]: x for x in rows_(con, "select * from roblox_games_enriched")}
    g1 = r[1]
    assert g1["creator_total_games"] == 4 and g1["creator_prev_games"] == 2
    assert g1["creator_prev_max_visits"] == 2_000_000
    assert g1["creator_prev_total_visits"] == 2_000_300 and g1["creator_prev_hits"] == 1
    assert g1["creator_first_game_at"] == d(2)
    assert r[2]["creator_total_games"] is None  # creador sin consultar

    f = {x["island_code"]: x for x in rows_(con, "select * from fortnite_games_enriched")}
    assert f["i1"]["creator_best_peak_other"] == 50     # la mejor isla ve la segunda
    assert f["i2"]["creator_best_peak_other"] == 300
    assert f["i3"]["creator_best_peak_other"] == 300
    assert f["i3"]["creator_other_with_data"] == 2 and f["i1"]["creator_other_with_data"] == 1
    assert f["i3"]["creator_prev_islands_seen"] == 2 and f["i1"]["creator_prev_islands_seen"] == 0
    assert f["j1"]["creator_other_islands"] == 0
    assert f["j1"]["creator_best_peak_other"] is None   # solo snapshots horarios: no cuentan


def rows_(con, sql):
    return con.sql(sql).pl().to_dicts()


def test_title_clones_across_platforms(tmp_path):
    d = lambda y, m=1: datetime(y, m, 1, tzinfo=UTC)  # noqa: E731
    rb = pl.DataFrame({
        "universe_id": [1, 2, 3, 4],
        "name": ["[💀] Steal a Brainrot", "[UPD] Steal a Brainrot 2", "Steal a Brainrot Duels",
                 "Grow a Garden"],
        "genre_l1": ["Simulation"] * 4, "genre_l2": ["Tycoon"] * 4, "genre_raw": ["All"] * 4,
        "created_at": [d(2025, 5), d(2025, 8), d(2025, 9), d(2025, 3)],
        "first_seen_at": [d(2026, 11)] * 4,
    })
    fn = pl.DataFrame({
        "island_code": ["f1", "f2", "f3", "f4", "f5"],
        "title": ["STEAL THE BRAINROTS 🧠", "1v1 Build Fight", "1V1 BUILD FIGHTS", "Build Fight",
                  "🔥"],
        "tags": [["tycoon"]] * 5, "category": [None] * 5,
        "first_seen_at": [d(2026, 11)] * 5,
    })
    write_dimension_changes(rb.clear(), rb, "universe_id", tmp_path, "roblox", T0, "r")
    write_dimension_changes(fn.clear(), fn, "island_code", tmp_path, "fortnite", T0, "f")
    con = connect(tmp_path, features=False)
    created = features.register(con, cfg={**features.load_genres(),
                                          "clone_stopwords": ["a", "the"]}, keywords=[])
    assert "title_clones" in created

    c = {(x["platform"], x["id"]): x for x in rows_(con, "select * from title_clones")}
    assert ("fortnite", "f5") not in c  # título sin palabras: no entra
    steal = [c[("roblox", "1")], c[("roblox", "2")], c[("fortnite", "f1")]]
    assert {x["title_key"] for x in steal} == {"brainrot steal"}
    assert steal[0]["title_family_size"] == 3
    duels = c[("roblox", "3")]
    assert duels["title_key"] == "brainrot duel steal" and duels["clone_key"] == "brainrot steal"
    assert duels["clone_group_size"] == 4
    assert duels["clone_group_size_roblox"] == 3 and duels["clone_group_size_fortnite"] == 1
    assert [c[("roblox", i)]["clone_rank"] for i in "123"] == [1, 2, 3]  # por fecha de creación
    assert c[("roblox", "1")]["clone_group_first_at"] == d(2025, 5)
    # "1v1 build fight" ya es familia (2 islas): no se mete en "build fight"
    assert c[("fortnite", "f2")]["clone_key"] == "1v1 build fight"
    assert c[("fortnite", "f2")]["clone_group_size"] == 2
    assert c[("fortnite", "f4")]["clone_group_size"] == 1
    assert c[("roblox", "4")]["clone_group_size"] == 1

    r = {x["universe_id"]: x for x in rows_(con, "select * from roblox_games_enriched")}
    assert r[3]["clone_key"] == "brainrot steal" and r[3]["clone_rank"] == 3
    f = {x["island_code"]: x for x in rows_(con, "select * from fortnite_games_enriched")}
    assert f["f5"]["clone_key"] is None


def test_stopwords_must_be_strings(tmp_path):
    import yaml
    cfg = features.load_genres()
    p = tmp_path / "g.yaml"
    p.write_text(yaml.safe_dump({**cfg, "clone_stopwords": ["a", True]}))  # y sin comillas
    with pytest.raises(ValueError, match="comillas"):
        features.load_genres(p)
