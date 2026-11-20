"""Tendencias entre plataformas (pipeline/trends.py) sobre tablas armadas a mano."""

import duckdb
import pytest

from pipeline import trends

# (plataforma, id, título, tokens, familia, lanzado, demanda w1, género)
GAMES = [
    # "Throw Food For Animals": nace en Roblox y ya es enorme ahí; Fortnite la copia
    ("roblox", "1", "Throw Food For Animals", ["animal", "food", "throw"], "animal food throw",
     "2026-09-18", 155e6, "tycoon"),
    ("roblox", "2", "Throw Food For Spooky Animals", ["animal", "food", "spooky", "throw"],
     "animal food throw", "2026-10-29", 1e5, "tycoon"),
    ("fortnite", "F1", "Throw Food For Animals", ["animal", "food", "throw"],
     "animal food throw", "2026-11-10", 2000, "tycoon"),  # carga inicial: fecha desconocida
    ("fortnite", "F2", "THROW FOOD TO ANIMALS", ["animal", "food", "throw"],
     "animal food throw", "2026-11-15", 2000, "tycoon"),
    ("fortnite", "F3", "Throw Food For Animals", ["animal", "food", "throw"],
     "animal food throw", "2026-11-18", 2000, "tycoon"),
    ("fortnite", "F4", "Throw Food For Animals!", ["animal", "food", "throw"],
     "animal food throw", "2026-11-19", 2000, "tycoon"),
    # "Seed Throw": Roblox el 29/10, Fortnite el 17/11 → 19 días
    ("roblox", "3", "Throw a Seed", ["seed", "throw"], "seed throw", "2026-10-29", 5e6, "tycoon"),
    ("fortnite", "F5", "THROW A SEED!", ["seed", "throw"], "seed throw", "2026-11-17", 900,
     "tycoon"),
    # ola de Roblox que no llegó a Fortnite
    ("roblox", "4", "Ragdoll Sandbox", ["ragdoll", "sandbox"], "ragdoll sandbox", "2026-11-01",
     1e6, "sandbox_building"),
    ("roblox", "5", "Ragdoll Sandbox 2", ["ragdoll", "sandbox"], "ragdoll sandbox",
     "2026-11-10", 6e5, "sandbox_building"),
    ("roblox", "6", "Ragdoll Sandbox!", ["ragdoll", "sandbox"], "ragdoll sandbox",
     "2026-11-19", 5e5, "sandbox_building"),
    # formato de combate de Fortnite: no se porta aunque crezca (palabra "zone")
    ("fortnite", "Z1", "Zone Wars", ["war", "zone"], "war zone", "2026-11-12", 9000, "shooter"),
    ("fortnite", "Z2", "Zone Wars 2", ["war", "zone"], "war zone", "2026-11-13", 9000, "shooter"),
    ("fortnite", "Z3", "ZONE WARS", ["war", "zone"], "war zone", "2026-11-14", 9000, "shooter"),
    # formato de Fortnite portable a Roblox (+ una isla de Epic que no cuenta)
    ("fortnite", "P1", "Pirate Island", ["island", "pirate"], "island pirate", "2026-11-12",
     4000, "obby_parkour"),
    ("fortnite", "P2", "PIRATE ISLAND", ["island", "pirate"], "island pirate", "2026-11-14",
     4000, "obby_parkour"),
    ("fortnite", "P3", "Pirate Island!", ["island", "pirate"], "island pirate", "2026-11-16",
     4000, "obby_parkour"),
    ("fortnite", "EPIC", "Pirate Island Epic", ["island", "pirate"], "island pirate",
     "2026-11-17", 1e6, "obby_parkour"),
    # palabra genérica: muchos títulos viejos y uno nuevo → no es ola
    *[("roblox", str(100 + i), f"Build Thing {i}", ["build", f"thing{i}"], None, "2020-01-01",
       100, "sandbox_building") for i in range(6)],
    ("roblox", "120", "Build New", ["build", "new"], None, "2026-11-19", 100, "sandbox_building"),
]


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("""create table title_clones (platform varchar, id varchar, title_clean varchar,
                 tokens varchar[], clone_key varchar, launched_at timestamptz)""")
    c.execute("create table roblox_game_demand (universe_id bigint, visits_w1 double, "
              "visits_w0 double)")
    c.execute("create table fortnite_island_demand (island_code varchar, demand_w1 double, "
              "demand_w0 double)")
    c.execute("create table fortnite_games (island_code varchar, creator_code varchar)")
    c.execute("create table roblox_game_features (universe_id bigint, genre_unified varchar)")
    c.execute("create table fortnite_game_features (island_code varchar, "
              "genre_unified varchar)")
    for plat, gid, title, toks, fam, at, w1, genre in GAMES:
        c.execute("insert into title_clones values (?, ?, ?, ?, ?, ?::timestamptz)",
                  [plat, gid, title, toks, fam, at + " 12:00:00+00"])
        if plat == "roblox":
            c.execute("insert into roblox_game_demand values (?, ?, null)", [int(gid), w1])
            c.execute("insert into roblox_game_features values (?, ?)", [int(gid), genre])
        else:
            c.execute("insert into fortnite_island_demand values (?, ?, null)", [gid, w1])
            c.execute("insert into fortnite_games values (?, ?)",
                      [gid, "epic" if gid == "EPIC" else "someone"])
            c.execute("insert into fortnite_game_features values (?, ?)", [gid, genre])
    # register() busca vistas: las tablas se exponen como vistas con el nombre esperado
    for t in ("title_clones", "roblox_game_demand", "fortnite_island_demand", "fortnite_games",
              "roblox_game_features", "fortnite_game_features"):
        c.execute(f"alter table {t} rename to _{t}")
        c.execute(f"create view {t} as select * from _{t}")
    assert trends.register(c) == ["trend_games", "cross_platform_trends"]
    return c


def theme(con, level, name):
    rows = con.sql("select * from cross_platform_trends where level = ? and theme = ?",
                   params=[level, name]).pl().to_dicts()
    return rows[0] if rows else None


def test_crossed_from_roblox_is_not_a_port_to_roblox(con):
    t = theme(con, "family", "animal food throw")
    assert t["rb_games"] == 2 and t["fn_games"] == 4
    assert t["rb_new_30d"] == 1          # el del 29/10; el del 18/09 es viejo
    assert t["fn_new_30d"] == 3          # F2..F4; F1 es de la carga inicial (fecha desconocida)
    assert t["fn_first_known"] is False
    assert t["first_platform"] == "unknown" and t["lag_days"] is None
    # crece en Fortnite con poca presencia en Roblox, pero en Roblox ya es enorme: nació ahí
    assert t["signal"] == "crossed"
    assert t["rb_top_game"] == "Throw Food For Animals"


def test_lag_when_both_dates_are_known(con):
    t = theme(con, "family", "seed throw")
    assert t["first_platform"] == "roblox" and t["lag_days"] == 19


def test_port_signals(con):
    assert theme(con, "family", "ragdoll sandbox")["signal"] == "port_to_fortnite"
    pirate = theme(con, "family", "island pirate")
    assert pirate["signal"] == "port_to_roblox"
    assert pirate["fn_games"] == 3                    # sin la isla de Epic
    assert pirate["fn_players_w1"] == pytest.approx(12000)
    assert pirate["genre"] == "obby_parkour"
    # formato de combate propio de Fortnite: crece, pero no se marca para portar
    assert theme(con, "family", "war zone")["signal"] is None


def test_keyword_level_needs_a_wave(con):
    build = theme(con, "keyword", "build")
    assert build["rb_games"] == 7 and build["rb_new_30d"] == 1
    assert build["new_share"] == pytest.approx(1 / 7)
    assert build["signal"] is None                    # genérica: no es ola
    assert theme(con, "keyword", "ragdoll") is None   # < 5 juegos
    throw = theme(con, "keyword", "throw")            # 8 juegos, 6 nuevos → ola
    assert throw["signal"] == "crossed"


def test_register_without_title_clones():
    assert trends.register(duckdb.connect()) == []
