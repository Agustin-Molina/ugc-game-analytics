from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from collectors.common import TS, write_dimension_changes, write_partition
from pipeline import opportunity
from pipeline.db import connect


def day(d: int) -> datetime:
    return datetime(2026, 10, d, tzinfo=UTC)


@pytest.fixture
def con(tmp_path):
    games = pl.DataFrame({
        "island_code": ["A", "B", "C", "D", "E"],
        "title": ["Tycoon A", "Tycoon B", "Horror C", "Tycoon D", "Epic E"],
        "tags": [["tycoon"], ["tycoon"], ["horror"], ["tycoon"], ["tycoon"]],
        "category": [None] * 5,
        "creator_code": ["x", "y", "z", "w", "epic"],
        "is_listed": [True] * 5,
        "first_seen_at": [day(1)] * 5,
        "first_traction_at": [None] * 5,
    }, schema_overrides={"first_seen_at": TS, "first_traction_at": TS})
    write_dimension_changes(games.clear(), games, "island_code", tmp_path, "fortnite", day(1),
                            "g")
    # A: consultada el 10 y el 24 → cobertura 3..23 (dos semanas); B, C, E: solo el 24
    log = pl.DataFrame(
        [{"island_code": c, "snapshot_at": day(d) + timedelta(hours=5), "interval": "day",
          "status": "ok"} for c, d in [("A", 10), ("A", 24), ("B", 24), ("C", 24), ("E", 24)]]
        + [{"island_code": "C", "snapshot_at": day(24), "interval": "hour", "status": "ok"}],
        schema_overrides={"snapshot_at": TS})
    write_partition(log, tmp_path, "fortnite", "fetch_log", day(24), "l")

    snaps = []
    snaps += [("A", d, 10) for d in range(10, 17)]           # semana 0: 70
    snaps += [("A", d, 20) for d in range(17, 24) if d != 20]  # semana 1: 6 días → 120
    snaps += [("B", d, 100) for d in (17, 18, 19)]           # 300, sin semana 0
    snaps += [("B", 24, 999)]                                # fuera de la ventana (hoy)
    snaps += [("E", d, 10_000) for d in range(17, 24)]       # Epic: no cuenta
    df = pl.DataFrame(
        [{"island_code": c, "interval": "day", "bucket_start": day(d),
          "snapshot_at": day(24), "is_partial": False, "unique_players": u, "peak_ccu": u,
          "plays": u, "favorites": 0, "recommendations": 0, "retention_d1": None,
          "retention_d7": None, "avg_minutes_per_player": 1.0, "minutes_played": 1}
         for c, d, u in snaps],
        schema_overrides={"bucket_start": TS, "snapshot_at": TS, "retention_d1": pl.Float64,
                          "retention_d7": pl.Float64})
    write_partition(df, tmp_path, "fortnite", "snapshots", day(24), "s")
    return connect(tmp_path)


def test_island_demand_windows(con):
    d = {r["island_code"]: r for r in
         con.sql("select * from fortnite_island_demand").pl().to_dicts()}
    assert d["A"]["demand_w1"] == 120 and d["A"]["demand_w0"] == 70
    assert d["B"]["demand_w1"] == 300 and d["B"]["demand_w0"] is None
    assert d["C"]["demand_w1"] == 0
    assert "D" not in d  # nunca consultada


def test_niche_opportunity(con):
    n = {r["genre_unified"]: r for r in
         con.sql("select * from fortnite_niche_opportunity").pl().to_dicts()}
    t = n["tycoon"]
    assert t["islands_listed"] == 3 and t["islands_active"] == 2  # sin la de Epic
    assert t["demand_w1"] == 420
    assert t["demand_growth"] == pytest.approx(120 / 70 - 1)
    assert t["demand_per_active_island"] == pytest.approx(210)
    assert t["median_island_demand"] == pytest.approx(210)
    assert t["top10_share"] == pytest.approx(1.0)
    assert t["opportunity_score"] == pytest.approx((120 - 70) / 2)
    h = n["horror"]
    assert h["islands_active"] == 0 and h["demand_w1"] == 0
    assert h["opportunity_score"] is None


def test_render(con):
    out = opportunity.render(con)
    assert "| tycoon | 3 | 2 | 420 | +71% |" in out
    assert out.index("| tycoon") < out.index("| horror")


# ---------------------------------------------------------------- Roblox

END = datetime(2026, 10, 24, 12, tzinfo=UTC)


@pytest.fixture
def rcon(tmp_path):
    old, new = datetime(2020, 1, 1, tzinfo=UTC), END - timedelta(days=2)
    games = pl.DataFrame({
        "universe_id": [1, 2, 3, 4],
        "name": ["Tycoon One", "Tycoon Two", "Roleplay Three", "Tycoon Four"],
        "genre_l1": ["Simulation", "Simulation", "Roleplay & Avatar Sim", "Simulation"],
        "genre_l2": ["Tycoon", "Tycoon", "", "Tycoon"],
        "genre_raw": ["All"] * 4,
        "created_at": [old, new, old, old],
        "first_seen_at": [old] * 4,
    }, schema_overrides={"created_at": TS, "first_seen_at": TS})
    write_dimension_changes(games.clear(), games, "universe_id", tmp_path, "roblox", END, "g")

    rows = []
    # 1: cada 12 h durante 14 días; 100 visitas/día la semana 0, 200/día la semana 1
    for h in range(0, 337, 12):
        v = 1000 + 100 * min(h, 168) / 24 + 200 * max(h - 168, 0) / 24
        rows.append((1, END - timedelta(days=14) + timedelta(hours=h), int(v), 50 + h))
    rows.append((1, END - timedelta(hours=1), 0, 0))  # glitch de la API (visits 0): se ignora
    # 2: creado hace 2 días → 500 visitas el primer día, 1000 el segundo
    rows += [(2, END - timedelta(days=1), 500, 30), (2, END, 1500, 40)]
    # 3: una sola foto → sin demanda; 4: dos fotos a 6 h → cobertura insuficiente
    rows += [(3, END, 10_000, 5)]
    rows += [(4, END - timedelta(hours=6), 100, 1), (4, END, 200, 1)]
    df = pl.DataFrame([{"universe_id": u, "snapshot_at": t, "visits": v, "playing": p}
                       for u, t, v, p in rows], schema_overrides={"snapshot_at": TS})
    write_partition(df, tmp_path, "roblox", "snapshots", END, "s")
    return connect(tmp_path)


def test_roblox_game_demand(rcon):
    d = {r["universe_id"]: r for r in
         rcon.sql("select * from roblox_game_demand").pl().to_dicts()}
    assert d[1]["visits_w1"] == pytest.approx(1400)
    assert d[1]["visits_w0"] == pytest.approx(700)
    assert d[1]["peak_ccu_w1"] == 50 + 336
    # juego nuevo: los días previos a la creación cuentan como cubiertos (0 visitas), sin
    # extrapolar 2 días a 7; sin semana anterior real
    assert d[2]["visits_w1"] == pytest.approx(1500)
    assert d[2]["w1_coverage_days"] == pytest.approx(7)
    assert d[2]["visits_w0"] is None
    assert 3 not in d
    assert d[4]["visits_w1"] is None  # 6 h no alcanzan para estimar la semana


def test_roblox_niche_opportunity(rcon):
    n = {r["genre_unified"]: r for r in
         rcon.sql("select * from roblox_niche_opportunity").pl().to_dicts()}
    t = n["tycoon"]
    assert t["games_tracked"] == 3 and t["games_active"] == 2
    assert t["visits_w1"] == pytest.approx(2900)
    assert t["visits_growth"] == pytest.approx(1400 / 700 - 1)
    assert t["median_game_visits"] == pytest.approx(1450)
    assert t["top10_share"] == pytest.approx(1.0)
    assert t["new_games_30d"] == 1
    assert t["new_games_visits_share"] == pytest.approx(1500 / 2900)
    assert t["opportunity_score"] == pytest.approx((1400 - 700) / 2)
    r = n["roleplay"]
    assert r["games_tracked"] == 1 and r["games_active"] == 0
    assert r["opportunity_score"] is None


def test_render_roblox(rcon):
    out = opportunity.render(rcon, "roblox")
    assert "| tycoon | 3 | 2 | 2,900 | +100% |" in out
    assert out.index("| tycoon") < out.index("| roleplay")


def test_main_without_data(tmp_path, capsys):
    assert opportunity.main(["--data-dir", str(tmp_path)]) == 1
    assert "faltan datos de roblox" in capsys.readouterr().out
