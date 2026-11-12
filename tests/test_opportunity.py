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
