from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from collectors.common import (
    TS,
    group_of,
    read_dimension,
    write_dimension_changes,
    write_partition,
)
from pipeline import quality

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
H = timedelta(hours=1)

R_SNAP = {"snapshot_at": TS, "universe_id": pl.Int64, "playing": pl.Int64,
          "visits": pl.Int64, "favorites": pl.Int64, "up_votes": pl.Int64,
          "down_votes": pl.Int64}
R_DIM = {"universe_id": pl.Int64, "first_seen_at": TS, "tier": pl.Utf8}
F_DIM = {"island_code": pl.Utf8, "first_seen_at": TS, "is_listed": pl.Boolean}
F_LOG = {"island_code": pl.Utf8, "snapshot_at": TS, "interval": pl.Utf8, "status": pl.Utf8}
F_SNAP = {"island_code": pl.Utf8, "interval": pl.Utf8, "bucket_start": TS, "snapshot_at": TS,
          "plays": pl.Int64, "unique_players": pl.Int64, "peak_ccu": pl.Int64,
          "avg_minutes_per_player": pl.Float64, "minutes_played": pl.Int64,
          "favorites": pl.Int64, "recommendations": pl.Int64, "retention_d1": pl.Float64,
          "retention_d7": pl.Float64}


def by_name(checks: list[quality.Check]) -> dict[str, quality.Check]:
    return {c.name: c for c in checks}


def write_dim(tmp_path, platform, key, df):
    old = read_dimension(tmp_path, platform, dict(df.schema), key)
    write_dimension_changes(old, df, key, tmp_path, platform, NOW - 30 * 24 * H, "dim")


def roblox_runs(tmp_path, times, ids=(1, 2), override=None):
    for i, t in enumerate(times):
        rows = [{"snapshot_at": t + timedelta(seconds=4 * j), "universe_id": uid,
                 "playing": 10, "visits": 1000 + i, "favorites": 5, "up_votes": 1,
                 "down_votes": 0, **(override or {}).get(uid, {})} for j, uid in enumerate(ids)]
        write_partition(pl.DataFrame(rows, schema=R_SNAP), tmp_path, "roblox", "snapshots",
                        t, f"r{int(t.timestamp())}")


@pytest.fixture
def rules():
    return quality.load_rules()


def test_max_gap_hours():
    starts = [NOW - 30 * H, NOW - 20 * H, NOW - 12 * H, NOW - 11 * H, NOW - H]
    gap, n = quality.max_gap_hours(starts, NOW)
    assert (gap, n) == (10.0, 4)  # 30 h → 20 h cruza el borde de la ventana
    assert quality.max_gap_hours([], NOW) == (24.0, 0)
    assert quality.max_gap_hours([NOW - 40 * H], NOW) == (40.0, 0)


def test_roblox_healthy(tmp_path, rules):
    dim = pl.DataFrame({"universe_id": [1, 2], "first_seen_at": [NOW - 100 * H] * 2,
                        "tier": ["hot", "warm"]}, schema=R_DIM)
    write_dim(tmp_path, "roblox", "universe_id", dim)
    roblox_runs(tmp_path, [NOW - h * H + timedelta(minutes=17) for h in range(26, 0, -1)])

    con = quality.connect(tmp_path)
    c = by_name(quality.check_roblox(con, NOW, rules))
    assert {n for n, x in c.items() if x.status == quality.OK} == {
        "max_gap_hours", "overdue_hot", "overdue_warm", "snapshot_duplicates",
        "negative_values", "negative_favorites_share", "null_share",
        "visits_decrease_share", "orphan_snapshots", "dim_duplicates"}
    assert c["overdue_cold"].status == quality.SKIP
    assert c["search_age_hours"].status == quality.SKIP
    assert "24 corridas" in c["max_gap_hours"].detail


def test_roblox_problems(tmp_path, rules):
    dim = pl.DataFrame({"universe_id": [1, 2, 3], "first_seen_at": [NOW - 100 * H] * 3,
                        "tier": ["hot", "hot", "cold"]}, schema=R_DIM)
    write_dim(tmp_path, "roblox", "universe_id", dim)
    # hueco de 9 h; el juego 2 deja de aparecer; el 9 no está en la dimensión
    roblox_runs(tmp_path, [NOW - 12 * H, NOW - 11 * H])
    roblox_runs(tmp_path, [NOW - 2 * H], ids=(1, 9), override={9: {"playing": -1, "favorites": -3}})
    roblox_runs(tmp_path, [NOW - H], ids=(1,), override={1: {"visits": 5}})  # visitas bajan
    write_partition(  # duplicado exacto
        pl.read_parquet(next((tmp_path / "roblox/snapshots").rglob(
            f"*-r{int((NOW - 12 * H).timestamp())}.parquet"))),
        tmp_path, "roblox", "snapshots", NOW - 12 * H, "dup")

    c = by_name(quality.check_roblox(quality.connect(tmp_path), NOW, rules))
    assert c["max_gap_hours"].status == quality.ERROR and c["max_gap_hours"].value == 9.0
    assert c["overdue_hot"].value == 0.5 and c["overdue_hot"].status == quality.ERROR
    assert c["overdue_cold"].status == quality.OK  # cold sin snapshot pero dentro de 21 días
    assert c["snapshot_duplicates"].status == quality.ERROR
    assert c["negative_values"].value == 1
    assert c["negative_favorites_share"].status == quality.ERROR  # 1 de 3 juegos
    assert c["visits_decrease_share"].value > 0
    assert c["orphan_snapshots"].value == 1


def fortnite_codes(n_groups=5):
    """Códigos de prueba agrupados por grupo de hash."""
    out: dict[int, list[str]] = {}
    i = 0
    while min((len(v) for v in out.values()), default=0) < 3 or len(out) < n_groups:
        code = f"0000-0000-{i:04d}"
        out.setdefault(group_of(code, n_groups), []).append(code)
        i += 1
    return out


def test_fortnite(tmp_path, rules):
    groups = fortnite_codes()
    g0, g1 = groups[0][:3], groups[1][:2]
    dim = pl.DataFrame({"island_code": g0 + g1, "first_seen_at": [NOW - 240 * H] * 5,
                        "is_listed": [True] * 5}, schema=F_DIM)
    write_dim(tmp_path, "fortnite", "island_code", dim)
    t = NOW - 8 * H
    log = pl.DataFrame([{"island_code": c, "snapshot_at": t, "interval": "day",
                         "status": s} for c, s in zip(g0[:2], ["ok", "error"], strict=True)],
                       schema=F_LOG)
    write_partition(log, tmp_path, "fortnite", "fetch_log", t, "f1")
    snaps = pl.DataFrame([
        {"island_code": g0[0], "interval": "day", "bucket_start": NOW - 24 * H,
         "snapshot_at": t, "plays": 10, "unique_players": 8, "peak_ccu": 9,
         "avg_minutes_per_player": 30.0, "retention_d1": 1.5, "retention_d7": 0.1},
        {"island_code": g0[0], "interval": "hour", "bucket_start": NOW - 20 * H,
         "snapshot_at": t, "plays": 10, "unique_players": 8, "peak_ccu": 3,
         "avg_minutes_per_player": 75.0},
    ], schema=F_SNAP)
    write_partition(snaps, tmp_path, "fortnite", "snapshots", t, "f1")

    c = by_name(quality.check_fortnite(quality.connect(tmp_path), NOW, rules, 5))
    assert c["last_run_age_hours"].status == quality.OK
    assert c["fetch_error_share"].value == 0.5
    assert c["group_missing_share"].value == pytest.approx(1 / 3)
    assert "grupo 0" in c["group_missing_share"].detail
    assert c["uncovered_share"].status == quality.SKIP  # 8 h de historia
    assert c["retention_out_of_range"].value == 1
    assert c["peak_above_unique"].value == 1
    assert c["minutes_above_bucket"].value == 1
    assert c["snapshot_duplicates"].status == quality.OK

    # con 7 días de historia, las islas sin consulta en 6 días se cuentan
    old = NOW - 7 * 24 * H
    write_partition(pl.DataFrame([{"island_code": g1[0], "snapshot_at": old,
                                   "interval": "day", "status": "ok"}], schema=F_LOG),
                    tmp_path, "fortnite", "fetch_log", old, "f0")
    c = by_name(quality.check_fortnite(quality.connect(tmp_path), NOW, rules, 5))
    assert c["uncovered_share"].value == pytest.approx(3 / 5)  # g0[2] y los dos de g1


def test_fortnite_without_recent_runs(tmp_path, rules):
    dim = pl.DataFrame({"island_code": ["A"], "first_seen_at": [NOW - 100 * H],
                        "is_listed": [True]}, schema=F_DIM)
    write_dim(tmp_path, "fortnite", "island_code", dim)
    old = NOW - 5 * 24 * H
    write_partition(pl.DataFrame([{"island_code": "A", "snapshot_at": old,
                                   "interval": "day", "status": "ok"}], schema=F_LOG),
                    tmp_path, "fortnite", "fetch_log", old, "f0")
    c = by_name(quality.check_fortnite(quality.connect(tmp_path), NOW, rules, 5))
    assert c["last_run_age_hours"].status == quality.ERROR


def test_run_writes_results_and_renders(tmp_path, rules):
    dim = pl.DataFrame({"universe_id": [1], "first_seen_at": [NOW - 100 * H],
                        "tier": ["hot"]}, schema=R_DIM)
    write_dim(tmp_path, "roblox", "universe_id", dim)
    roblox_runs(tmp_path, [NOW - H], ids=(1,))

    results = quality.run(tmp_path, ("roblox", "fortnite"), NOW, rules=rules)
    assert results["fortnite"][0].status == quality.ERROR  # sin datos de Fortnite
    df = pl.read_parquet(tmp_path / "roblox/quality/**/*.parquet")
    assert df.height == len(results["roblox"])
    assert set(df["status"]) <= {"ok", "warn", "error", "skip"}
    text = quality.render(results, NOW)
    assert "### roblox" in text and "| max_gap_hours |" in text
    assert quality.connect(tmp_path).execute(
        "select count(*) from roblox_quality").fetchall()[0][0] == df.height
