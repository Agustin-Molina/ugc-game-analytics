from datetime import UTC, datetime

import polars as pl

from collectors.common import TS, write_dimension_changes, write_partition
from pipeline.db import connect

T0 = datetime(2026, 11, 9, 23, tzinfo=UTC)
T1 = datetime(2026, 11, 10, 1, tzinfo=UTC)


def test_views_current_state_and_dedup(tmp_path):
    schema = {"universe_id": pl.Int64, "name": pl.Utf8}
    v1 = pl.DataFrame({"universe_id": [1, 2], "name": ["a", "b"]}, schema=schema)
    v2 = pl.DataFrame({"universe_id": [1, 2], "name": ["a", "b2"]}, schema=schema)
    write_dimension_changes(v1.clear(), v1, "universe_id", tmp_path, "roblox", T0, "r1")
    write_dimension_changes(v1, v2, "universe_id", tmp_path, "roblox", T1, "r2")

    snaps = pl.DataFrame(
        {"island_code": ["X", "X"], "interval": ["day", "day"],
         "bucket_start": [T0, T0], "snapshot_at": [T0, T1], "peak_ccu": [10, 12]},
        schema={"island_code": pl.Utf8, "interval": pl.Utf8, "bucket_start": TS,
                "snapshot_at": TS, "peak_ccu": pl.Int64},
    )
    write_partition(snaps, tmp_path, "fortnite", "snapshots", T1, "r3")

    con = connect(tmp_path)
    assert con.sql("select universe_id, name from roblox_games order by 1").fetchall() == [
        (1, "a"), (2, "b2")]
    assert con.execute("select count(*) from roblox_games_history").fetchall()[0][0] == 3
    assert con.sql("select peak_ccu from fortnite_snapshots_latest").fetchall() == [(12,)]
    assert con.execute("select date::varchar from fortnite_snapshots limit 1").fetchall()[0][0] \
        == "2026-11-10"
    views = {r[0] for r in con.sql("select view_name from duckdb_views() where not internal")
             .fetchall()}
    assert "roblox_snapshots" not in views  # sin archivos → sin vista
