from datetime import UTC, datetime, timedelta

import polars as pl

from collectors.common import TS, write_dimension_changes, write_partition
from pipeline import compact
from pipeline.db import connect

NOW = datetime.now(UTC)
# a medianoche: las corridas de OLD + 0..5 h tienen que caer en la misma partición
OLD = (NOW - timedelta(days=5)).replace(hour=0, minute=0, second=0, microsecond=0)
SCHEMA = {"snapshot_at": TS, "universe_id": pl.Int64, "playing": pl.Int64}


def snaps(t: datetime, ids: list[int]) -> pl.DataFrame:
    return pl.DataFrame({"snapshot_at": [t] * len(ids), "universe_id": ids,
                         "playing": [i * 10 for i in ids]}, schema=SCHEMA)


def files(path) -> list[str]:
    return sorted(p.name for p in path.glob("*.parquet"))


def test_compacts_closed_partitions_without_changing_content(tmp_path):
    for i in range(3):  # 3 corridas viejas en la misma partición
        write_partition(snaps(OLD + timedelta(hours=i), [i, i + 10]), tmp_path, "roblox",
                        "snapshots", OLD + timedelta(hours=i), f"r{i}")
    # una corrida vieja con una columna más (evolución de esquema)
    extra = snaps(OLD + timedelta(hours=5), [99]).with_columns(pl.lit(7).alias("visits"))
    write_partition(extra, tmp_path, "roblox", "snapshots", OLD + timedelta(hours=5), "r5")
    for i in range(2):  # partición reciente: no se toca
        write_partition(snaps(NOW, [i]), tmp_path, "roblox", "snapshots", NOW, f"n{i}")
    dim = pl.DataFrame({"universe_id": [1], "name": ["a"]})
    write_dimension_changes(dim.clear(), dim, "universe_id", tmp_path, "roblox", OLD, "d1")
    dim2 = pl.DataFrame({"universe_id": [1], "name": ["b"]})
    write_dimension_changes(dim, dim2, "universe_id", tmp_path, "roblox",
                            OLD + timedelta(hours=1), "d2")

    before = connect(tmp_path).sql(
        "select * from roblox_snapshots order by snapshot_at, universe_id").pl()
    res = compact.run(tmp_path)

    old_part = tmp_path / f"roblox/snapshots/date={OLD:%Y-%m-%d}"
    new_part = tmp_path / f"roblox/snapshots/date={NOW:%Y-%m-%d}"
    assert files(old_part) == [compact.COMPACTED]
    assert len(files(new_part)) == 2
    assert res.partitions == 2 and res.files_before == 6 and res.rows == 9

    con = connect(tmp_path)
    after = con.sql("select * from roblox_snapshots order by snapshot_at, universe_id").pl()
    assert after.equals(before.select(after.columns))
    assert con.sql("select name from roblox_games").fetchall() == [("b",)]
    got = pl.read_parquet(old_part / compact.COMPACTED)
    assert got["universe_id"].to_list() == [0, 10, 1, 11, 2, 12, 99]  # orden de las corridas
    assert got["visits"].to_list() == [None] * 6 + [7]


def test_recompacts_when_late_files_arrive_and_dry_run_is_harmless(tmp_path):
    for i in range(2):
        write_partition(snaps(OLD, [i]), tmp_path, "fortnite", "fetch_log", OLD, f"a{i}")
    compact.run(tmp_path)
    write_partition(snaps(OLD + timedelta(hours=1), [5]), tmp_path, "fortnite", "fetch_log",
                    OLD + timedelta(hours=1), "late")
    part = tmp_path / f"fortnite/fetch_log/date={OLD:%Y-%m-%d}"

    res = compact.run(tmp_path, dry_run=True)
    assert res.partitions == 1 and len(files(part)) == 2

    compact.run(tmp_path)
    assert files(part) == [compact.COMPACTED]
    assert pl.read_parquet(part / compact.COMPACTED)["universe_id"].to_list() == [0, 1, 5]
    assert compact.run(tmp_path).partitions == 0  # ya no queda nada
