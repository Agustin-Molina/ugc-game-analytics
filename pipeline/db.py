"""Vistas DuckDB sobre los datos Parquet.

    from pipeline.db import connect
    con = connect()                       # usa ./data o $UGC_DATA_DIR
    con.sql("select * from roblox_games limit 5")

Vistas creadas (solo las de tablas que ya tienen archivos):

- `<plataforma>_games`           estado actual de la dimensión (última fila por id)
- `<plataforma>_games_history`   log completo de la dimensión (una fila por cambio)
- `<plataforma>_<tabla>`         tablas de hechos (snapshots, events, …) con columna `date`
- `fortnite_snapshots_latest`    snapshots de Fortnite sin duplicados de solapamiento
                                 (la fila con `snapshot_at` más reciente por bucket)

`python -m pipeline.db [--data-dir PATH]` lista las vistas y sus filas.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from collectors.common import resolve_data_dir

DIMENSION_KEYS = {"roblox": "universe_id", "fortnite": "island_code"}
FACT_TABLES = {
    "roblox": ["snapshots", "chart_positions", "events", "tier_history"],
    "fortnite": ["snapshots", "fetch_log", "events"],
}


def current_state_sql(source: str, key: str) -> str:
    """SQL del estado actual de una dimensión append-only: última fila por `key`."""
    return (
        f"select * from {source} "
        f"qualify row_number() over (partition by {key} order by valid_from desc) = 1"
    )


def _glob(data_dir: Path, platform: str, table: str) -> str | None:
    base = data_dir / platform / table
    if not any(base.glob("date=*/*.parquet")):
        return None
    return (base / "date=*" / "*.parquet").as_posix()


def connect(
    data_dir: str | Path | None = None, con: duckdb.DuckDBPyConnection | None = None
) -> duckdb.DuckDBPyConnection:
    data_dir = resolve_data_dir(str(data_dir) if data_dir else None)
    con = con or duckdb.connect()
    for platform, key in DIMENSION_KEYS.items():
        glob = _glob(data_dir, platform, "games")
        if glob:
            history = f"{platform}_games_history"
            con.execute(
                f"create or replace view {history} as select * from read_parquet("
                f"'{glob}', hive_partitioning = false, union_by_name = true)"
            )
            con.execute(
                f"create or replace view {platform}_games as "
                f"{current_state_sql(history, key)}"
            )
        for table in FACT_TABLES[platform]:
            glob = _glob(data_dir, platform, table)
            if glob:
                con.execute(
                    f"create or replace view {platform}_{table} as select * from read_parquet("
                    f"'{glob}', hive_partitioning = true, union_by_name = true)"
                )
    if _glob(data_dir, "fortnite", "snapshots"):
        con.execute(
            "create or replace view fortnite_snapshots_latest as "
            "select * from fortnite_snapshots qualify row_number() over ("
            "partition by island_code, interval, bucket_start order by snapshot_at desc) = 1"
        )
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description="Lista las vistas DuckDB disponibles")
    parser.add_argument("--data-dir", default=None)
    args = parser.parse_args()
    con = connect(args.data_dir)
    views = con.sql(
        "select view_name from duckdb_views() where not internal order by 1"
    ).fetchall()
    for (name,) in views:
        n = con.execute(f"select count(*) from {name}").fetchall()[0][0]
        print(f"{name:32s} {n:>10d} filas")


if __name__ == "__main__":
    main()
