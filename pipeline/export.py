"""Exporta el dataset para Kaggle: un Parquet por tabla + dataset-metadata.json.

El repo de datos guarda miles de Parquet chicos particionados por día; para publicar se arma
un archivo por tabla (los que crecen sin límite, uno por mes), con las features ya calculadas
y el esquema documentado (config/kaggle.yaml). Las tablas cuya vista no existe todavía se
saltean.

Uso: python -m pipeline.export --out export --owner <usuario de Kaggle> [--data-dir PATH]
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import yaml

from pipeline.db import connect

ROOT = Path(__file__).resolve().parents[1]
KAGGLE_PATH = ROOT / "config" / "kaggle.yaml"
DESCRIPTION_PATH = ROOT / "config" / "kaggle_description.md"
log = logging.getLogger("pipeline.export")

_TYPES = {
    "BIGINT": "integer", "INTEGER": "integer", "HUGEINT": "integer", "SMALLINT": "integer",
    "TINYINT": "integer", "UBIGINT": "integer", "UINTEGER": "integer",
    "DOUBLE": "number", "FLOAT": "number",
    "BOOLEAN": "boolean",
    "DATE": "datetime", "TIMESTAMP": "datetime", "TIMESTAMP WITH TIME ZONE": "datetime",
}


def kaggle_type(duck_type: str) -> str:
    if duck_type.startswith("DECIMAL"):
        return "number"
    return _TYPES.get(duck_type, "string")


def load_spec(path: Path = KAGGLE_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class ExportResult:
    files: list[tuple[str, int]] = field(default_factory=list)   # (archivo, filas)
    skipped: list[str] = field(default_factory=list)              # tablas sin vista
    undocumented: set[str] = field(default_factory=set)           # columnas sin descripción


def _has_view(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute(
        "select count(*) from duckdb_views() where view_name = ?", [name]).fetchall()[0][0])


def _select(view: str, exclude: list[str], cols: list[str]) -> str:
    excl = [c for c in exclude if c in cols]
    return f"select * exclude ({', '.join(excl)}) from {view}" if excl else f"select * from {view}"


def _copy(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> int:
    con.execute(f"copy ({sql}) to '{path.as_posix()}' "
                "(format parquet, compression zstd, compression_level 9)")
    return con.execute(f"select count(*) from read_parquet('{path.as_posix()}')"
                       ).fetchall()[0][0]


def export(con: duckdb.DuckDBPyConnection, out: Path, owner: str,
           spec: dict[str, Any] | None = None) -> ExportResult:
    spec = spec or load_spec()
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.parquet"):
        old.unlink()
    res = ExportResult()
    resources = []
    columns_doc: dict[str, str] = spec["columns"]

    for name, t in spec["tables"].items():
        view = t["view"]
        if not _has_view(con, view):
            res.skipped.append(name)
            continue
        described = con.sql(f"describe {view}").fetchall()
        cols = [r[0] for r in described]
        sql = _select(view, t.get("exclude", []), cols)
        fields = []
        for col, typ, *_ in described:
            if col in t.get("exclude", []):
                continue
            desc = columns_doc.get(col)
            if desc is None:
                res.undocumented.add(col)
            fields.append({"name": col, "description": desc or "", "type": kaggle_type(typ)})

        if split := t.get("split_by"):
            months = [r[0] for r in con.sql(
                f"select distinct strftime({split} at time zone 'UTC', '%Y-%m') "
                f"from {view} where {split} is not null order by 1").fetchall()]
            parts = [(f"{name}_{m}.parquet",
                      f"select * from ({sql}) where strftime({split} at time zone 'UTC', "
                      f"'%Y-%m') = '{m}'", f" Month {m}.") for m in months]
        else:
            parts = [(f"{name}.parquet", sql, "")]

        for fname, part_sql, suffix in parts:
            n = _copy(con, part_sql, out / fname)
            res.files.append((fname, n))
            resources.append({"path": fname,
                              "description": " ".join(t["description"].split()) + suffix,
                              "schema": {"fields": fields}})

    ds = spec["dataset"]
    meta = {
        "title": ds["title"],
        "subtitle": ds["subtitle"],
        "description": DESCRIPTION_PATH.read_text(encoding="utf-8"),
        "id": f"{owner}/{ds['slug']}",
        "licenses": [{"name": ds["license"]}],
        "keywords": ds["keywords"],
        "expectedUpdateFrequency": ds["expected_update_frequency"],
        "userSpecifiedSources": " ".join(ds["sources"].split()),
        "resources": resources,
    }
    (out / "dataset-metadata.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    return res


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="export")
    parser.add_argument("--owner", required=True, help="usuario u organización de Kaggle")
    parser.add_argument("--data-dir", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    out = Path(args.out)
    res = export(connect(args.data_dir), out, args.owner)
    total = sum((out / f).stat().st_size for f, _ in res.files)
    for f, n in res.files:
        log.info("%-42s %10d filas", f, n)
    log.info("%d archivos, %.1f MB", len(res.files), total / 1e6)
    if res.skipped:
        log.warning("tablas sin datos todavía: %s", ", ".join(res.skipped))
    if res.undocumented:
        log.warning("columnas sin descripción en config/kaggle.yaml: %s",
                    ", ".join(sorted(res.undocumented)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
