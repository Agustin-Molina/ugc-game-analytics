"""Compactación de Parquet chicos: un archivo por partición `date=` cerrada.

    python -m pipeline.compact [--data-dir PATH] [--min-age-days 2] [--dry-run]

Cada corrida de los colectores escribe sus propios archivos (el loop horario de Roblox, ~5
por hora), así que una partición diaria junta cientos de archivos chicos. Esto los junta en
`<data_dir>/<plataforma>/<tabla>/date=YYYY-MM-DD/000000-compacted.parquet`:

- Solo particiones con al menos `min_age_days` días (por defecto 2): ningún colector puede
  seguir escribiendo ahí (las corridas largas escriben en la fecha en que arrancaron).
- No cambia el contenido: mismas filas, mismo orden (el de los nombres de archivo, que
  empiezan con la hora de la corrida). Si los esquemas difieren (columnas agregadas con el
  tiempo), une por nombre y completa con null.
- Escritura atómica y verificación de filas antes de borrar los originales. Si ya existe un
  compactado y llegaron archivos nuevos, se vuelve a compactar todo junto.

No achica el historial de git (los archivos viejos siguen en commits anteriores): sirve para
que leer sea rápido y el árbol de trabajo no tenga miles de archivos.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from collectors.common import resolve_data_dir, setup_logging, utcnow

log = logging.getLogger(__name__)

COMPACTED = "000000-compacted.parquet"
PLATFORMS = ("roblox", "fortnite")


@dataclass
class Result:
    partitions: int = 0
    files_before: int = 0
    rows: int = 0
    bytes_before: int = 0
    bytes_after: int = 0


def partitions(data_dir: Path, cutoff: date) -> list[Path]:
    """Particiones `date=` anteriores o iguales a `cutoff` con más de un archivo."""
    out = []
    for platform in PLATFORMS:
        for part in sorted((data_dir / platform).glob("*/date=*")):
            try:
                day = date.fromisoformat(part.name.removeprefix("date="))
            except ValueError:
                continue
            if day <= cutoff and len(list(part.glob("*.parquet"))) > 1:
                out.append(part)
    return out


def read_all(files: list[Path]) -> pl.DataFrame:
    frames = [pl.read_parquet(f, hive_partitioning=False) for f in files]
    if all(f.schema == frames[0].schema for f in frames):
        return pl.concat(frames, how="vertical")
    return pl.concat(frames, how="diagonal_relaxed")


def compact_partition(part: Path, dry_run: bool = False) -> tuple[int, int, int, int]:
    """Devuelve (archivos, filas, bytes antes, bytes después)."""
    files = sorted(part.glob("*.parquet"))
    before = sum(f.stat().st_size for f in files)
    df = read_all(files)
    expected = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    if df.height != expected:
        raise RuntimeError(f"{part}: {df.height} filas leídas, se esperaban {expected}")
    if dry_run:
        return len(files), df.height, before, before
    target = part / COMPACTED
    tmp = part / (COMPACTED + ".tmp")
    df.write_parquet(tmp, compression="zstd", compression_level=9, statistics=True)
    if pq.ParquetFile(tmp).metadata.num_rows != expected:
        tmp.unlink()
        raise RuntimeError(f"{part}: el compactado no tiene {expected} filas")
    os.replace(tmp, target)
    for f in files:
        if f != target:
            f.unlink()
    return len(files), df.height, before, target.stat().st_size


def run(data_dir: Path, min_age_days: int = 2, dry_run: bool = False) -> Result:
    cutoff = utcnow().date() - timedelta(days=min_age_days)
    res = Result()
    for part in partitions(data_dir, cutoff):
        n_files, rows, before, after = compact_partition(part, dry_run)
        log.info("%s: %d archivos → 1 (%d filas, %.1f → %.1f KB)",
                 part.relative_to(data_dir), n_files, rows, before / 1e3, after / 1e3)
        res.partitions += 1
        res.files_before += n_files
        res.rows += rows
        res.bytes_before += before
        res.bytes_after += after
    return res


def render(res: Result, min_age_days: int, dry_run: bool) -> str:
    mode = " (dry-run)" if dry_run else ""
    return "\n".join([
        f"## Compactación{mode}",
        "",
        f"- particiones compactadas: {res.partitions} (de hace ≥ {min_age_days} días)",
        f"- archivos: {res.files_before} → {res.partitions}",
        f"- filas: {res.rows}",
        f"- tamaño: {res.bytes_before / 1e6:.1f} MB → {res.bytes_after / 1e6:.1f} MB",
        "",
    ])


def main() -> None:
    parser = argparse.ArgumentParser(description="Compacta Parquet chicos por partición")
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--min-age-days", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    setup_logging(args.verbose)
    res = run(resolve_data_dir(args.data_dir), args.min_age_days, args.dry_run)
    text = render(res, args.min_age_days, args.dry_run)
    print(text)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    main()
