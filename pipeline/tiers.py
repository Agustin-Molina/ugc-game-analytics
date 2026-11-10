"""Tiers de tracking (hot / warm / cold) con reglas configurables en `config/tiers.yaml`.

Dos piezas, que el colector usa en cada corrida:

- `select_due`: qué juegos consultar ahora según su tier (hot siempre; warm y cold repartidos
  por hash del id en slots horarios, más los atrasados).
- `assign_roblox_tiers`: recalcula el tier de cada juego con la historia reciente
  (snapshots + chart_positions). Sin estado extra en la dimensión: la histéresis sale de
  mirar `hold_hours` hacia atrás. Los cambios van a `tier_history`.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from collectors.common import ROOT, TS, group_of, load_config

TIERS_PATH = ROOT / "config" / "tiers.yaml"
HOT, WARM, COLD = "hot", "warm", "cold"

TIER_HISTORY_SCHEMA: dict[str, pl.DataType] = {
    "changed_at": TS,
    "universe_id": pl.Int64,
    "from_tier": pl.Utf8,
    "to_tier": pl.Utf8,
    "reason": pl.Utf8,
}


def load_rules(platform: str, path: Path = TIERS_PATH) -> dict[str, Any]:
    return load_config(path)[platform]


def read_recent(
    data_dir: Path, platform: str, table: str, columns: list[str], since: datetime
) -> pl.DataFrame | None:
    """Columnas de una tabla de hechos desde `since` (poda por partición `date=`)."""
    base = data_dir / platform / table
    first_day = since.date().isoformat()
    files = sorted(
        f
        for d in base.glob("date=*")
        if d.name.removeprefix("date=") >= first_day
        for f in d.glob("*.parquet")
    )
    if not files:
        return None
    frames = []
    for f in files:  # tolera archivos viejos sin alguna columna (evolución de esquema)
        present = [c for c in columns if c in pl.read_parquet_schema(f)]
        frames.append(pl.read_parquet(f, columns=present))
    df = pl.concat(frames, how="diagonal_relaxed")
    missing = [c for c in columns if c not in df.columns]
    df = df.with_columns(pl.lit(None).alias(c) for c in missing).select(columns)
    return df.filter(pl.col("snapshot_at") >= since)


# --------------------------------------------------------------------------- asignación


def _growth_signals(snaps: pl.DataFrame, hot: dict[str, Any]) -> pl.DataFrame:
    """Por cada observación: ¿creció >= growth_ratio respecto de la de ~24 h antes?"""
    obs = snaps.select("universe_id", "snapshot_at", "playing").drop_nulls().sort("snapshot_at")
    ref = obs.rename({"snapshot_at": "ref_at", "playing": "ref_playing"})
    left = obs.with_columns(
        (pl.col("snapshot_at") - timedelta(hours=hot["growth_min_lag_hours"])).alias("target")
    ).sort("target")
    joined = left.join_asof(
        ref, left_on="target", right_on="ref_at", by="universe_id", strategy="backward",
        check_sortedness=False,  # ambos ordenados globalmente por la clave → también por grupo
    )
    max_lag = timedelta(hours=hot["growth_max_lag_hours"])
    return joined.with_columns(
        (
            pl.col("ref_at").is_not_null()
            & ((pl.col("snapshot_at") - pl.col("ref_at")) <= max_lag)
            & (pl.col("playing") >= hot["growth_min_ccu"])
            & (pl.col("playing") >= hot["growth_ratio"] * pl.max_horizontal("ref_playing", 1))
        ).alias("growth")
    ).select("universe_id", "snapshot_at", "playing", "growth")


def assign_roblox_tiers(
    dim: pl.DataFrame,
    snaps: pl.DataFrame,
    charts: pl.DataFrame,
    now: datetime,
    rules: dict[str, Any],
) -> pl.DataFrame:
    """Tier y motivo de cada juego de la dimensión → (universe_id, tier, reason).

    `snaps` (universe_id, snapshot_at, playing) y `charts` (universe_id, snapshot_at) deben
    cubrir al menos `window_days` hacia atrás, incluida la corrida actual.
    """
    hot, warm = rules["hot"], rules["warm"]
    hold_from = now - timedelta(hours=rules["hold_hours"])
    key = "universe_id"

    obs = _growth_signals(snaps, hot)
    recent = (
        obs.filter(pl.col("snapshot_at") >= hold_from)
        .group_by(key)
        .agg(
            (pl.col("playing") >= hot["min_ccu"]).any().alias("sig_ccu"),
            pl.col("growth").any().alias("sig_growth"),
            pl.col("playing").max().alias("max_hold"),
        )
    )
    window = obs.group_by(key).agg(pl.col("playing").max().alias("max_window"))
    chart_hold = (
        charts.filter(pl.col("snapshot_at") >= hold_from).select(key).unique()
        .with_columns(pl.lit(True).alias("chart_hold"))
    )
    chart_warm = (
        charts.filter(pl.col("snapshot_at") >= now - timedelta(days=warm["chart_days"]))
        .select(key).unique().with_columns(pl.lit(True).alias("chart_warm"))
    )

    df = (
        dim.select(key, "created_at")
        .join(recent, on=key, how="left")
        .join(window, on=key, how="left")
        .join(chart_hold, on=key, how="left")
        .join(chart_warm, on=key, how="left")
    )
    is_new = (pl.col("created_at") >= now - timedelta(days=hot["new_max_age_days"])) & (
        pl.col("max_hold") >= hot["new_min_ccu"]
    )
    # (condición, tier, motivo) en orden de prioridad
    rules_list = [
        (pl.col("chart_hold").fill_null(False) & hot.get("chart", True), HOT, "chart"),
        (pl.col("sig_ccu").fill_null(False), HOT, f"ccu>={hot['min_ccu']}"),
        (pl.col("sig_growth").fill_null(False), HOT, "growth"),
        (is_new.fill_null(False), HOT, "new"),
        (pl.col("max_window") >= warm["min_ccu"], WARM, f"ccu>={warm['min_ccu']}"),
        (pl.col("chart_warm").fill_null(False), WARM, "chart_7d"),
        (pl.col("max_window").is_null(), COLD, "no_data"),
    ]
    tier_expr = pl.when(rules_list[0][0]).then(pl.lit(rules_list[0][1]))
    reason_expr = pl.when(rules_list[0][0]).then(pl.lit(rules_list[0][2]))
    for cond, tier, reason in rules_list[1:]:
        tier_expr = tier_expr.when(cond.fill_null(False)).then(pl.lit(tier))
        reason_expr = reason_expr.when(cond.fill_null(False)).then(pl.lit(reason))
    return df.select(
        key,
        tier_expr.otherwise(pl.lit(COLD)).alias("tier"),
        reason_expr.otherwise(pl.lit("inactive")).alias("reason"),
    )


def tier_changes(
    old_dim: pl.DataFrame, assigned: pl.DataFrame, now: datetime, key: str = "universe_id"
) -> pl.DataFrame:
    """Filas de `tier_history`: juegos nuevos (from_tier null) o con tier distinto."""
    old = old_dim.select(key, pl.col("tier").alias("from_tier"))
    changed = assigned.join(old, on=key, how="left").filter(
        pl.col("from_tier").is_null() | (pl.col("from_tier") != pl.col("tier"))
    )
    return changed.select(
        pl.lit(now).cast(TS).alias("changed_at"),
        pl.col(key).cast(pl.Int64),
        "from_tier",
        pl.col("tier").alias("to_tier"),
        "reason",
    ).sort(key)


# --------------------------------------------------------------------------- selección


def select_due(
    dim: pl.DataFrame,
    last_obs: pl.DataFrame,
    now: datetime,
    cadence: dict[str, Any],
    key: str = "universe_id",
) -> tuple[list[int], dict[str, int]]:
    """IDs a consultar en la corrida de `now` y conteo por motivo.

    hot: siempre. warm/cold: cuando el slot horario (hash del id) coincide con la hora de
    `now`, o si están atrasados (> overdue_factor × período sin observación).
    """
    hour = int(now.timestamp() // 3600)
    slots = {WARM: int(cadence["warm_slots"]), COLD: int(cadence["cold_slots"])}
    overdue_after = {t: timedelta(hours=n * float(cadence["overdue_factor"]))
                     for t, n in slots.items()}
    last = dict(zip(last_obs[key].to_list(), last_obs["last_at"].to_list(), strict=True))
    due: list[int] = []
    overdue: list[int] = []
    counts = {HOT: 0, WARM: 0, COLD: 0, "overdue": 0}
    for uid, tier in zip(dim[key].to_list(), dim["tier"].to_list(), strict=True):
        tier = tier if tier in slots or tier == HOT else WARM  # null/desconocido → warm
        if tier == HOT:
            due.append(uid)
            counts[HOT] += 1
        elif group_of(str(uid), slots[tier]) == hour % slots[tier]:
            due.append(uid)
            counts[tier] += 1
        elif (seen := last.get(uid)) is not None and now - seen > overdue_after[tier]:
            overdue.append(uid)
    overdue = overdue[: int(cadence["max_overdue_per_run"])]
    counts["overdue"] = len(overdue)
    return due + overdue, counts
