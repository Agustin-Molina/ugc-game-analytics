"""Infraestructura compartida por los colectores: HTTP con reintentos, rate limiting,
escritura de Parquet particionado y resumen de corrida."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "collectors.yaml"
TS = pl.Datetime("us", "UTC")

log = logging.getLogger("collectors")


# --------------------------------------------------------------------------- utilidades


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
    )
    logging.Formatter.converter = time.gmtime
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def utcnow() -> datetime:
    return datetime.now(UTC)


def floor_hour(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def floor_day(ts: datetime) -> datetime:
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_ts(value: str | None) -> datetime | None:
    """Parsea ISO-8601 (con 'Z' o con offset) a datetime UTC tz-aware."""
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso_z(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def group_of(key: str, n: int = 5) -> int:
    """Grupo estable en [0, n) a partir de un sha1 (no usar hash(): está salado por proceso)."""
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % n


def group_for_date(day: datetime, n: int = 5) -> int:
    return day.date().toordinal() % n


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def resolve_data_dir(cli_value: str | None = None) -> Path:
    raw = cli_value or os.environ.get("UGC_DATA_DIR") or str(ROOT / "data")
    path = Path(raw)
    return path if path.is_absolute() else (Path.cwd() / path)


def new_run_id() -> str:
    gh = os.environ.get("GITHUB_RUN_ID")
    return f"gh{gh}" if gh else uuid.uuid4().hex[:8]


# --------------------------------------------------------------------------- rate limiting


class RateLimiter:
    """Token bucket async muy simple (un slot cada 1/rate segundos).

    Si `adaptive`, baja la tasa un 20 % ante cada 429 y la recupera de a 10 % cada
    `recover_after` respuestas OK seguidas, sin pasar de la tasa inicial.
    """

    def __init__(self, rate: float, *, adaptive: bool = False, recover_after: int = 200):
        self.max_rate = float(rate)
        self.rate = float(rate)
        self.min_rate = rate / 10
        self.adaptive = adaptive
        self.recover_after = recover_after
        self._next = 0.0
        self._paused_until = 0.0
        self._ok_streak = 0
        self._lock = asyncio.Lock()
        self.throttles = 0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            slot = max(now, self._next, self._paused_until)
            self._next = slot + 1.0 / self.rate
        delay = slot - now
        if delay > 0:
            await asyncio.sleep(delay)

    def on_success(self) -> None:
        if not self.adaptive:
            return
        self._ok_streak += 1
        if self._ok_streak >= self.recover_after and self.rate < self.max_rate:
            self.rate = min(self.max_rate, self.rate * 1.1)
            self._ok_streak = 0

    def on_throttle(self, pause_s: float) -> None:
        self.throttles += 1
        self._ok_streak = 0
        if self.adaptive:
            self.rate = max(self.min_rate, self.rate * 0.8)
        self._paused_until = max(self._paused_until, time.monotonic() + pause_s)


# --------------------------------------------------------------------------- HTTP


class HttpError(Exception):
    """Respuesta 4xx no reintentable (ej. 404) o reintentos agotados."""

    def __init__(self, status: int | None, url: str, detail: str = "", kind: str = ""):
        self.status = status
        self.url = url
        self.detail = detail
        self.kind = kind or (f"http_{status}" if status else "network")
        super().__init__(f"{self.kind} {url} {detail[:200]}")


@dataclass
class HttpStats:
    requests: int = 0
    retries: int = 0
    by_status: Counter = field(default_factory=Counter)


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    for header in ("retry-after", "x-ratelimit-reset"):
        raw = resp.headers.get(header)
        if raw:
            try:
                return max(0.0, float(raw.split(",")[0].strip()))
            except ValueError:
                continue
    return None


class Http:
    """Cliente HTTP async con reintentos (backoff exponencial + full jitter)."""

    def __init__(self, cfg: dict[str, Any], client: httpx.AsyncClient | None = None):
        hcfg = cfg["http"]
        self.max_retries = int(hcfg["max_retries"])
        self.base = float(hcfg["backoff_base_s"])
        self.cap = float(hcfg["backoff_cap_s"])
        self.min_pause = float(hcfg["throttle_min_pause_s"])
        self.stats = HttpStats()
        self.client = client or httpx.AsyncClient(
            headers={"User-Agent": cfg["user_agent"], "Accept": "application/json"},
            timeout=float(hcfg["timeout_s"]),
            follow_redirects=True,
        )

    async def __aenter__(self) -> Http:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.client.aclose()

    def _backoff(self, attempt: int) -> float:
        return random.uniform(0, min(self.cap, self.base * 2**attempt))

    async def get_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        limiter: RateLimiter | None = None,
    ) -> Any:
        last_kind, last_status, last_detail = "network", None, ""
        for attempt in range(self.max_retries + 1):
            if limiter:
                await limiter.acquire()
            self.stats.requests += 1
            try:
                resp = await self.client.get(url, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_kind, last_status, last_detail = type(exc).__name__, None, str(exc)
                self.stats.by_status[last_kind] += 1
                wait = self._backoff(attempt)
            else:
                status = resp.status_code
                self.stats.by_status[status] += 1
                if status == 200:
                    if limiter:
                        limiter.on_success()
                    return resp.json()
                if status != 429 and status < 500:
                    raise HttpError(status, str(resp.url), resp.text)
                last_kind, last_status, last_detail = f"http_{status}", status, resp.text
                wait = _retry_after_seconds(resp)
                if wait is None:
                    wait = self._backoff(attempt)
                if status == 429:
                    wait = max(wait, self.min_pause * random.uniform(1, 3))
                    if limiter:
                        limiter.on_throttle(wait)
            if attempt == self.max_retries:
                break
            self.stats.retries += 1
            log.debug("retry %s in %.1fs (%s) %s", attempt + 1, wait, last_kind, url)
            await asyncio.sleep(wait)
        raise HttpError(last_status, url, last_detail, kind=f"exhausted_{last_kind}")


# --------------------------------------------------------------------------- Parquet


def _atomic_write(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.write_parquet(tmp, compression="zstd", compression_level=9, statistics=True)
    os.replace(tmp, path)


def write_partition(
    df: pl.DataFrame,
    data_dir: Path,
    platform: str,
    table: str,
    partition_ts: datetime,
    run_id: str,
    part: int | None = None,
) -> Path | None:
    """Escribe `<data_dir>/<platform>/<table>/date=YYYY-MM-DD/<HHMMSS>-<run_id>[-pN].parquet`.

    Append-only: nunca sobreescribe un archivo existente.
    """
    if df.is_empty():
        return None
    ts = partition_ts.astimezone(UTC)
    suffix = f"-p{part:03d}" if part is not None else ""
    path = (
        data_dir
        / platform
        / table
        / f"date={ts:%Y-%m-%d}"
        / f"{ts:%H%M%S}-{run_id}{suffix}.parquet"
    )
    if path.exists():
        raise FileExistsError(path)
    _atomic_write(df, path)
    return path


# --------------------------------------------------------------------------- dimensiones
#
# Las dimensiones (`games`) se guardan como LOG APPEND-ONLY: cada corrida escribe solo las
# filas nuevas o con algún atributo cambiado, con `valid_from` = momento de la escritura.
# Estado actual = última fila por id (mayor `valid_from`). Así git no acumula una copia
# completa de la dimensión en cada corrida. Ver pipeline/db.py para la vista en DuckDB.

DIM_TABLE = "games"
VALID_FROM = "valid_from"


def _conform(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Alinea un archivo viejo al esquema actual (agrega columnas nuevas como null y
    descarta las que ya no existen)."""
    missing = [pl.lit(None, dtype=t).alias(c) for c, t in schema.items() if c not in df.columns]
    if missing:
        df = df.with_columns(missing)
    return df.select(list(schema)).cast(schema)


def read_dimension(
    data_dir: Path,
    platform: str,
    schema: dict[str, pl.DataType],
    key: str | list[str],
    table: str = DIM_TABLE,
) -> pl.DataFrame:
    """Estado actual de un log de cambios (`games` u otro): última fila por `key` según
    `valid_from`. `schema` describe los atributos (sin `valid_from`)."""
    files = sorted((data_dir / platform / table).glob("date=*/*.parquet"))
    if not files:
        return pl.DataFrame(schema=schema)
    full_schema = {**schema, VALID_FROM: TS}
    log_df = pl.concat([_conform(pl.read_parquet(f), full_schema) for f in files])
    return (
        log_df.sort(VALID_FROM, maintain_order=True)
        .unique(key, keep="last", maintain_order=True)
        .drop(VALID_FROM)
        .sort(key)
    )


def dimension_changes(
    old: pl.DataFrame, new: pl.DataFrame, key: str | list[str]
) -> pl.DataFrame:
    """Filas de `new` que no existen en `old` o con algún atributo distinto (null-safe)."""
    if old.is_empty():
        return new
    keys = [key] if isinstance(key, str) else key
    attrs = [c for c in new.columns if c not in keys]
    old_marked = old.select(new.columns).with_columns(pl.lit(True).alias("__in_old"))
    joined = new.join(old_marked, on=key, how="left", suffix="__old")
    changed = pl.any_horizontal([pl.col(c).ne_missing(pl.col(f"{c}__old")) for c in attrs])
    return joined.filter(pl.col("__in_old").is_null() | changed).select(new.columns)


def write_dimension_changes(
    old: pl.DataFrame,
    new: pl.DataFrame,
    key: str | list[str],
    data_dir: Path,
    platform: str,
    partition_ts: datetime,
    run_id: str,
    part: int | None = None,
    table: str = DIM_TABLE,
) -> tuple[Path | None, int]:
    """Agrega al log (`games` u otro) solo los cambios de `new` respecto de `old`."""
    changes = dimension_changes(old, new, key)
    if changes.is_empty():
        return None, 0
    changes = changes.with_columns(pl.lit(utcnow(), dtype=TS).alias(VALID_FROM))
    path = write_partition(changes, data_dir, platform, table, partition_ts, run_id, part)
    return path, changes.height


def frame(rows: list[dict[str, Any]], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Lista de dicts → DataFrame con esquema explícito (claves faltantes = null)."""
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema, strict=False)


# --------------------------------------------------------------------------- resumen


@dataclass
class RunSummary:
    platform: str
    run_id: str
    started_at: datetime = field(default_factory=utcnow)
    rows: Counter = field(default_factory=Counter)
    files: list[str] = field(default_factory=list)
    errors: Counter = field(default_factory=Counter)
    ops_total: int = 0
    ops_failed: int = 0
    notes: list[str] = field(default_factory=list)
    http: HttpStats | None = None
    finished_at: datetime | None = None

    def add_file(self, table: str, path: Path | None, n_rows: int) -> None:
        self.rows[table] += n_rows
        if path is not None:
            self.files.append(str(path))

    def error(self, kind: str) -> None:
        self.errors[kind] += 1

    @property
    def error_rate(self) -> float:
        return self.ops_failed / self.ops_total if self.ops_total else 0.0

    def exit_code(self, max_error_rate: float) -> int:
        if sum(self.rows.values()) == 0:
            return 1
        return 1 if self.error_rate > max_error_rate else 0

    def render(self) -> str:
        end = self.finished_at or utcnow()
        dur = end - self.started_at
        lines = [
            f"## {self.platform} — run `{self.run_id}`",
            "",
            f"- inicio: {iso_z(self.started_at)} · duración: {_fmt_td(dur)}",
            f"- operaciones: {self.ops_total} · fallidas: {self.ops_failed} "
            f"({self.error_rate:.1%})",
        ]
        if self.http:
            statuses = ", ".join(f"{k}: {v}" for k, v in sorted(self.http.by_status.items(),
                                                                  key=lambda kv: str(kv[0])))
            lines.append(
                f"- requests: {self.http.requests} · reintentos: {self.http.retries} "
                f"· status: {statuses}"
            )
        lines += ["", "| tabla | filas |", "|---|---:|"]
        lines += [f"| {t} | {n} |" for t, n in sorted(self.rows.items())]
        if self.errors:
            lines += ["", "**Errores:** " + ", ".join(f"{k}={v}" for k, v in self.errors.items())]
        if self.notes:
            lines += ["", *[f"- {n}" for n in self.notes]]
        return "\n".join(lines) + "\n"

    def emit(self) -> None:
        self.finished_at = self.finished_at or utcnow()
        text = self.render()
        print(text)
        step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if step_summary:
            with open(step_summary, "a", encoding="utf-8") as f:
                f.write(text + "\n")


def _fmt_td(td: timedelta) -> str:
    secs = int(td.total_seconds())
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"
