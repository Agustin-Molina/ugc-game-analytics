from collections import Counter
from datetime import UTC, datetime

import httpx
import polars as pl
import pytest
import respx

from collectors import common
from collectors.common import (
    Http,
    HttpError,
    RateLimiter,
    RunSummary,
    dimension_changes,
    group_of,
    parse_ts,
    read_dimension,
    write_dimension_changes,
    write_partition,
)


@pytest.fixture
def cfg():
    c = common.load_config()
    # tests rápidos: sin esperas reales
    c["http"].update(backoff_base_s=0.0, backoff_cap_s=0.0, throttle_min_pause_s=0.0,
                     max_retries=3)
    return c


def test_group_of_is_stable_and_balanced():
    assert group_of("2810-4003-9383") == group_of("2810-4003-9383")
    # valor fijo: si cambia el algoritmo, se reasignan todos los grupos (romper a propósito)
    assert group_of("2810-4003-9383", 5) == group_of("2810-4003-9383", 5) % 5
    counts = Counter(group_of(f"{i:04d}-0000-{i * 7 % 10000:04d}") for i in range(10_000))
    assert set(counts) == {0, 1, 2, 3, 4}
    assert max(counts.values()) - min(counts.values()) < 400


def test_parse_ts_is_utc():
    ts = parse_ts("2017-07-14T19:26:21.347Z")
    assert ts == datetime(2017, 7, 14, 19, 26, 21, 347000, tzinfo=UTC)
    assert parse_ts("2026-11-09T20:00:00-03:00") == datetime(2026, 11, 9, 23, tzinfo=UTC)
    assert parse_ts(None) is None


@respx.mock
async def test_get_json_retries_on_429_and_503(cfg):
    route = respx.get("https://x.test/a").mock(
        side_effect=[
            httpx.Response(429),
            httpx.Response(503),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    limiter = RateLimiter(1000, adaptive=True)
    async with Http(cfg) as http:
        assert await http.get_json("https://x.test/a", limiter=limiter) == {"ok": True}
        assert http.stats.retries == 2
    assert route.call_count == 3
    assert limiter.throttles == 1
    assert limiter.rate < 1000  # bajó la tasa por el 429


@respx.mock
async def test_get_json_does_not_retry_404(cfg):
    route = respx.get("https://x.test/missing").mock(return_value=httpx.Response(404))
    async with Http(cfg) as http:
        with pytest.raises(HttpError) as exc:
            await http.get_json("https://x.test/missing")
    assert exc.value.status == 404
    assert route.call_count == 1


@respx.mock
async def test_get_json_gives_up_after_max_retries(cfg):
    respx.get("https://x.test/down").mock(return_value=httpx.Response(500))
    async with Http(cfg) as http:
        with pytest.raises(HttpError) as exc:
            await http.get_json("https://x.test/down")
        assert http.stats.requests == cfg["http"]["max_retries"] + 1
    assert exc.value.kind == "exhausted_http_500"


@respx.mock
async def test_get_json_retries_network_errors(cfg):
    respx.get("https://x.test/flaky").mock(
        side_effect=[httpx.ConnectError("boom"), httpx.Response(200, json=[1])]
    )
    async with Http(cfg) as http:
        assert await http.get_json("https://x.test/flaky") == [1]


def test_write_partition_layout_and_append_only(tmp_path):
    df = pl.DataFrame({"a": [1, 2]})
    ts = datetime(2026, 11, 9, 23, 5, 7, tzinfo=UTC)
    path = write_partition(df, tmp_path, "roblox", "snapshots", ts, "abc")
    assert path == tmp_path / "roblox/snapshots/date=2026-11-09/230507-abc.parquet"
    assert pl.read_parquet(path).equals(df)
    with pytest.raises(FileExistsError):
        write_partition(df, tmp_path, "roblox", "snapshots", ts, "abc")
    assert write_partition(pl.DataFrame({"a": []}), tmp_path, "r", "t", ts, "x") is None
    p2 = write_partition(df, tmp_path, "roblox", "snapshots", ts, "abc", part=1)
    assert p2.name == "230507-abc-p001.parquet"


def test_partition_uses_utc_date(tmp_path):
    # 22:30 en Argentina (UTC-3) ya es el día siguiente en UTC
    from datetime import timedelta, timezone

    local = datetime(2026, 11, 9, 22, 30, tzinfo=timezone(timedelta(hours=-3)))
    path = write_partition(pl.DataFrame({"a": [1]}), tmp_path, "p", "t", local, "r")
    assert "date=2026-11-10" in str(path)


SCHEMA = {"id": pl.Int64, "name": pl.Utf8, "tags": pl.List(pl.Utf8)}


def dim(rows):
    return pl.DataFrame(rows, schema=SCHEMA)


def test_dimension_changes_is_null_safe_and_handles_lists():
    old = dim([{"id": 1, "name": "a", "tags": ["x"]}, {"id": 2, "name": None, "tags": []}])
    new = dim([
        {"id": 1, "name": "a", "tags": ["x"]},     # igual
        {"id": 2, "name": None, "tags": ["y"]},    # cambia lista (name null == null)
        {"id": 3, "name": "c", "tags": None},      # nuevo
    ])
    assert dimension_changes(old, new, "id")["id"].to_list() == [2, 3]
    assert dimension_changes(dim([]), new, "id").height == 3


def test_dimension_log_append_only_and_current_state(tmp_path):
    t0 = datetime(2026, 11, 9, 23, tzinfo=UTC)
    t1 = datetime(2026, 11, 10, 0, tzinfo=UTC)
    v1 = dim([{"id": 1, "name": "a", "tags": ["x"]}, {"id": 2, "name": "b", "tags": []}])
    _, n = write_dimension_changes(dim([]), v1, "id", tmp_path, "r", t0, "run1")
    assert n == 2
    v2 = v1.with_columns(pl.when(pl.col("id") == 2).then(pl.lit("b2")).otherwise("name")
                         .alias("name"))
    path, n = write_dimension_changes(v1, v2, "id", tmp_path, "r", t1, "run2")
    assert n == 1 and "date=2026-11-10" in str(path)
    # sin cambios → no escribe archivo
    assert write_dimension_changes(v2, v2, "id", tmp_path, "r", t1, "run3") == (None, 0)

    state = read_dimension(tmp_path, "r", SCHEMA, "id")
    assert state.to_dicts() == v2.to_dicts()
    files = list((tmp_path / "r/games").glob("date=*/*.parquet"))
    assert len(files) == 2
    assert "valid_from" in pl.read_parquet(files[0]).columns

    # evolución de esquema: columna nueva → null; columna eliminada → se descarta
    schema_v2 = {"id": pl.Int64, "name": pl.Utf8, "tier": pl.Utf8}
    evolved = read_dimension(tmp_path, "r", schema_v2, "id")
    assert evolved.columns == ["id", "name", "tier"]
    assert evolved["tier"].to_list() == [None, None]
    assert read_dimension(tmp_path, "otra", SCHEMA, "id").is_empty()


def test_run_summary_exit_code():
    s = RunSummary("roblox", "r1")
    assert s.exit_code(0.2) == 1  # 0 filas
    s.rows["snapshots"] = 10
    s.ops_total, s.ops_failed = 10, 1
    assert s.exit_code(0.2) == 0
    s.ops_failed = 3
    assert s.exit_code(0.2) == 1
    assert "snapshots" in s.render()
