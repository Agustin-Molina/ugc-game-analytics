import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from collectors import fortnite
from collectors.common import load_config, read_dimension

FIX = Path(__file__).parent / "fixtures"
BASE = "https://api.fortnite.com/ecosystem/v1"
T0 = datetime(2026, 11, 9, 23, 30, tzinfo=UTC)
T1 = datetime(2026, 11, 10, 3, 40, tzinfo=UTC)


def metrics_day() -> dict:
    return json.loads((FIX / "fortnite_metrics_day.json").read_text(encoding="utf-8"))


def null_metrics() -> dict:
    ts = "2026-11-09T00:00:00.000Z"
    return {"plays": [{"value": None, "timestamp": ts}],
            "retention": [{"d1": None, "d7": None, "timestamp": ts}]}


def hour_metrics() -> dict:
    return {
        "peakCCU": [{"value": 60, "timestamp": "2026-11-09T21:00:00.000Z"},
                    {"value": 139, "timestamp": "2026-11-09T22:00:00.000Z"},
                    {"value": None, "timestamp": "2026-11-09T23:00:00.000Z"}],
        "plays": [{"value": 30, "timestamp": "2026-11-09T21:00:00.000Z"},
                  {"value": 70, "timestamp": "2026-11-09T22:00:00.000Z"},
                  {"value": None, "timestamp": "2026-11-09T23:00:00.000Z"}],
    }


def island(code: str, title: str, tags=("pvp",), category=None) -> dict:
    return {"code": code, "creatorCode": "someone", "title": title, "category": category,
            "createdIn": "UEFN", "tags": list(tags)}


@pytest.fixture
def cfg():
    c = load_config()
    c["http"].update(backoff_base_s=0.0, backoff_cap_s=0.0, throttle_min_pause_s=0.0)
    c["fortnite"].update(rate_per_s=10_000, n_groups=1, flush_every=2)
    return c


# --------------------------------------------------------------------------- parseo


def test_parse_metrics_day():
    snap = datetime(2026, 11, 9, 23, 16, tzinfo=UTC)
    rows = fortnite.parse_metrics("9782-7008-5547", metrics_day(), "day", snap)
    assert [r["bucket_start"].day for r in rows] == [8, 9]  # el 7 es todo null → descartado
    d8, d9 = rows
    assert d8["peak_ccu"] == 40 and d8["minutes_played"] == 1000
    assert d8["retention_d1"] == 0.12 and d8["retention_d7"] is None
    assert d8["is_partial"] is False
    assert d9["is_partial"] is True  # el día 9 no terminó a las 23:16
    assert d9["avg_minutes_per_player"] == 14.14
    assert all(r["snapshot_at"] == snap for r in rows)


def test_parse_metrics_hour_drops_null_buckets():
    snap = datetime(2026, 11, 9, 23, 16, tzinfo=UTC)
    rows = fortnite.parse_metrics("x", hour_metrics(), "hour", snap)
    assert [r["peak_ccu"] for r in rows] == [60, 139]
    assert not any(r["is_partial"] for r in rows)


def test_metrics_window_never_in_future():
    frm, to = fortnite.metrics_window(datetime(2026, 11, 9, 23, 16, 5, tzinfo=UTC), 7)
    assert frm == "2026-11-02T00:00:00Z"
    assert to == "2026-11-09T23:00:00Z"


# --------------------------------------------------------------------------- dimensión


def catalog(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame([fortnite.parse_island(r) for r in rows],
                        schema={k: fortnite.GAMES_SCHEMA[k]
                                for k in ["island_code", *fortnite.CATALOG_FIELDS]})


def empty_dim() -> pl.DataFrame:
    return pl.DataFrame(schema=fortnite.GAMES_SCHEMA)


def test_initial_load_has_no_events():
    cat = catalog([island("A", "a"), island("B", "b")])
    dim, events = fortnite.update_dimension_with_catalog(empty_dim(), cat, T0, complete=True)
    assert dim.height == 2 and events.is_empty()
    assert dim["is_listed"].to_list() == [True, True]
    assert dim["first_seen_at"].to_list() == [T0, T0]
    assert dim["tier"].to_list() == ["warm", "warm"]


def test_events_new_removed_title_tags():
    cat0 = catalog([island("A", "a", ("pvp", "1v1")), island("B", "b"), island("C", "c")])
    dim0, _ = fortnite.update_dimension_with_catalog(empty_dim(), cat0, T0, complete=True)
    cat1 = catalog([
        island("A", "a [UPD]", ("1v1", "pvp")),       # título cambia; tags iguales (otro orden)
        island("B", "b", ("pvp", "tycoon")),          # tags cambian
        island("D", "d", category="LEGO"),            # nueva
    ])                                                # C desaparece
    dim1, ev = fortnite.update_dimension_with_catalog(dim0, cat1, T1, complete=True)
    got = {(r["island_code"], r["event_type"]): (r["old_value"], r["new_value"])
           for r in ev.iter_rows(named=True)}
    assert got == {
        ("A", "title_change"): ("a", "a [UPD]"),
        ("B", "tags_change"): ("pvp", "pvp|tycoon"),
        ("D", "island_new"): (None, "d"),
        ("C", "island_removed"): ("c", None),
    }
    by = {r["island_code"]: r for r in dim1.iter_rows(named=True)}
    assert by["C"]["is_listed"] is False and by["C"]["title"] == "c"
    assert by["A"]["first_seen_at"] == T0
    assert by["D"]["first_seen_at"] == T1

    # vuelve C → island_relisted
    cat2 = pl.concat([cat1, catalog([island("C", "c")])])
    dim2, ev2 = fortnite.update_dimension_with_catalog(dim1, cat2, T1, complete=True)
    assert ev2.filter(pl.col("island_code") == "C")["event_type"].to_list() == ["island_relisted"]
    assert dim2.filter(pl.col("island_code") == "C")["is_listed"].item() is True


def test_incomplete_catalog_does_not_remove():
    cat0 = catalog([island("A", "a"), island("B", "b")])
    dim0, _ = fortnite.update_dimension_with_catalog(empty_dim(), cat0, T0, complete=True)
    dim1, ev = fortnite.update_dimension_with_catalog(dim0, catalog([island("A", "a")]), T1,
                                                      complete=False)
    assert ev.is_empty()
    assert dim1.filter(pl.col("island_code") == "B")["is_listed"].item() is True


def test_select_today_prioritizes_active():
    dim = pl.DataFrame(
        {"island_code": ["A", "B", "C", "D", "E"], "is_listed": [True, True, True, False, True],
         "first_traction_at": [None, T0, None, T1, T1]},
    )
    assert fortnite.select_today(dim, 0, 1) == ["E", "B", "A", "C"]


# --------------------------------------------------------------------------- end to end


@respx.mock
async def test_run_end_to_end(tmp_path, cfg):
    page1 = {"links": {"next": "/x"}, "meta": {"page": {"nextCursor": "CUR1"}},
             "data": [island("1111-1111-1111", "hot one"), island("2222-2222-2222", "dead")]}
    page2 = {"links": {"next": None}, "meta": {"page": {"nextCursor": None}},
             "data": [island("3333-3333-3333", "gone")]}

    def islands(request: httpx.Request) -> httpx.Response:
        assert request.url.params["size"] == "1000"
        return httpx.Response(200, json=page2 if request.url.params.get("after") == "CUR1"
                              else page1)

    respx.get(f"{BASE}/islands").mock(side_effect=islands)
    day_hot = respx.get(f"{BASE}/islands/1111-1111-1111/metrics/day").mock(
        return_value=httpx.Response(200, json=metrics_day()))
    hour_hot = respx.get(f"{BASE}/islands/1111-1111-1111/metrics/hour").mock(
        return_value=httpx.Response(200, json=hour_metrics()))
    respx.get(f"{BASE}/islands/2222-2222-2222/metrics/day").mock(
        return_value=httpx.Response(200, json=null_metrics()))
    respx.get(f"{BASE}/islands/3333-3333-3333/metrics/day").mock(
        return_value=httpx.Response(404, json={"errorCode": "not_found"}))

    summary = await fortnite.run(tmp_path, cfg, group=0)

    params = day_hot.calls[0].request.url.params
    assert params["from"].endswith("T00:00:00Z") and params["to"].endswith(":00:00Z")
    assert hour_hot.call_count == 1  # peak 139 >= 50

    snaps = pl.read_parquet(tmp_path / "fortnite/snapshots/**/*.parquet")
    assert snaps.filter(pl.col("interval") == "day").height == 2
    assert snaps.filter(pl.col("interval") == "hour").height == 2
    assert set(snaps["island_code"]) == {"1111-1111-1111"}

    flog = pl.read_parquet(tmp_path / "fortnite/fetch_log/**/*.parquet")
    status = {(r["island_code"], r["interval"]): r["status"] for r in flog.iter_rows(named=True)}
    assert status == {
        ("1111-1111-1111", "day"): "ok",
        ("1111-1111-1111", "hour"): "ok",
        ("2222-2222-2222", "day"): "ok",
        ("3333-3333-3333", "day"): "not_found",
    }

    dim = read_dimension(tmp_path, "fortnite", fortnite.GAMES_SCHEMA, "island_code")
    hot = dim.filter(pl.col("island_code") == "1111-1111-1111").row(0, named=True)
    assert hot["first_traction_at"] == datetime(2026, 11, 9, tzinfo=UTC)  # 139 >= 50; el 8 = 40
    dead = dim.filter(pl.col("island_code") == "2222-2222-2222").row(0, named=True)
    assert dead["first_traction_at"] is None
    # log: carga inicial (3 filas) + solo la isla que ganó tracción (1 fila)
    logs = [pl.read_parquet(f) for f in (tmp_path / "fortnite/games").glob("date=*/*.parquet")]
    assert [d.height for d in sorted(logs, key=lambda d: d["valid_from"].min())] == [3, 1]

    assert summary.ops_failed == 0  # 404 no cuenta como error
    assert summary.exit_code(0.2) == 0
    # flush_every=2 → al menos 2 archivos de snapshots/fetch_log
    assert len(list((tmp_path / "fortnite/fetch_log").glob("**/*.parquet"))) >= 2


@respx.mock
async def test_limited_run_does_not_persist_dimension(tmp_path, cfg):
    page = {"links": {"next": "/x"}, "meta": {"page": {"nextCursor": "CUR1"}},
            "data": [island("1111-1111-1111", "hot one"), island("2222-2222-2222", "x")]}
    respx.get(f"{BASE}/islands").mock(return_value=httpx.Response(200, json=page))
    respx.get(url__regex=rf"{BASE}/islands/.+/metrics/day").mock(
        return_value=httpx.Response(200, json=metrics_day()))
    respx.get(url__regex=rf"{BASE}/islands/.+/metrics/hour").mock(
        return_value=httpx.Response(200, json=hour_metrics()))

    summary = await fortnite.run(tmp_path, cfg, group=0, limit=2)

    assert list((tmp_path / "fortnite").glob("snapshots/**/*.parquet"))
    # un catálogo recortado no puede pasar por carga inicial de la dimensión
    assert not list((tmp_path / "fortnite").glob("games/**/*.parquet"))
    assert not list((tmp_path / "fortnite").glob("events/**/*.parquet"))
    assert any("no se persisten" in n for n in summary.notes)


@respx.mock
async def test_hourly_threshold_is_configurable(tmp_path, cfg):
    cfg["fortnite"]["hourly_min_peak_ccu"] = 500
    respx.get(f"{BASE}/islands/1111-1111-1111/metrics/day").mock(
        return_value=httpx.Response(200, json=metrics_day()))
    hour = respx.get(f"{BASE}/islands/1111-1111-1111/metrics/hour").mock(
        return_value=httpx.Response(200, json=hour_metrics()))
    await fortnite.run(tmp_path, cfg, codes_override=["1111-1111-1111"], dry_run=True)
    assert hour.call_count == 0


@respx.mock
async def test_server_errors_count_toward_error_rate(tmp_path, cfg):
    cfg["http"]["max_retries"] = 1
    respx.get(f"{BASE}/islands/1111-1111-1111/metrics/day").mock(
        return_value=httpx.Response(503))
    summary = await fortnite.run(tmp_path, cfg, codes_override=["1111-1111-1111"],
                                 dry_run=True)
    assert summary.ops_failed == 1
    assert summary.exit_code(0.2) == 1
