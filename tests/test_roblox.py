import json
import re
from datetime import UTC, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from collectors import roblox
from collectors.common import load_config, read_dimension

FIX = Path(__file__).parent / "fixtures"
SORTS = "https://apis.roblox.com/explore-api/v1/get-sorts"
GAMES = "https://games.roblox.com/v1/games"
VOTES = "https://games.roblox.com/v1/games/votes"


def load(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


@pytest.fixture
def cfg():
    c = load_config()
    c["http"].update(backoff_base_s=0.0, backoff_cap_s=0.0, throttle_min_pause_s=0.0)
    c["roblox"].update(games_ids_per_s=1e6, votes_ids_per_s=1e6, rate_explore_per_s=1000)
    return c


def votes_response(request: httpx.Request) -> httpx.Response:
    ids = [int(x) for x in request.url.params["universeIds"].split(",")]
    return httpx.Response(
        200, json={"data": [{"id": i, "upVotes": i % 1000, "downVotes": 1} for i in ids]}
    )


def games_response(request: httpx.Request) -> httpx.Response:
    ids = {int(x) for x in request.url.params["universeIds"].split(",")}
    data = [g for g in load("roblox_games.json")["data"] if g["id"] in ids]
    return httpx.Response(200, json={"data": data})


def mock_apis():
    def sorts(request: httpx.Request) -> httpx.Response:
        assert request.url.params["device"] == "all"
        assert request.url.params["country"] == "all"
        page = request.url.params.get("sortsPageToken")
        return httpx.Response(200, json=load("roblox_sorts_p2.json" if page else
                                             "roblox_sorts_p1.json"))

    respx.get(SORTS).mock(side_effect=sorts)
    games = respx.get(url__regex=re.escape(GAMES) + r"\?").mock(side_effect=games_response)
    votes = respx.get(VOTES).mock(side_effect=votes_response)
    return games, votes


def test_parse_sorts_page_skips_non_games():
    ts = datetime(2026, 11, 9, tzinfo=UTC)
    rows, meta = roblox.parse_sorts_page(load("roblox_sorts_p2.json"), ts)
    assert [(r["sort_id"], r["rank"], r["universe_id"]) for r in rows] == [
        ("up-and-coming", 1, 994732206),
        ("up-and-coming", 2, 383310974),
    ]
    assert rows[0]["is_sponsored"] is True
    assert meta[994732206]["age_rating"] == "mild"


@respx.mock
async def test_run_end_to_end(tmp_path, cfg):
    games_route, votes_route = mock_apis()
    summary = await roblox.run(tmp_path, limit=None, dry_run=False, cfg=cfg)

    charts = pl.read_parquet(tmp_path / "roblox/chart_positions/**/*.parquet")
    assert charts.height == 4
    assert set(charts["sort_id"]) == {"top-trending", "up-and-coming"}

    snaps = pl.read_parquet(tmp_path / "roblox/snapshots/**/*.parquet")
    assert snaps.height == 3
    assert snaps.schema["snapshot_at"] == pl.Datetime("us", "UTC")
    adopt = snaps.filter(pl.col("universe_id") == 383310974).row(0, named=True)
    assert adopt["playing"] == 150671
    assert adopt["visits"] == 45372596152
    assert adopt["up_votes"] == 383310974 % 1000

    dim = read_dimension(tmp_path, "roblox", roblox.GAMES_SCHEMA, "universe_id")
    assert dim.height == 3
    adopt_dim = dim.filter(pl.col("universe_id") == 383310974).row(0, named=True)
    assert adopt_dim["genre_l2"] == "Pet Care"
    assert adopt_dim["creator_verified"] is True
    assert adopt_dim["discovered_via"] == "chart:top-trending"
    assert adopt_dim["first_traction_at"] is not None
    small = dim.filter(pl.col("universe_id") == 10766972131).row(0, named=True)
    assert small["first_traction_at"] is None  # playing 42 < 100

    # primera corrida: sin eventos
    assert not list((tmp_path / "roblox").glob("events/**/*.parquet"))
    assert summary.exit_code(0.2) == 0
    assert games_route.call_count == 1 and votes_route.call_count == 1


@respx.mock
async def test_second_run_detects_events_and_keeps_first_seen(tmp_path, cfg):
    mock_apis()
    await roblox.run(tmp_path, limit=None, dry_run=False, cfg=cfg)
    dim1 = read_dimension(tmp_path, "roblox", roblox.GAMES_SCHEMA, "universe_id")

    changed = load("roblox_games.json")
    changed["data"][0]["name"] = "[🔥] Adopt Me!"
    changed["data"][0]["updated"] = "2026-11-10T01:00:00Z"
    respx.get(url__regex=re.escape(GAMES) + r"\?").mock(
        return_value=httpx.Response(200, json=changed)
    )
    await roblox.run(tmp_path, limit=None, dry_run=False, cfg=cfg)

    events = pl.read_parquet(tmp_path / "roblox/events/**/*.parquet")
    assert sorted(events["event_type"].to_list()) == ["name_change", "update"]
    name_ev = events.filter(pl.col("event_type") == "name_change").row(0, named=True)
    assert name_ev["old_value"] == "[⚡] Adopt Me!"
    assert name_ev["new_value"] == "[🔥] Adopt Me!"

    dim2 = read_dimension(tmp_path, "roblox", roblox.GAMES_SCHEMA, "universe_id")
    assert dim2["first_seen_at"].to_list() == dim1["first_seen_at"].to_list()
    assert len(list((tmp_path / "roblox/snapshots").glob("**/*.parquet"))) == 2

    # log append-only: la segunda corrida solo agrega la fila que cambió
    logs = [pl.read_parquet(f) for f in (tmp_path / "roblox/games").glob("date=*/*.parquet")]
    assert [d.height for d in sorted(logs, key=lambda d: d["valid_from"].min())] == [3, 1]
    assert dim2.filter(pl.col("universe_id") == 383310974)["name"].item() == "[🔥] Adopt Me!"


@respx.mock
async def test_batches_respect_api_limits(tmp_path, cfg):
    respx.get(SORTS).mock(return_value=httpx.Response(200, json={"sorts": []}))
    games_route = respx.get(url__regex=re.escape(GAMES) + r"\?").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    votes_route = respx.get(VOTES).mock(side_effect=votes_response)
    ids = list(range(1, 131))
    summary = roblox.RunSummary("roblox", "t")
    async with roblox.Http(cfg) as http:
        await roblox.fetch_details_and_votes(http, cfg, ids, summary)
    sizes = sorted(len(c.request.url.params["universeIds"].split(","))
                   for c in games_route.calls)
    assert sizes == [30, 50, 50]
    vsizes = sorted(len(c.request.url.params["universeIds"].split(","))
                    for c in votes_route.calls)
    assert vsizes == [30, 100]


@respx.mock
async def test_failed_batch_counts_as_error(tmp_path, cfg):
    respx.get(SORTS).mock(return_value=httpx.Response(200, json=load("roblox_sorts_p2.json")))
    respx.get(url__regex=re.escape(GAMES) + r"\?").mock(return_value=httpx.Response(400))
    respx.get(VOTES).mock(side_effect=votes_response)
    summary = await roblox.run(tmp_path, limit=None, dry_run=True, cfg=cfg)
    assert summary.errors["games_http_400"] == 1
    assert summary.exit_code(0.2) == 1
