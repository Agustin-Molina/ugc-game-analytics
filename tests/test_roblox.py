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
    c["roblox"].update(games_ids_per_s=1e6, votes_ids_per_s=1e6, rate_explore_per_s=1000,
                       rate_search_per_s=1000)
    return c


SEARCH = "https://apis.roblox.com/search-api/omni-search"
EMPTY_SEARCH = {"searchResults": [], "nextPageToken": None}


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
    respx.get(SEARCH).mock(return_value=httpx.Response(200, json=EMPTY_SEARCH))
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

    # primera corrida: sin eventos; todos vienen de charts → hot, con su tier_history
    assert not list((tmp_path / "roblox").glob("events/**/*.parquet"))
    assert set(dim["tier"]) == {"hot"}
    th = pl.read_parquet(tmp_path / "roblox/tier_history/**/*.parquet")
    assert th.height == 3 and set(th["reason"]) == {"chart"}
    assert th["from_tier"].is_null().all()
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


def test_keywords_due_spreads_over_slots():
    kws = [f"k{i}" for i in range(58)]
    t0 = datetime(2026, 11, 10, tzinfo=UTC)
    seen = [k for h in range(24)
            for k in roblox.keywords_due(kws, t0 + h * roblox.timedelta(hours=1), 24)]
    assert sorted(seen) == sorted(kws)  # cada keyword exactamente una vez por día
    assert roblox.load_keywords()  # config/keywords.yaml válido


@respx.mock
async def test_search_discovers_untracked_games(tmp_path, cfg):
    cfg["roblox"].update(search_slots=1, search_max_pages=15)
    mock_apis()

    def search(request: httpx.Request) -> httpx.Response:
        assert request.url.params["searchQuery"] == "tycoon"
        page = request.url.params.get("pageToken")
        return httpx.Response(200, json=load("roblox_search_p2.json" if page else
                                             "roblox_search_p1.json"))

    def games(request: httpx.Request) -> httpx.Response:  # inventa detalles para ids nuevos
        ids = [int(x) for x in request.url.params["universeIds"].split(",")]
        base = load("roblox_games.json")["data"]
        known = {g["id"]: g for g in base}
        data = [known.get(i) or {**base[2], "id": i, "name": f"g{i}"} for i in ids]
        return httpx.Response(200, json={"data": data})

    respx.get(SEARCH).mock(side_effect=search)
    games_route = respx.get(url__regex=re.escape(GAMES) + r"\?").mock(side_effect=games)
    summary = await roblox.run(tmp_path, limit=None, dry_run=False, cfg=cfg,
                               keywords=["tycoon"])

    pos = pl.read_parquet(tmp_path / "roblox/search_positions/**/*.parquet").sort("rank")
    assert pos["rank"].to_list() == [1, 2, 3, 4]  # ranking continuo entre páginas
    assert set(pos["keyword"]) == {"tycoon"}

    queried = {int(x) for c in games_route.calls
               for x in c.request.url.params["universeIds"].split(",")}
    assert {5000000001, 5000000003} <= queried   # descubiertos con jugadores
    assert 5000000002 not in queried              # 0 jugadores → no se trackea

    dim = read_dimension(tmp_path, "roblox", roblox.GAMES_SCHEMA, "universe_id")
    via = dict(zip(dim["universe_id"].to_list(), dim["discovered_via"].to_list(), strict=True))
    assert via[5000000001] == "search:tycoon"
    assert via[383310974] == "chart:top-trending"  # chart tiene prioridad
    assert any("2 juegos nuevos a trackear" in n for n in summary.notes)


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
    respx.get(SEARCH).mock(return_value=httpx.Response(200, json=EMPTY_SEARCH))
    respx.get(url__regex=re.escape(GAMES) + r"\?").mock(return_value=httpx.Response(400))
    respx.get(VOTES).mock(side_effect=votes_response)
    summary = await roblox.run(tmp_path, limit=None, dry_run=True, cfg=cfg, keywords=[])
    assert summary.errors["games_http_400"] == 1
    assert summary.exit_code(0.2) == 1
