import base64
import json
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from collectors import roblox_search
from collectors.common import load_config

FIX = Path(__file__).parent / "fixtures"
SEARCH = "https://apis.roblox.com/search-api/omni-search"


def load(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


@pytest.fixture
def cfg():
    c = load_config()
    c["http"].update(backoff_base_s=0.0, backoff_cap_s=0.0, throttle_min_pause_s=0.0)
    c["roblox"]["rate_search_per_s"] = 1e6
    return c


def full_page(keyword: str, start: int, last: bool) -> dict:
    return {
        "searchResults": [
            {"contentGroupType": "Game", "contents": [{
                "universeId": 10_000 * (start + i + 1) + len(keyword), "playerCount": i,
                "rootPlaceId": 1, "contentMaturity": "minimal", "contentType": "Game"}]}
            for i in range(40)
        ],
        "nextPageToken": None if last else roblox_search.page_token(start + 40),
    }


def test_page_token_is_stateless_base64():
    tok = roblox_search.page_token(80)
    assert json.loads(base64.b64decode(tok)) == {"start": 80, "count": 40, "endOfPage": False}


def test_parse_search_page_ranks_continue():
    rows = roblox_search.parse_search_page(load("roblox_search_p1.json"), "tycoon",
                                           roblox_search.utcnow(), 40)
    assert [(r["rank"], r["universe_id"]) for r in rows] == [(41, 5000000001), (42, 383310974)]
    assert rows[0]["age_rating"] == "minimal"


@respx.mock
async def test_sweep_is_breadth_first_and_stops_when_exhausted(tmp_path, cfg):
    calls: list[tuple[str, int]] = []

    def search(request: httpx.Request) -> httpx.Response:
        kw = request.url.params["searchQuery"]
        tok = request.url.params.get("pageToken")
        start = json.loads(base64.b64decode(tok))["start"] if tok else 0
        calls.append((kw, start))
        # "a" tiene 3 páginas; "b" se termina en la primera
        return httpx.Response(200, json=full_page(kw, start, last=(kw == "b" or start == 80)))

    respx.get(SEARCH).mock(side_effect=search)
    summary = await roblox_search.run(tmp_path, cfg, keywords=["a", "b"], max_pages=5)

    assert calls == [("a", 0), ("b", 0), ("a", 40), ("a", 80)]  # en anchura
    df = pl.read_parquet(tmp_path / "roblox/search_positions/**/*.parquet")
    assert df.height == 4 * 40
    a = df.filter(pl.col("keyword") == "a").sort("rank")
    assert a["rank"].to_list() == list(range(1, 121))
    assert summary.exit_code(0.2) == 0


@respx.mock
async def test_sweep_respects_budget_and_keeps_partial_results(tmp_path, cfg):
    respx.get(SEARCH).mock(return_value=httpx.Response(200, json=full_page("x", 0, last=False)))
    summary = await roblox_search.run(tmp_path, cfg, keywords=["a", "b"], budget_minutes=0)
    assert any("presupuesto agotado" in n for n in summary.notes)
    assert not list(tmp_path.glob("roblox/search_positions/**/*.parquet")) or \
        pl.read_parquet(tmp_path / "roblox/search_positions/**/*.parquet").height == 0


@respx.mock
async def test_failed_page_is_retried_next_pass(tmp_path, cfg):
    cfg["http"]["max_retries"] = 0
    seen: list[tuple[str, int]] = []

    def search(request: httpx.Request) -> httpx.Response:
        kw = request.url.params["searchQuery"]
        tok = request.url.params.get("pageToken")
        start = json.loads(base64.b64decode(tok))["start"] if tok else 0
        seen.append((kw, start))
        if len(seen) == 1:
            return httpx.Response(500)
        return httpx.Response(200, json=full_page(kw, start, last=True))

    respx.get(SEARCH).mock(side_effect=search)
    summary = await roblox_search.run(tmp_path, cfg, keywords=["a"], max_pages=3)
    assert seen == [("a", 0), ("a", 40)]  # la falla se reintenta como página siguiente
    assert summary.ops_failed == 1
