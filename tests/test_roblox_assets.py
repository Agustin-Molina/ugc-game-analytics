import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from collectors import roblox_assets as ra
from collectors.common import group_of, load_config, read_dimension, write_dimension_changes
from pipeline.db import connect

FIX = Path(__file__).parent / "fixtures"
ICONS = "https://thumbnails.roblox.com/v1/games/icons"
THUMBS = "https://thumbnails.roblox.com/v1/games/multiget/thumbnails"
PASSES = "https://apis.roblox.com/game-passes/v1/universes/{}/game-passes"
A, B = 383310974, 5000000001


def load(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


@pytest.fixture
def cfg():
    c = load_config()
    c["http"].update(backoff_base_s=0.0, backoff_cap_s=0.0, throttle_min_pause_s=0.0,
                     max_retries=0)
    c["roblox"].update(rate_icons_per_s=1e6, rate_thumbnails_per_s=1e6,
                       rate_gamepasses_per_s=1e6)
    return c


def write_games(tmp_path, tiers: dict[int, str]) -> None:
    dim = pl.DataFrame({"universe_id": list(tiers), "tier": list(tiers.values())},
                       schema={"universe_id": pl.Int64, "tier": pl.Utf8})
    write_dimension_changes(dim.clear(), dim, "universe_id", tmp_path, "roblox",
                            datetime(2026, 11, 10, tzinfo=UTC), "g")


def test_icon_hash_from_both_url_formats():
    assert ra.icon_hash("https://tr.rbxcdn.com/180DAY-16972705ca6b7ee982e76269d5b887e8/512/512/"
                        "Image/Png/noFilter") == "16972705ca6b7ee982e76269d5b887e8"
    assert ra.icon_hash("https://t7.rbxcdn.com/180DAY-88edbd4c22e4a1b7ef17bb94d78ebde1") \
        == "88edbd4c22e4a1b7ef17bb94d78ebde1"
    assert ra.icon_hash(None) is None


def test_select_ids_cold_weekly():
    now = datetime(2026, 11, 11, tzinfo=UTC)
    ids = list(range(1, 200))
    dim = pl.DataFrame({"universe_id": ids, "tier": ["cold"] * 199})
    got = ra.select_ids(dim, now, 7)
    assert got == [u for u in ids if group_of(str(u), 7) == now.date().toordinal() % 7]
    assert 10 < len(got) < 50
    hot = pl.DataFrame({"universe_id": [1, 2, 3], "tier": ["hot", "warm", None]})
    assert ra.select_ids(hot, now, 7) == [1, 2, 3]


def mock_api(icons: dict, thumbs: dict, pages_a: list[dict], status_a: int = 200) -> None:
    respx.get(ICONS).mock(return_value=httpx.Response(200, json=icons))
    respx.get(THUMBS).mock(return_value=httpx.Response(200, json=thumbs))

    def passes_a(request: httpx.Request) -> httpx.Response:
        if status_a != 200:
            return httpx.Response(status_a)
        tok = request.url.params.get("pageToken")
        return httpx.Response(200, json=pages_a[1] if tok else pages_a[0])

    respx.get(PASSES.format(A)).mock(side_effect=passes_a)
    respx.get(PASSES.format(B)).mock(return_value=httpx.Response(
        404, json={"errorCode": "UniverseNotFound"}))


def pages() -> list[dict]:
    p1, p2 = load("roblox_gamepasses_p1.json"), load("roblox_gamepasses_p2.json")
    p2["nextPageToken"] = None
    return [p1, p2]


@respx.mock
async def test_run_tracks_changes_over_two_days(tmp_path, cfg):
    write_games(tmp_path, {A: "hot", B: "warm"})
    icons, thumbs = load("roblox_icons.json"), load("roblox_thumbnails.json")
    mock_api(icons, thumbs, pages())

    s1 = await ra.run(tmp_path, cfg)
    media = read_dimension(tmp_path, "roblox", ra.MEDIA_SCHEMA, "universe_id", table="media")
    assert media["universe_id"].to_list() == [A]  # B no vino en las miniaturas → no se guarda
    assert media["thumbnail_ids"].to_list() == [[105014675704626, 87733783441530,
                                                  98732815508759]]
    gp = read_dimension(tmp_path, "roblox", ra.GAMEPASS_SCHEMA, ra.PASS_KEY, table="gamepasses")
    assert gp.height == 4 and gp["is_listed"].all()  # 2 páginas de 2
    checks = pl.read_parquet(tmp_path / "roblox/asset_checks/**/*.parquet").sort("universe_id")
    assert checks["gamepasses_status"].to_list() == ["ok", "not_found"]
    assert checks["n_gamepasses"].to_list() == [4, 0]
    assert not list(tmp_path.glob("roblox/events/**/*.parquet"))  # primera carga
    assert s1.ops_failed == 0

    # día 2: cambia el ícono, se reordenan las miniaturas, la página 2 desaparece y un pase
    # cambia de precio
    respx.reset()
    icons2 = copy.deepcopy(icons)
    icons2["data"][0]["imageUrl"] = "https://tr.rbxcdn.com/180DAY-aaaa/512/512/Image/Png/x"
    thumbs2 = copy.deepcopy(thumbs)
    thumbs2["data"][0]["thumbnails"].reverse()
    p1 = copy.deepcopy(pages()[0])
    p1["nextPageToken"] = None
    p1["gamePasses"][1]["price"] = 999
    mock_api(icons2, thumbs2, [p1, p1])
    await ra.run(tmp_path, cfg)

    ev = pl.read_parquet(tmp_path / "roblox/events/**/*.parquet")
    assert sorted(ev["event_type"].to_list()) == ["icon_change", "thumbnail_change"]
    icon_ev = ev.filter(pl.col("event_type") == "icon_change").row(0, named=True)
    assert (icon_ev["old_value"], icon_ev["new_value"]) == (
        "16972705ca6b7ee982e76269d5b887e8", "aaaa")

    log = pl.read_parquet(tmp_path / "roblox/gamepasses/**/*.parquet")
    assert log.height == 4 + 3  # día 2: 2 bajas + 1 cambio de precio
    gp = read_dimension(tmp_path, "roblox", ra.GAMEPASS_SCHEMA, ra.PASS_KEY, table="gamepasses")
    assert gp.filter(pl.col("is_listed")).height == 2
    assert gp.filter(pl.col("price") == 999).height == 1

    con = connect(tmp_path)  # vistas DuckDB de estado actual
    assert con.sql("select count(*) from roblox_gamepasses where is_listed").fetchall() == [(2,)]
    assert con.sql("select count(*) from roblox_gamepasses_history").fetchall() == [(7,)]
    assert con.sql("select icon_hash from roblox_media").fetchall() == [("aaaa",)]
    assert con.sql("select count(*) from roblox_asset_checks").fetchall() == [(4,)]


@respx.mock
async def test_gamepass_error_does_not_mark_passes_as_removed(tmp_path, cfg):
    write_games(tmp_path, {A: "hot"})
    mock_api(load("roblox_icons.json"), load("roblox_thumbnails.json"), pages())
    await ra.run(tmp_path, cfg)
    respx.reset()
    mock_api(load("roblox_icons.json"), load("roblox_thumbnails.json"), pages(), status_a=500)
    summary = await ra.run(tmp_path, cfg)
    assert summary.ops_failed == 1
    gp = read_dimension(tmp_path, "roblox", ra.GAMEPASS_SCHEMA, ra.PASS_KEY, table="gamepasses")
    assert gp["is_listed"].all() and gp.height == 4
    assert pl.read_parquet(tmp_path / "roblox/gamepasses/**/*.parquet").height == 4
