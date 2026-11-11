import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from collectors import roblox_creators as rc
from collectors.common import load_config, write_dimension_changes, write_partition
from pipeline.db import connect

FIX = Path(__file__).parent / "fixtures"
GROUP = "https://games.roblox.com/v2/groups/{}/games"
USER = "https://games.roblox.com/v2/users/{}/games"
NOW = datetime(2026, 11, 11, 9, tzinfo=UTC)


def load(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


@pytest.fixture
def cfg():
    c = load_config()
    c["http"].update(backoff_base_s=0.0, backoff_cap_s=0.0, throttle_min_pause_s=0.0,
                     max_retries=0)
    c["roblox"].update(rate_creator_groups_per_s=1e6, rate_creator_users_per_s=1e6)
    return c


def write_games(tmp_path, rows: list[tuple[int, str, int, str]], run_id: str = "g") -> None:
    """rows: (universe_id, creator_type, creator_id, tier)"""
    dim = pl.DataFrame(rows, schema={"universe_id": pl.Int64, "creator_type": pl.Utf8,
                                     "creator_id": pl.Int64, "tier": pl.Utf8}, orient="row")
    write_dimension_changes(dim.clear(), dim, "universe_id", tmp_path, "roblox",
                            NOW - timedelta(days=20), run_id)


def write_checks(tmp_path, rows: list[tuple[str, int, str, datetime]]) -> None:
    df = pl.DataFrame(
        [{"snapshot_at": at, "creator_type": t, "creator_id": i, "status": s, "n_games": 1,
          "truncated": False} for t, i, s, at in rows], schema=rc.CREATOR_CHECKS_SCHEMA)
    write_partition(df, tmp_path, "roblox", "creator_checks", NOW - timedelta(days=1), "c")


def test_select_creators_new_by_tier_then_due(tmp_path):
    write_games(tmp_path, [(10, "Group", 1, "hot"), (11, "Group", 1, "cold"),
                           (20, "Group", 2, "cold"), (30, "User", 3, "warm"),
                           (40, "Group", 4, "cold"), (50, "Group", 5, "cold"),
                           (60, "User", 6, "cold")])
    write_checks(tmp_path, [("Group", 4, "ok", NOW - timedelta(days=2)),
                            ("Group", 5, "not_found", NOW - timedelta(days=10)),
                            ("User", 6, "error", NOW - timedelta(days=2))])
    dim = rc.read_dimension(tmp_path, "roblox", rc.GAMES_SCHEMA, "universe_id")
    order, counts = rc.select_creators(dim, rc.last_checks(tmp_path), NOW, 7)
    assert order == [("Group", 1), ("User", 3), ("Group", 2), ("User", 6), ("Group", 5)]
    assert counts == {"nuevos": 4, "vencidos": 1, "al día": 1}


def test_parse_games():
    rows = rc.parse_games(("User", 68816760), load("roblox_user_games.json")["data"], NOW)
    assert len(rows) == 3
    r = rows[0]
    assert r["universe_id"] == 6347378905 and r["root_place_id"] == 18734197659
    assert r["created_at"] == datetime(2024, 7, 31, 8, 15, 26, 720000, tzinfo=UTC)
    assert r["place_visits"] == 73 and r["creator_type"] == "User"


@respx.mock
def test_run_end_to_end(tmp_path, cfg):
    write_games(tmp_path, [(10, "Group", 12208023, "hot"), (30, "User", 68816760, "warm"),
                           (20, "Group", 2, "cold"), (40, "Group", 5, "cold")])

    def group_pages(request: httpx.Request) -> httpx.Response:
        assert request.url.params["limit"] == "100"
        assert request.url.params["accessFilter"] == "Public"
        page = "p2" if request.url.params.get("cursor") == "CURSOR2" else "p1"
        return httpx.Response(200, json=load(f"roblox_group_games_{page}.json"))

    respx.get(GROUP.format(12208023)).mock(side_effect=group_pages)
    respx.get(USER.format(68816760)).mock(
        return_value=httpx.Response(200, json=load("roblox_user_games.json")))
    respx.get(GROUP.format(2)).mock(return_value=httpx.Response(
        400, json={"errors": [{"code": 1, "message": "Group is invalid or does not exist."}]}))
    respx.get(GROUP.format(5)).mock(return_value=httpx.Response(503))

    summary = rc.asyncio.run(rc.run(tmp_path, cfg))
    assert summary.rows["creator_games"] == 5 + 3
    assert summary.ops_failed == 1

    con = connect(tmp_path)
    checks = {(t, i): (s, n) for t, i, s, n in con.sql(
        "select creator_type, creator_id, status, n_games from roblox_creator_checks"
    ).fetchall()}
    assert checks == {("Group", 12208023): ("ok", 5), ("User", 68816760): ("ok", 3),
                      ("Group", 2): ("not_found", 0), ("Group", 5): ("error", 0)}
    assert con.sql("select count(distinct universe_id) from roblox_creator_games"
                   ).fetchall()[0][0] == 8

    # segunda corrida: solo el que dio error queda pendiente
    order, counts = rc.select_creators(
        rc.read_dimension(tmp_path, "roblox", rc.GAMES_SCHEMA, "universe_id"),
        rc.last_checks(tmp_path), rc.utcnow(), 7)
    assert order == [("Group", 5)] and counts["al día"] == 3


@respx.mock
def test_truncated_and_budget(tmp_path, cfg):
    write_games(tmp_path, [(10, "Group", 12208023, "hot")])
    respx.get(GROUP.format(12208023)).mock(
        return_value=httpx.Response(200, json=load("roblox_group_games_p1.json")))
    cfg["roblox"]["creators_max_pages"] = 1
    rc.asyncio.run(rc.run(tmp_path, cfg))
    con = connect(tmp_path)
    assert con.sql("select truncated, n_games from roblox_creator_checks").fetchall() == [
        (True, 3)]

    write_games(tmp_path, [(11, "Group", 7, "hot")], run_id="g2")
    summary = rc.asyncio.run(rc.run(tmp_path, cfg, budget_minutes=0, dry_run=True))
    assert summary.ops_total == 0
    assert any("quedan 1 grupos" in n for n in summary.notes)
