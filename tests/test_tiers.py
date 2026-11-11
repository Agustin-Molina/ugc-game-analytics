from datetime import UTC, datetime, timedelta

import polars as pl

from collectors.common import TS, group_of, write_partition
from pipeline import tiers

NOW = datetime(2026, 11, 10, 12, 17, tzinfo=UTC)
H = timedelta(hours=1)
RULES = tiers.load_rules("roblox")


def snaps(rows: list[tuple[int, datetime, int]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={"universe_id": pl.Int64, "snapshot_at": TS,
                                      "playing": pl.Int64}, orient="row")


def charts(rows: list[tuple[int, datetime]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={"universe_id": pl.Int64, "snapshot_at": TS},
                        orient="row")


def test_assign_roblox_tiers_rules():
    dim = pl.DataFrame(
        {"universe_id": list(range(1, 10)),
         "created_at": [NOW - timedelta(days=400)] * 4 + [NOW - timedelta(days=3)]
         + [NOW - timedelta(days=400)] * 4},
        schema={"universe_id": pl.Int64, "created_at": TS},
    )
    s = snaps([
        (1, NOW, 5),                                   # en chart hace 10 h → hot
        (2, NOW - 30 * H, 1500), (2, NOW, 50),         # 1500 dentro de 48 h → hot
        (3, NOW - 60 * H, 1500), (3, NOW, 50),         # 1500 fuera de 48 h → warm
        (4, NOW - 24 * H, 60), (4, NOW, 200),          # ×3,3 en 24 h → hot growth
        (5, NOW, 25),                                  # creado hace 3 días → hot new
        (6, NOW, 5),                                   # chart hace 5 días → warm
        (7, NOW - 24 * H, 3), (7, NOW, 3),             # nada → cold
        (8, NOW - 24 * H, 900), (8, NOW, 1000),        # crecimiento chico pero >= 1000 → hot
        # 9: sin observaciones → cold no_data
    ])
    c = charts([(1, NOW - 10 * H), (6, NOW - timedelta(days=5))])
    got = tiers.assign_roblox_tiers(dim, s, c, NOW, RULES).sort("universe_id")
    assert got.rows() == [
        (1, "hot", "chart"),
        (2, "hot", "ccu>=1000"),
        (3, "warm", "ccu>=20"),
        (4, "hot", "growth"),
        (5, "hot", "new"),
        (6, "warm", "chart_7d"),
        (7, "cold", "inactive"),
        (8, "hot", "ccu>=1000"),
        (9, "cold", "no_data"),
    ]


def test_growth_needs_reference_about_a_day_before():
    dim = pl.DataFrame({"universe_id": [1, 2], "created_at": [None, None]},
                       schema={"universe_id": pl.Int64, "created_at": TS})
    s = snaps([
        (1, NOW - 2 * H, 60), (1, NOW, 200),           # solo 2 h antes → no es growth
        (2, NOW - 50 * H, 60), (2, NOW, 200),          # 50 h antes (> 36) → no es growth
    ])
    got = tiers.assign_roblox_tiers(dim, s, charts([]), NOW, RULES)
    assert set(got["tier"]) == {"warm"}


def test_tier_changes_only_new_or_changed():
    old = pl.DataFrame({"universe_id": [1, 2, 3], "tier": ["warm", "hot", None]})
    assigned = pl.DataFrame({"universe_id": [1, 2, 3, 4], "tier": ["warm", "warm", "hot", "hot"],
                             "reason": ["x", "ccu>=20", "chart", "new"]})
    got = tiers.tier_changes(old, assigned, NOW)
    assert got.select("universe_id", "from_tier", "to_tier").rows() == [
        (2, "hot", "warm"), (3, None, "hot"), (4, None, "hot")]
    assert got.schema == tiers.TIER_HISTORY_SCHEMA


def test_select_due_cadence_and_overdue():
    hour = int(NOW.timestamp() // 3600)
    used: set[int] = set()

    def pick(n: int, in_slot: bool) -> int:
        i = next(i for i in range(1, 5000)
                 if i not in used and (group_of(str(i), n) == hour % n) == in_slot)
        used.add(i)
        return i

    warm_in, warm_out, warm_late = pick(24, True), pick(24, False), pick(24, False)
    cold_in, cold_out = pick(168, True), pick(168, False)
    dim = pl.DataFrame({
        "universe_id": [100_000, warm_in, warm_out, warm_late, cold_in, cold_out],
        "tier": ["hot", "warm", "warm", "warm", "cold", "cold"],
    })
    last = pl.DataFrame(
        {"universe_id": [warm_out, warm_late, cold_out],
         "last_at": [NOW - 20 * H, NOW - 40 * H, NOW - 100 * H]},  # warm_late: > 36 h
        schema={"universe_id": pl.Int64, "last_at": TS},
    )
    due, counts = tiers.select_due(dim, last, NOW, RULES["cadence"])
    assert set(due) == {100_000, warm_in, warm_late, cold_in}
    assert counts == {"hot": 1, "warm": 1, "cold": 1, "overdue": 1}


def test_select_due_spreads_warm_over_the_day():
    dim = pl.DataFrame({"universe_id": list(range(24_000)), "tier": ["warm"] * 24_000})
    last = pl.DataFrame(schema={"universe_id": pl.Int64, "last_at": TS})
    sizes = [len(tiers.select_due(dim, last, NOW + h * H, RULES["cadence"])[0])
             for h in range(24)]
    assert sum(sizes) == 24_000  # cada juego warm exactamente una vez por día
    assert max(sizes) < 1_300


def test_read_recent_prunes_by_partition(tmp_path):
    old = snaps([(1, NOW - timedelta(days=20), 5)])
    new = snaps([(1, NOW - 2 * H, 7)])
    write_partition(old, tmp_path, "roblox", "snapshots", NOW - timedelta(days=20), "a")
    write_partition(new, tmp_path, "roblox", "snapshots", NOW - 2 * H, "b")
    got = tiers.read_recent(tmp_path, "roblox", "snapshots",
                            ["universe_id", "playing", "snapshot_at"], NOW - timedelta(days=11))
    assert got["playing"].to_list() == [7]
    assert tiers.read_recent(tmp_path, "roblox", "events", ["snapshot_at"], NOW) is None


# --------------------------------------------------------------------------- Fortnite

F_RULES = tiers.load_rules("fortnite")
DAY = timedelta(days=1)
TODAY = datetime(2026, 11, 10, tzinfo=UTC)


def day_snaps(rows: list[tuple[str, datetime, int | None]]) -> pl.DataFrame:
    """(isla, bucket, peak); snapshot_at = NOW."""
    return pl.DataFrame([(c, b, NOW, p) for c, b, p in rows],
                        schema={"island_code": pl.Utf8, "bucket_start": TS, "snapshot_at": TS,
                                "peak_ccu": pl.Int64}, orient="row")


def test_assign_fortnite_tiers_rules():
    load = NOW - 5 * DAY
    codes = ["PEAK", "GROW", "FLAT", "NEW", "OLDNEW", "DATA", "NOFETCH", "EMPTY"]
    dim = pl.DataFrame(
        {"island_code": codes,
         "first_seen_at": [load] * 3 + [NOW - DAY, NOW - 4 * DAY] + [load] * 3},
        schema={"island_code": pl.Utf8, "first_seen_at": TS},
    )
    s = day_snaps([
        ("PEAK", TODAY - 6 * DAY, 80), ("PEAK", TODAY - DAY, 10),
        ("GROW", TODAY - 5 * DAY, 10), ("GROW", TODAY - DAY, 25),   # 25 >= 2 × 10
        ("FLAT", TODAY - 5 * DAY, 15), ("FLAT", TODAY - DAY, 25),   # 25 < 2 × 15
        ("DATA", TODAY - 9 * DAY, 300),                             # fuera de los 7 días
        ("EMPTY", TODAY - DAY, None),
    ])
    fetched = pl.DataFrame({"island_code": [c for c in codes if c != "NOFETCH"]})
    got = tiers.assign_fortnite_tiers(dim, s, fetched, NOW, F_RULES)
    out = {r["island_code"]: (r["tier"], r["reason"]) for r in got.iter_rows(named=True)}
    assert out == {
        "PEAK": ("hot", "peak>=50"),
        "GROW": ("hot", "growth"),
        "FLAT": ("warm", "has_data"),
        "NEW": ("hot", "new"),          # llegó al catálogo después de la carga inicial
        "OLDNEW": ("cold", "no_data"),  # nueva hace 4 días (> 3) y sin datos
        "DATA": ("warm", "has_data"),
        "NOFETCH": ("warm", "no_fetch"),
        "EMPTY": ("cold", "no_data"),
    }


def test_assign_fortnite_tiers_uses_latest_fetch_of_each_bucket():
    dim = pl.DataFrame({"island_code": ["A"], "first_seen_at": [NOW - 9 * DAY]},
                       schema={"island_code": pl.Utf8, "first_seen_at": TS})
    s = pl.DataFrame(
        [("A", TODAY - DAY, NOW - 2 * DAY, 90), ("A", TODAY - DAY, NOW, 30)],
        schema={"island_code": pl.Utf8, "bucket_start": TS, "snapshot_at": TS,
                "peak_ccu": pl.Int64}, orient="row")
    got = tiers.assign_fortnite_tiers(dim, s, pl.DataFrame({"island_code": ["A"]}), NOW,
                                      F_RULES)
    assert got["tier"].to_list() == ["hot"]  # 30 con growth (sin datos antes), no por 90
    assert got["reason"].to_list() == ["growth"]


def test_select_fortnite_due():
    cad = F_RULES["cadence"]
    day = NOW.date().toordinal()
    codes = [f"C{i}" for i in range(400)]

    def pick(n: int, k: int, hit: bool, used: set) -> str:
        for c in codes:
            if c not in used and (group_of(c, n) == day % n) == hit:
                used.add(c)
                return c
        raise AssertionError

    used: set[str] = set()
    warm_due = pick(cad["warm_days"], 0, True, used)
    warm_not = pick(cad["warm_days"], 0, False, used)
    warm_late = pick(cad["warm_days"], 0, False, used)
    cold_due = pick(cad["cold_days"], 0, True, used)
    cold_not = pick(cad["cold_days"], 0, False, used)
    dim = pl.DataFrame({
        "island_code": ["HOT", warm_due, warm_not, warm_late, cold_due, cold_not],
        "tier": ["hot", "warm", "warm", "warm", "cold", "cold"]})
    last = pl.DataFrame(
        {"island_code": [warm_not, warm_late, cold_not],
         "last_at": [NOW - 2 * DAY, NOW - 6.5 * DAY, NOW - 7.5 * DAY]},  # cold: 7 + 1 = 8
        schema={"island_code": pl.Utf8, "last_at": TS})
    got, counts = tiers.select_fortnite_due(dim, last, NOW, cad)
    assert got == ["HOT", warm_late, warm_due, cold_due]  # hot, atrasadas, warm, cold
    assert counts == {"hot": 1, "overdue": 1, "warm": 1, "cold": 1}
