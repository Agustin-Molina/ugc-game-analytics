import json
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from collectors.common import TS, write_dimension_changes, write_partition
from pipeline import export
from pipeline.db import connect

T0 = datetime(2026, 10, 30, 12, tzinfo=UTC)


@pytest.fixture
def con(tmp_path):
    games = pl.DataFrame({"universe_id": [1, 2], "name": ["Grow a Garden", "Obby"]})
    write_dimension_changes(games.clear(), games, "universe_id", tmp_path, "roblox", T0, "g")
    for i, t in enumerate([T0, T0 + timedelta(days=3)]):  # octubre y noviembre
        snaps = pl.DataFrame({"snapshot_at": [t, t], "universe_id": [1, 2],
                              "playing": [10, 20]}, schema_overrides={"snapshot_at": TS})
        write_partition(snaps, tmp_path, "roblox", "snapshots", t, f"s{i}")
    return connect(tmp_path)


def test_export_files_and_metadata(con, tmp_path):
    out = tmp_path / "export"
    out.mkdir()
    (out / "stale.parquet").write_text("x")  # restos de otra corrida: se borran
    res = export.export(con, out, "someone")

    names = dict(res.files)
    assert names["roblox_snapshots_2026-10.parquet"] == 2
    assert names["roblox_snapshots_2026-11.parquet"] == 2
    assert names["roblox_games_history.parquet"] == 2
    assert not (out / "stale.parquet").exists()
    assert "fortnite_islands" in res.skipped  # sin datos de Fortnite
    assert not res.undocumented

    snap = pl.read_parquet(out / "roblox_snapshots_2026-11.parquet")
    assert "date" not in snap.columns  # columna interna de partición

    meta = json.loads((out / "dataset-metadata.json").read_text(encoding="utf-8"))
    assert meta["id"] == "someone/" + export.load_spec()["dataset"]["slug"]
    assert 6 <= len(meta["title"]) <= 50 and 20 <= len(meta["subtitle"]) <= 80
    assert meta["licenses"] == [{"name": "CC-BY-NC-4.0"}]
    assert meta["description"].startswith("Live tracker")
    res_by_path = {r["path"]: r for r in meta["resources"]}
    assert set(res_by_path) == {f for f, _ in res.files}
    fields = res_by_path["roblox_snapshots_2026-11.parquet"]["schema"]["fields"]
    assert [f["name"] for f in fields] == snap.columns
    types = {f["name"]: f["type"] for f in fields}
    assert types == {"snapshot_at": "datetime", "universe_id": "integer",
                     "playing": "integer"}
    assert all(f["description"] for f in fields)
    assert res_by_path["roblox_snapshots_2026-11.parquet"]["description"].endswith(
        "Month 2026-11.")


def test_spec_is_consistent():
    spec = export.load_spec()
    ds = spec["dataset"]
    assert 3 <= len(ds["slug"]) <= 50
    assert 6 <= len(ds["title"]) <= 50 and 20 <= len(ds["subtitle"]) <= 80
    for name, t in spec["tables"].items():
        assert t["view"] and t["description"], name


def test_kaggle_types():
    assert export.kaggle_type("DECIMAL(18,3)") == "number"
    assert export.kaggle_type("VARCHAR[]") == "string"
    assert export.kaggle_type("TIMESTAMP WITH TIME ZONE") == "datetime"
