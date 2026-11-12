import pytest


@pytest.fixture(autouse=True)
def _random_run_ids(monkeypatch):
    """En Actions, new_run_id() usa GITHUB_RUN_ID, igual para todo el job: dos corridas
    simuladas en el mismo segundo escribirían el mismo archivo. Los tests usan ids al azar."""
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
