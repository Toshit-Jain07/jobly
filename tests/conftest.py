import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    """A client backed by a fresh temp database for every test."""
    monkeypatch.setenv("JOBLY_DB", str(tmp_path / "test.db"))
    with TestClient(app) as c:  # `with` runs startup, which creates the table
        yield c
