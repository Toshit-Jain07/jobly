from concurrent.futures import ThreadPoolExecutor

from app.db import connect

JOB = {"type": "webhook_delivery", "payload": {"url": "http://example.com/hook", "body": {"a": 1}}}


def count_jobs() -> int:
    conn = connect()
    try:
        return conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    finally:
        conn.close()


def test_create_job_is_queued(client):
    r = client.post("/jobs", json=JOB, headers={"Idempotency-Key": "k1"})
    assert r.status_code == 201
    data = r.json()
    assert data["status"] == "queued"
    assert data["attempts"] == 0
    assert data["max_attempts"] == 5
    assert data["payload"] == JOB["payload"]
    assert data["idempotency_key"] == "k1"


def test_same_key_twice_creates_one_row(client):
    first = client.post("/jobs", json=JOB, headers={"Idempotency-Key": "dup"})
    second = client.post("/jobs", json=JOB, headers={"Idempotency-Key": "dup"})
    assert first.status_code == 201
    assert second.status_code == 200  # existing job returned, not a new one
    assert first.json()["id"] == second.json()["id"]
    assert count_jobs() == 1


def test_same_key_different_request_is_conflict(client):
    client.post("/jobs", json=JOB, headers={"Idempotency-Key": "k"})
    other = {"type": "url_check", "payload": {"url": "http://example.com"}}
    r = client.post("/jobs", json=other, headers={"Idempotency-Key": "k"})
    assert r.status_code == 409
    assert count_jobs() == 1


def test_no_key_creates_separate_jobs(client):
    a = client.post("/jobs", json=JOB)
    b = client.post("/jobs", json=JOB)
    assert a.json()["id"] != b.json()["id"]
    assert count_jobs() == 2


def test_get_job(client):
    created = client.post("/jobs", json=JOB, headers={"Idempotency-Key": "g"}).json()
    r = client.get(f"/jobs/{created['id']}")
    assert r.status_code == 200
    assert r.json() == created


def test_get_missing_job_404(client):
    assert client.get("/jobs/9999").status_code == 404


def test_unknown_job_type_rejected(client):
    r = client.post("/jobs", json={"type": "make_coffee", "payload": {}})
    assert r.status_code == 422


def test_concurrent_same_key_creates_one_row(client):
    """20 simultaneous requests with one key must still yield exactly one job."""

    def post(_):
        return client.post("/jobs", json=JOB, headers={"Idempotency-Key": "race"})

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(post, range(20)))

    assert all(r.status_code in (200, 201) for r in results)
    assert sum(r.status_code == 201 for r in results) == 1
    assert len({r.json()["id"] for r in results}) == 1
    assert count_jobs() == 1
