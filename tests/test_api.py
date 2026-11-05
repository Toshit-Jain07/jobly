from app import store
from app.db import connect

URL_JOB = {"type": "url_check", "payload": {"url": "http://example.com"}}


def test_webhook_without_url_is_rejected(client):
    r = client.post("/jobs", json={"type": "webhook_delivery", "payload": {"body": {}}})
    assert r.status_code == 422


def test_report_generate_payload_is_validated(client):
    bad = {"type": "report_generate", "payload": {"rows": 10**9}}
    assert client.post("/jobs", json=bad).status_code == 422
    ok = {"type": "report_generate", "payload": {"rows": 10, "seconds": 0}}
    assert client.post("/jobs", json=ok).status_code == 201


def test_list_jobs_filters_by_status(client):
    client.post("/jobs", json=URL_JOB, headers={"Idempotency-Key": "a"})
    client.post("/jobs", json=URL_JOB, headers={"Idempotency-Key": "b"})
    assert len(client.get("/jobs").json()) == 2
    assert len(client.get("/jobs?status=queued").json()) == 2
    assert client.get("/jobs?status=dead").json() == []
    assert client.get("/jobs?status=bogus").status_code == 422


def test_dead_letter_inspection_and_manual_retry(client):
    job = client.post("/jobs", json={**URL_JOB, "max_attempts": 1}).json()
    worker = store.worker_connect()
    claimed = store.claim_job(worker)
    store.fail_job(worker, claimed, "target is down")

    dead = client.get("/jobs?status=dead").json()
    assert [j["id"] for j in dead] == [job["id"]]
    assert dead[0]["last_error"] == "target is down"

    retried = client.post(f"/jobs/{job['id']}/retry")
    assert retried.status_code == 200
    assert retried.json()["status"] == "queued"
    assert retried.json()["attempts"] == 0


def test_retry_rejects_non_dead_and_missing_jobs(client):
    job = client.post("/jobs", json=URL_JOB).json()
    assert client.post(f"/jobs/{job['id']}/retry").status_code == 409
    assert client.post("/jobs/9999/retry").status_code == 404


def test_stats_endpoint(client):
    client.post("/jobs", json=URL_JOB)
    stats = client.get("/stats").json()
    assert stats["by_status"]["queued"] == 1
    assert stats["queue_depth"] == 1
    assert stats["success_rate"] is None


def test_prometheus_metrics_endpoint(client):
    client.post("/jobs", json=URL_JOB)
    text = client.get("/metrics").text
    assert 'jobly_jobs{status="queued"} 1' in text
    assert "jobly_queue_depth 1" in text
    assert "# TYPE jobly_jobs gauge" in text


def test_flaky_endpoint_can_succeed_and_fail(client):
    assert client.get("/_demo/flaky?fail=0&hang=0").status_code == 200
    assert client.get("/_demo/flaky?fail=1&hang=0").status_code == 500
