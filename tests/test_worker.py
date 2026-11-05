import threading
import time

import httpx
import pytest

from app import store
from app.db import connect
from worker import handlers as h
from worker.handlers import PermanentError
from worker.runner import JobTimeout, process_job, run_with_timeout, run_worker


def claim_one(max_attempts=3, job_type="report_generate"):
    conn = connect()
    store.create_job(conn, job_type, {}, "k", max_attempts, now=1000.0)
    worker = store.worker_connect()
    return conn, worker, store.claim_job(worker, now=1000.0)


# --------------------------------------------------------------- process_job


def test_success_marks_job_succeeded(client):
    conn, worker, job = claim_one()
    status = process_job(worker, job, {"report_generate": lambda p: {"done": 1}})
    assert status == "succeeded"
    assert store.job_to_dict(store.get_job(conn, job["id"]))["result"] == {"done": 1}


def test_exception_triggers_retry(client):
    conn, worker, job = claim_one()

    def boom(_):
        raise RuntimeError("target exploded")

    assert process_job(worker, job, {"report_generate": boom}) == "queued"
    row = store.get_job(conn, job["id"])
    assert "RuntimeError: target exploded" in row["last_error"]


def test_permanent_error_skips_retries(client):
    conn, worker, job = claim_one(max_attempts=5)

    def reject(_):
        raise PermanentError("bad payload")

    assert process_job(worker, job, {"report_generate": reject}) == "dead"
    assert store.get_job(conn, job["id"])["attempts"] == 1


def test_unknown_job_type_goes_dead(client):
    conn, worker, job = claim_one()
    assert process_job(worker, job, {}) == "dead"
    assert "no handler" in store.get_job(conn, job["id"])["last_error"]


def test_timeout_counts_as_a_failed_attempt(client):
    conn, worker, job = claim_one()
    status = process_job(
        worker, job, {"report_generate": lambda p: time.sleep(1)}, job_timeout=0.1
    )
    assert status == "queued"
    assert "timed out" in store.get_job(conn, job["id"])["last_error"]


def test_run_with_timeout_returns_result_and_reraises():
    assert run_with_timeout(lambda p: p["x"] * 2, {"x": 21}, timeout=1) == 42
    with pytest.raises(ValueError):
        run_with_timeout(lambda p: int("nope"), {}, timeout=1)
    with pytest.raises(JobTimeout):
        run_with_timeout(lambda p: time.sleep(1), {}, timeout=0.05)


# ---------------------------------------------------------------- handlers


def fake_post(status):
    def post(url, **kwargs):
        return httpx.Response(status, request=httpx.Request("POST", url))
    return post


def test_webhook_success(monkeypatch):
    monkeypatch.setattr(h.httpx, "post", fake_post(200))
    assert h.webhook_delivery({"url": "http://x"}) == {"status_code": 200}


def test_webhook_5xx_is_retryable(monkeypatch):
    monkeypatch.setattr(h.httpx, "post", fake_post(503))
    with pytest.raises(httpx.HTTPStatusError):
        h.webhook_delivery({"url": "http://x"})


@pytest.mark.parametrize("status", [400, 404])
def test_webhook_4xx_is_permanent(monkeypatch, status):
    monkeypatch.setattr(h.httpx, "post", fake_post(status))
    with pytest.raises(PermanentError):
        h.webhook_delivery({"url": "http://x"})


def test_webhook_429_is_retryable_not_permanent(monkeypatch):
    monkeypatch.setattr(h.httpx, "post", fake_post(429))
    with pytest.raises(httpx.HTTPStatusError):
        h.webhook_delivery({"url": "http://x"})


def test_report_generate():
    result = h.report_generate({"rows": 50, "seconds": 0})
    assert result["rows"] == 50 and result["bytes"] > 0


# ---------------------------------------------------- multi-worker integration


def test_three_workers_process_twenty_jobs_exactly_once(client):
    conn = connect()
    for i in range(20):
        store.create_job(conn, "report_generate", {}, f"job-{i}", max_attempts=3)

    calls, lock = [], threading.Lock()

    def handler(payload):
        with lock:
            calls.append(1)
        time.sleep(0.02)
        return {"ok": True}

    stop = threading.Event()
    threads = [
        threading.Thread(
            target=run_worker,
            kwargs=dict(
                poll_interval=0.02,
                handlers={"report_generate": handler},
                stop_event=stop,
                install_signals=False,
            ),
        )
        for _ in range(3)
    ]
    for t in threads:
        t.start()

    deadline = time.time() + 15
    while time.time() < deadline:
        if store.metrics(conn)["by_status"]["succeeded"] == 20:
            break
        time.sleep(0.05)
    stop.set()
    for t in threads:
        t.join(timeout=5)

    assert store.metrics(conn)["by_status"]["succeeded"] == 20
    assert len(calls) == 20  # 20 jobs, 20 executions: nothing ran twice
    rows = conn.execute("SELECT attempts FROM jobs").fetchall()
    assert all(r["attempts"] == 1 for r in rows)
