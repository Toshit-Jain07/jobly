import random
from concurrent.futures import ThreadPoolExecutor

from app import store
from app.db import connect


def new_conn():
    return connect()


def make_job(conn, key="k", job_type="webhook_delivery", max_attempts=5, now=1000.0, run_at=None):
    row, _ = store.create_job(
        conn, job_type, {"url": "http://example.com"}, key, max_attempts, now=now, run_at=run_at
    )
    return row


# ---------------------------------------------------------------------- claim


def test_claim_returns_oldest_due_job_and_marks_it_running(client):
    conn = new_conn()
    make_job(conn, "a", run_at=1000.0)
    make_job(conn, "b", run_at=900.0)  # older -> goes first
    worker = store.worker_connect()

    job = store.claim_job(worker, now=1000.0)
    assert job["idempotency_key"] == "b"
    assert job["status"] == "running"
    assert job["attempts"] == 1
    assert job["locked_at"] == 1000.0


def test_claim_skips_jobs_scheduled_in_the_future(client):
    conn = new_conn()
    make_job(conn, "later", run_at=5000.0)
    worker = store.worker_connect()
    assert store.claim_job(worker, now=1000.0) is None
    assert store.claim_job(worker, now=5000.0) is not None


def test_claim_on_empty_queue_returns_none(client):
    assert store.claim_job(store.worker_connect(), now=1000.0) is None


def test_concurrent_workers_never_claim_the_same_job(client):
    conn = new_conn()
    total = 60
    for i in range(total):
        make_job(conn, f"job-{i}")

    def drain(_):
        worker = store.worker_connect()
        claimed = []
        while True:
            job = store.claim_job(worker, now=2000.0)
            if job is None:
                return claimed
            claimed.append(job["id"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = [ids for batch in pool.map(drain, range(8)) for ids in batch]

    assert len(results) == total              # every job was claimed...
    assert len(set(results)) == total         # ...exactly once


# -------------------------------------------------------------------- backoff


def test_backoff_grows_exponentially_with_jitter():
    rng = random.Random(1)
    for attempts, ceiling in [(1, 2), (2, 4), (3, 8), (4, 16), (5, 32)]:
        for _ in range(50):
            delay = store.backoff_delay(attempts, base=2.0, rng=rng)
            assert ceiling / 2 <= delay <= ceiling


def test_backoff_is_capped():
    assert store.backoff_delay(30, base=2.0, cap=300.0) <= 300.0


# ------------------------------------------------------------- fail / complete


def test_failed_job_is_requeued_in_the_future(client):
    conn = new_conn()
    make_job(conn, "x", max_attempts=3)
    worker = store.worker_connect()
    job = store.claim_job(worker, now=1000.0)

    status, run_at = store.fail_job(worker, job, "boom", now=1000.0)
    assert status == "queued"
    assert 1001.0 <= run_at <= 1002.0  # attempt 1 -> 2s ceiling, half is fixed

    row = store.get_job(conn, job["id"])
    assert row["status"] == "queued"
    assert row["last_error"] == "boom"
    assert row["locked_at"] is None
    assert store.claim_job(worker, now=1000.5) is None  # still backing off


def test_job_goes_dead_after_max_attempts(client):
    conn = new_conn()
    make_job(conn, "x", max_attempts=3)
    worker = store.worker_connect()

    statuses = []
    clock = 1000.0
    for _ in range(3):
        job = store.claim_job(worker, now=clock)
        statuses.append(store.fail_job(worker, job, "still broken", now=clock)[0])
        clock += 1000  # jump past any backoff

    assert statuses == ["queued", "queued", "dead"]
    row = store.get_job(conn, job["id"])
    assert row["status"] == "dead"
    assert row["attempts"] == 3
    assert store.claim_job(worker, now=clock + 10**6) is None  # dead jobs never run again


def test_non_retryable_failure_goes_straight_to_dead(client):
    conn = new_conn()
    make_job(conn, "x", max_attempts=5)
    worker = store.worker_connect()
    job = store.claim_job(worker, now=1000.0)
    status, _ = store.fail_job(worker, job, "bad payload", retryable=False, now=1000.0)
    assert status == "dead"
    assert store.get_job(conn, job["id"])["attempts"] == 1


def test_complete_job_records_result_and_duration(client):
    conn = new_conn()
    make_job(conn, "x")
    worker = store.worker_connect()
    job = store.claim_job(worker, now=1000.0)

    assert store.complete_job(worker, job, {"ok": True}, 12.5, now=1001.0)
    out = store.job_to_dict(store.get_job(conn, job["id"]))
    assert out["status"] == "succeeded"
    assert out["result"] == {"ok": True}
    assert out["duration_ms"] == 12.5


# ------------------------------------------------------------------- recovery


def test_stuck_job_is_requeued_and_stale_worker_cannot_overwrite(client):
    conn = new_conn()
    make_job(conn, "x")
    worker = store.worker_connect()
    job = store.claim_job(worker, now=1000.0)  # worker "crashes" here

    assert store.recover_stuck(worker, lock_timeout=60, now=1030.0) == 0  # lock still fresh
    assert store.recover_stuck(worker, lock_timeout=60, now=1100.0) == 1
    assert store.get_job(conn, job["id"])["status"] == "queued"

    # The original worker wakes up late. Its result must be ignored.
    assert store.complete_job(worker, job, {"late": True}, 1.0, now=1101.0) is False
    assert store.fail_job(worker, job, "late failure", now=1101.0) is None
    assert store.get_job(conn, job["id"])["status"] == "queued"


def test_stuck_job_with_no_attempts_left_goes_dead(client):
    conn = new_conn()
    make_job(conn, "x", max_attempts=1)
    worker = store.worker_connect()
    job = store.claim_job(worker, now=1000.0)
    store.recover_stuck(worker, lock_timeout=60, now=2000.0)
    assert store.get_job(conn, job["id"])["status"] == "dead"


def test_retry_dead_requeues_with_fresh_attempts(client):
    conn = new_conn()
    make_job(conn, "x", max_attempts=1)
    worker = store.worker_connect()
    job = store.claim_job(worker, now=1000.0)
    store.fail_job(worker, job, "nope", now=1000.0)
    assert store.get_job(conn, job["id"])["status"] == "dead"

    assert store.retry_dead(conn, job["id"], now=2000.0) is True
    row = store.get_job(conn, job["id"])
    assert (row["status"], row["attempts"]) == ("queued", 0)
    assert store.retry_dead(conn, job["id"]) is False  # only dead jobs qualify


# -------------------------------------------------------------------- metrics


def test_metrics_counts_and_rates(client):
    conn = new_conn()
    worker = store.worker_connect()
    make_job(conn, "ok", now=1000.0)
    make_job(conn, "bad", max_attempts=1, now=1000.0)
    make_job(conn, "waiting", now=1000.0)
    make_job(conn, "later", now=1000.0, run_at=9000.0)

    first = store.claim_job(worker, now=1000.0)
    store.complete_job(worker, first, {}, 100.0, now=1000.0)
    second = store.claim_job(worker, now=1000.0)
    store.fail_job(worker, second, "x", now=1000.0)

    m = store.metrics(conn, now=1010.0)
    assert m["by_status"] == {"queued": 2, "running": 0, "succeeded": 1, "dead": 1}
    assert m["queue_depth"] == 1          # "waiting"
    assert m["scheduled_retries"] == 1    # "later"
    assert m["avg_duration_ms"] == 100.0
    assert m["success_rate"] == 0.5
    assert m["oldest_ready_age_seconds"] == 10.0
