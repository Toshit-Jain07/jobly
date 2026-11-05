"""All queue logic lives here: create, claim, complete, fail, recover, metrics.

Both the API and the workers use these functions, so the rules for a job's life
(queued -> running -> succeeded / retry / dead) are in exactly one place.

Time is passed in as `now` so tests can control it instead of sleeping.
"""
import json
import random
import sqlite3
import time

from app.db import STATUSES, connect

BACKOFF_BASE = 2.0     # seconds: ceiling of the first retry delay
BACKOFF_CAP = 300.0    # never wait longer than 5 minutes between attempts


def _now(now: float | None) -> float:
    return time.time() if now is None else now


def worker_connect() -> sqlite3.Connection:
    """Connection for workers: autocommit, we open transactions explicitly."""
    conn = connect()
    conn.isolation_level = None
    return conn


def dump_payload(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True)


def job_to_dict(row: sqlite3.Row) -> dict:
    job = dict(row)
    job["payload"] = json.loads(job["payload"])
    job["result"] = json.loads(job["result"]) if job["result"] else None
    return job


# --------------------------------------------------------------------- create


def create_job(
    conn: sqlite3.Connection,
    job_type: str,
    payload: dict,
    idempotency_key: str,
    max_attempts: int = 5,
    now: float | None = None,
    run_at: float | None = None,
) -> tuple[sqlite3.Row, bool]:
    """Insert a job, or return the existing one if the key was already used.

    Returns (row, created). One atomic statement, so two concurrent requests with
    the same key can never both insert.
    """
    now = _now(now)
    with conn:
        cur = conn.execute(
            """
            INSERT INTO jobs (type, payload, idempotency_key, max_attempts,
                              run_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO NOTHING
            """,
            (job_type, dump_payload(payload), idempotency_key, max_attempts,
             now if run_at is None else run_at, now, now),
        )
    created = cur.rowcount == 1
    row = conn.execute(
        "SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,)
    ).fetchone()
    return row, created


def get_job(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


# ---------------------------------------------------------------------- claim


def claim_job(conn: sqlite3.Connection, now: float | None = None) -> sqlite3.Row | None:
    """Atomically take the oldest due job. Needs a connection from worker_connect().

    BEGIN IMMEDIATE grabs the database write lock *before* we look for a job, so
    two workers can never both SELECT the same row and both flip it to 'running'.
    (On Postgres you'd use `SELECT ... FOR UPDATE SKIP LOCKED` for the same job.)
    """
    now = _now(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            """
            SELECT id FROM jobs
            WHERE status = 'queued' AND run_at <= ?
            ORDER BY run_at, id
            LIMIT 1
            """,
            (now,),
        ).fetchone()
        job = None
        if row is not None:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'running', locked_at = ?, attempts = attempts + 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, row["id"]),
            )
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
        conn.execute("COMMIT")
        return job
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


# ------------------------------------------------------------ finish a claimed job
#
# Both functions only act if the job is still 'running' with the same locked_at
# we claimed it with (a "fencing token"). If a slow worker's lock expired and the
# job was handed to someone else, the slow worker's late result is ignored
# instead of overwriting the newer attempt.


def complete_job(
    conn: sqlite3.Connection,
    job: sqlite3.Row,
    result: dict | None,
    duration_ms: float,
    now: float | None = None,
) -> bool:
    now = _now(now)
    cur = conn.execute(
        """
        UPDATE jobs
        SET status = 'succeeded', result = ?, duration_ms = ?, locked_at = NULL,
            last_error = NULL, updated_at = ?
        WHERE id = ? AND status = 'running' AND locked_at = ?
        """,
        (json.dumps(result) if result is not None else None, duration_ms, now,
         job["id"], job["locked_at"]),
    )
    return cur.rowcount == 1


def backoff_delay(
    attempts: int,
    base: float = BACKOFF_BASE,
    cap: float = BACKOFF_CAP,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with jitter: 'equal jitter' keeps half, randomizes half.

    Without jitter, every job that failed together retries together and hammers the
    recovering target in waves. Jitter spreads them out.
    """
    rng = rng or random
    ceiling = min(cap, base * 2 ** (attempts - 1))
    return ceiling / 2 + rng.uniform(0, ceiling / 2)


def fail_job(
    conn: sqlite3.Connection,
    job: sqlite3.Row,
    error: str,
    retryable: bool = True,
    now: float | None = None,
    backoff_base: float = BACKOFF_BASE,
    rng: random.Random | None = None,
) -> tuple[str, float] | None:
    """Retry later with backoff, or move to dead-letter once attempts run out.

    Returns (new_status, run_at), or None if this worker no longer owns the job.
    """
    now = _now(now)
    if not retryable or job["attempts"] >= job["max_attempts"]:
        status, run_at = "dead", job["run_at"]
    else:
        status = "queued"
        run_at = now + backoff_delay(job["attempts"], base=backoff_base, rng=rng)

    cur = conn.execute(
        """
        UPDATE jobs
        SET status = ?, run_at = ?, locked_at = NULL, last_error = ?, updated_at = ?
        WHERE id = ? AND status = 'running' AND locked_at = ?
        """,
        (status, run_at, error[:2000], now, job["id"], job["locked_at"]),
    )
    return (status, run_at) if cur.rowcount == 1 else None


# ------------------------------------------------------------------- recovery


def recover_stuck(conn: sqlite3.Connection, lock_timeout: float, now: float | None = None) -> int:
    """Rescue jobs whose worker died mid-run (lock older than lock_timeout).

    Out of attempts -> dead. Otherwise back to queued, runnable immediately.
    Returns how many jobs were recovered.
    """
    now = _now(now)
    cur = conn.execute(
        """
        UPDATE jobs
        SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
            run_at = ?, locked_at = NULL,
            last_error = 'worker lost: lock expired', updated_at = ?
        WHERE status = 'running' AND locked_at < ?
        """,
        (now, now, now - lock_timeout),
    )
    return cur.rowcount


def retry_dead(conn: sqlite3.Connection, job_id: int, now: float | None = None) -> bool:
    """Manually requeue a dead-lettered job with a fresh attempt budget."""
    now = _now(now)
    with conn:
        cur = conn.execute(
            """
            UPDATE jobs
            SET status = 'queued', attempts = 0, run_at = ?, locked_at = NULL,
                updated_at = ?
            WHERE id = ? AND status = 'dead'
            """,
            (now, now, job_id),
        )
    return cur.rowcount == 1


# -------------------------------------------------------------------- metrics


def metrics(conn: sqlite3.Connection, now: float | None = None) -> dict:
    now = _now(now)
    by_status = {s: 0 for s in STATUSES}
    for row in conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"):
        by_status[row["status"]] = row["n"]

    ready = conn.execute(
        "SELECT COUNT(*) AS n, MIN(run_at) AS oldest FROM jobs "
        "WHERE status = 'queued' AND run_at <= ?",
        (now,),
    ).fetchone()
    avg = conn.execute(
        "SELECT AVG(duration_ms) AS avg FROM jobs WHERE status = 'succeeded'"
    ).fetchone()["avg"]

    finished = by_status["succeeded"] + by_status["dead"]
    return {
        "queue_depth": ready["n"],  # due right now, waiting for a worker
        "scheduled_retries": by_status["queued"] - ready["n"],  # waiting out a backoff
        "by_status": by_status,
        "avg_duration_ms": round(avg, 2) if avg is not None else None,
        "success_rate": round(by_status["succeeded"] / finished, 4) if finished else None,
        "oldest_ready_age_seconds": round(now - ready["oldest"], 2) if ready["oldest"] else 0.0,
    }


def metrics_prometheus(conn: sqlite3.Connection, now: float | None = None) -> str:
    m = metrics(conn, now)
    lines = [
        "# HELP jobly_jobs Number of jobs by status.",
        "# TYPE jobly_jobs gauge",
    ]
    lines += [f'jobly_jobs{{status="{s}"}} {n}' for s, n in m["by_status"].items()]
    lines += [
        "# HELP jobly_queue_depth Jobs due now and waiting for a worker.",
        "# TYPE jobly_queue_depth gauge",
        f"jobly_queue_depth {m['queue_depth']}",
        "# HELP jobly_scheduled_retries Jobs waiting out a retry backoff.",
        "# TYPE jobly_scheduled_retries gauge",
        f"jobly_scheduled_retries {m['scheduled_retries']}",
        "# HELP jobly_oldest_ready_age_seconds Age of the oldest job waiting for a worker.",
        "# TYPE jobly_oldest_ready_age_seconds gauge",
        f"jobly_oldest_ready_age_seconds {m['oldest_ready_age_seconds']}",
    ]
    if m["avg_duration_ms"] is not None:
        lines += [
            "# HELP jobly_avg_duration_ms Mean handler time of succeeded jobs.",
            "# TYPE jobly_avg_duration_ms gauge",
            f"jobly_avg_duration_ms {m['avg_duration_ms']}",
        ]
    return "\n".join(lines) + "\n"
