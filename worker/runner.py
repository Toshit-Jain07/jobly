import json
import logging
import signal
import sqlite3
import threading
import time

from app import store
from worker.handlers import HANDLERS, PermanentError

log = logging.getLogger("jobly.worker")

RECOVER_EVERY = 10.0  # seconds between sweeps for jobs whose worker died


class JobTimeout(Exception):
    pass


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(processName)s %(message)s",
        datefmt="%H:%M:%S",
    )


def run_with_timeout(fn, payload: dict, timeout: float):
    """Run fn(payload), giving up after `timeout` seconds.

    Python can't kill a thread, so a timed-out handler keeps running in the
    background until it finishes on its own; we just stop waiting and treat the
    attempt as failed. Handlers should also set their own network timeouts.
    """
    box: dict = {}

    def target():
        try:
            box["result"] = fn(payload)
        except Exception as exc:  # reported to the caller below
            box["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise JobTimeout(f"timed out after {timeout}s")
    if "error" in box:
        raise box["error"]
    return box.get("result")


def process_job(
    conn: sqlite3.Connection,
    job: sqlite3.Row,
    handlers: dict | None = None,
    job_timeout: float = 10.0,
    backoff_base: float = store.BACKOFF_BASE,
) -> str | None:
    """Run one claimed job and record the outcome. Returns the job's new status."""
    handlers = HANDLERS if handlers is None else handlers
    label = f"job {job['id']} ({job['type']}) attempt {job['attempts']}/{job['max_attempts']}"

    handler = handlers.get(job["type"])
    if handler is None:
        outcome = store.fail_job(conn, job, f"no handler for type {job['type']!r}", retryable=False)
        log.info("%s: no handler -> dead", label)
        return outcome[0] if outcome else None

    started = time.monotonic()
    try:
        result = run_with_timeout(handler, json.loads(job["payload"]), job_timeout)
    except PermanentError as exc:
        outcome = store.fail_job(conn, job, str(exc), retryable=False)
        log.info("%s: permanent failure -> dead: %s", label, exc)
        return outcome[0] if outcome else None
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        outcome = store.fail_job(conn, job, message, backoff_base=backoff_base)
        if outcome is None:
            log.warning("%s: lost the lock, result discarded", label)
            return None
        status, run_at = outcome
        if status == "dead":
            log.info("%s: out of attempts -> dead (%s)", label, message)
        else:
            log.info("%s: failed, retry in %.1fs (%s)", label, run_at - time.time(), message)
        return status

    duration_ms = (time.monotonic() - started) * 1000
    if store.complete_job(conn, job, result, duration_ms):
        log.info("%s: succeeded in %.0fms", label, duration_ms)
        return "succeeded"
    log.warning("%s: lost the lock, result discarded", label)
    return None


def run_worker(
    poll_interval: float = 0.5,
    job_timeout: float = 10.0,
    lock_timeout: float = 60.0,
    backoff_base: float = store.BACKOFF_BASE,
    handlers: dict | None = None,
    stop_event: threading.Event | None = None,
    install_signals: bool = True,
) -> None:
    """Claim -> run -> record, until told to stop. Finishes the current job first."""
    configure_logging()
    stop = stop_event or threading.Event()
    if install_signals:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stop.set())

    conn = store.worker_connect()
    last_sweep = 0.0
    log.info("worker started")
    try:
        while not stop.is_set():
            try:
                if time.monotonic() - last_sweep >= RECOVER_EVERY:
                    last_sweep = time.monotonic()
                    recovered = store.recover_stuck(conn, lock_timeout)
                    if recovered:
                        log.warning("recovered %d stuck job(s) from dead workers", recovered)

                job = store.claim_job(conn)
                if job is None:
                    stop.wait(poll_interval)
                    continue
                process_job(conn, job, handlers, job_timeout, backoff_base)
            except sqlite3.OperationalError as exc:  # e.g. database busy; try again
                log.warning("database error, retrying: %s", exc)
                stop.wait(poll_interval)
    finally:
        conn.close()
        log.info("worker stopped")
