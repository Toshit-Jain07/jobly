import os
import sqlite3

STATUSES = ("queued", "running", "succeeded", "dead")

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    type             TEXT    NOT NULL,
    payload          TEXT    NOT NULL,              -- JSON string
    idempotency_key  TEXT    NOT NULL UNIQUE,       -- the DB enforces "no duplicates"
    status           TEXT    NOT NULL DEFAULT 'queued'
                     CHECK (status IN {STATUSES}),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    run_at           REAL    NOT NULL,              -- epoch seconds: earliest time to run
    locked_at        REAL,                          -- set when a worker claims the job
    last_error       TEXT,
    result           TEXT,                          -- JSON string, set on success
    duration_ms      REAL,                          -- handler time, set on success
    created_at       REAL    NOT NULL,
    updated_at       REAL    NOT NULL
);

-- Workers constantly ask: "oldest queued job whose run_at has passed".
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, run_at);
"""

# Columns added after the first release. Lets an older jobly.db upgrade in place.
_ADDED_COLUMNS = {"result": "TEXT", "duration_ms": "REAL"}


def db_path() -> str:
    # Read at call time so tests can point this at a temp file.
    return os.environ.get("JOBLY_DB", "jobly.db")


def connect() -> sqlite3.Connection:
    # check_same_thread=False: FastAPI may open the connection in one threadpool
    # thread and run the endpoint in another. Safe because every request gets its
    # own connection and never shares it.
    conn = sqlite3.connect(db_path(), timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # readers don't block the writer
    conn.execute("PRAGMA busy_timeout=5000")  # wait instead of failing on a locked DB
    return conn


def init_db() -> None:
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        existing = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
        for column, col_type in _ADDED_COLUMNS.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} {col_type}")
        conn.commit()
    finally:
        conn.close()


def get_db():
    """FastAPI dependency: one short-lived connection per request."""
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()
