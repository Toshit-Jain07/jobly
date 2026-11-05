import sqlite3
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import PlainTextResponse

from app import store
from app.db import get_db, init_db
from app.flaky import router as flaky_router
from app.schemas import JobCreate, JobOut, JobStatus


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Jobly", description="A small, reliable job queue.", lifespan=lifespan)
app.include_router(flaky_router)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs", response_model=JobOut)
def create_job(
    body: JobCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None),
    conn: sqlite3.Connection = Depends(get_db),
):
    key = idempotency_key or str(uuid.uuid4())
    row, created = store.create_job(conn, body.type, body.payload, key, body.max_attempts)

    # Same key reused for a *different* request is a client bug, not a retry.
    if not created and (
        row["type"] != body.type or row["payload"] != store.dump_payload(body.payload)
    ):
        raise HTTPException(
            status_code=409,
            detail="Idempotency-Key was already used with a different request",
        )

    response.status_code = 201 if created else 200
    return store.job_to_dict(row)


@app.get("/jobs", response_model=list[JobOut])
def list_jobs(
    status: JobStatus | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Newest first. `?status=dead` is how you inspect the dead-letter queue."""
    if status:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status = ? ORDER BY id DESC LIMIT ?", (status, limit)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [store.job_to_dict(r) for r in rows]


@app.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: int, conn: sqlite3.Connection = Depends(get_db)):
    row = store.get_job(conn, job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return store.job_to_dict(row)


@app.post("/jobs/{job_id}/retry", response_model=JobOut)
def retry_job(job_id: int, conn: sqlite3.Connection = Depends(get_db)):
    """Requeue a dead-lettered job with a fresh attempt budget."""
    if store.get_job(conn, job_id) is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if not store.retry_dead(conn, job_id):
        raise HTTPException(status_code=409, detail="Only dead jobs can be retried")
    return store.job_to_dict(store.get_job(conn, job_id))


@app.get("/stats")
def stats(conn: sqlite3.Connection = Depends(get_db)):
    """Queue health as JSON."""
    return store.metrics(conn)


@app.get("/metrics", response_class=PlainTextResponse)
def prometheus_metrics(conn: sqlite3.Connection = Depends(get_db)):
    """Same numbers in Prometheus text format, ready for a scraper."""
    return store.metrics_prometheus(conn)
