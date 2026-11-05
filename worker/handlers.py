"""Job handlers. A handler takes a payload dict and returns a result dict.

- Raise any exception -> the job is retried with backoff.
- Raise PermanentError -> retrying can't help, the job goes straight to dead.

Adding a job type = write one function and add it to HANDLERS (and to JobType in
app/schemas.py so the API accepts it).
"""
import csv
import io
import random
import time

import httpx


class PermanentError(Exception):
    """The job can never succeed (bad payload, 404 target...). Don't retry."""


def webhook_delivery(payload: dict) -> dict:
    url = payload.get("url")
    if not url:
        raise PermanentError("payload.url is missing")

    response = httpx.post(url, json=payload.get("body", {}), timeout=payload.get("timeout", 5.0))

    # 4xx means *we* sent something wrong; retrying the same request won't fix it.
    # The exceptions are 408 (timeout) and 429 (rate limited): those are temporary.
    if 400 <= response.status_code < 500 and response.status_code not in (408, 429):
        raise PermanentError(f"target rejected the request with HTTP {response.status_code}")
    response.raise_for_status()  # 5xx (and 408/429) -> normal retryable failure
    return {"status_code": response.status_code}


def url_check(payload: dict) -> dict:
    """Fetch a URL and report status and latency. A 500 is a *result*, not a failure."""
    url = payload.get("url")
    if not url:
        raise PermanentError("payload.url is missing")
    started = time.monotonic()
    response = httpx.get(url, timeout=payload.get("timeout", 5.0), follow_redirects=True)
    return {
        "status_code": response.status_code,
        "latency_ms": round((time.monotonic() - started) * 1000, 1),
    }


def report_generate(payload: dict) -> dict:
    """A slow, CPU-ish job: good for exercising timeouts and concurrency."""
    rows = int(payload.get("rows", 1000))
    time.sleep(float(payload.get("seconds", 0)))
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["id", "value"])
    for i in range(rows):
        writer.writerow([i, random.random()])
    return {"rows": rows, "bytes": len(buffer.getvalue())}


HANDLERS = {
    "webhook_delivery": webhook_delivery,
    "url_check": url_check,
    "report_generate": report_generate,
}
