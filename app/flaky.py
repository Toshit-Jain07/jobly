"""A deliberately unreliable endpoint to demo retries, timeouts and the dead-letter queue.

Point a webhook_delivery job at it:
    http://127.0.0.1:8000/_demo/flaky?fail=0.4&hang=0.15&hang_seconds=8

`fail` is the chance of a 500, `hang` the chance of stalling for `hang_seconds`.
Everything else returns 200.
"""
import random
import time

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/_demo", tags=["demo"])


@router.api_route("/flaky", methods=["GET", "POST"])
def flaky(fail: float = 0.3, hang: float = 0.1, hang_seconds: float = 15.0):
    roll = random.random()
    if roll < hang:
        time.sleep(hang_seconds)
        return {"ok": True, "note": "slow response"}
    if roll < hang + fail:
        raise HTTPException(status_code=500, detail="flaky target failed")
    return {"ok": True}
