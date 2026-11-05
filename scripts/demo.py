"""Submit jobs aimed at the flaky endpoint and watch Jobly cope.

Terminal 1:  uvicorn app.main:app
Terminal 2:  python -m worker --processes 3 --job-timeout 3 --backoff-base 1
Terminal 3:  python scripts/demo.py
"""
import argparse
import time
import uuid

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://127.0.0.1:8000")
    parser.add_argument("--target", default=None,
                        help="URL the jobs deliver to (default: the API's own flaky endpoint)")
    parser.add_argument("--n", type=int, default=20)
    parser.add_argument("--max-attempts", type=int, default=4)
    args = parser.parse_args()

    target = args.target or f"{args.api}/_demo/flaky?fail=0.4&hang=0.15&hang_seconds=8"
    run_id = uuid.uuid4().hex[:8]  # keeps idempotency keys unique per demo run

    with httpx.Client(base_url=args.api, timeout=10) as api:
        ids = []
        for i in range(args.n):
            r = api.post(
                "/jobs",
                json={"type": "webhook_delivery", "payload": {"url": target, "body": {"n": i}},
                      "max_attempts": args.max_attempts},
                headers={"Idempotency-Key": f"demo-{run_id}-{i}"},
            )
            r.raise_for_status()
            ids.append(r.json()["id"])
        print(f"submitted {len(ids)} jobs -> {target}\n")

        while True:
            stats = api.get("/stats").json()
            by = stats["by_status"]
            print(f"queued={by['queued']:>2} (ready {stats['queue_depth']}, backing off "
                  f"{stats['scheduled_retries']})  running={by['running']}  "
                  f"succeeded={by['succeeded']:>2}  dead={by['dead']}")
            if by["queued"] == 0 and by["running"] == 0:
                break
            time.sleep(2)

        jobs = [api.get(f"/jobs/{i}").json() for i in ids]

    print("\nfinal outcome")
    for status in ("succeeded", "dead"):
        group = [j for j in jobs if j["status"] == status]
        attempts = sorted(j["attempts"] for j in group)
        print(f"  {status:<9} {len(group):>2} jobs   attempts used: {attempts}")
    retried_ok = sum(1 for j in jobs if j["status"] == "succeeded" and j["attempts"] > 1)
    print(f"\n{retried_ok} job(s) failed at least once and still succeeded thanks to retries.")
    for j in (j for j in jobs if j["status"] == "dead"):
        print(f"dead-lettered: job {j['id']}: {j['last_error']}")


if __name__ == "__main__":
    main()
