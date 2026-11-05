"""Run workers:  python -m worker --processes 3"""
import argparse
import multiprocessing as mp
import signal
import sys

from app.db import init_db
from worker.runner import run_worker


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m worker", description="Jobly worker")
    parser.add_argument("--processes", type=int, default=1, help="worker processes to run")
    parser.add_argument("--poll-interval", type=float, default=0.5,
                        help="seconds to sleep when the queue is empty")
    parser.add_argument("--job-timeout", type=float, default=10.0,
                        help="give up on a single attempt after this many seconds")
    parser.add_argument("--lock-timeout", type=float, default=None,
                        help="requeue a running job after this long (default: 3x job timeout, min 60)")
    parser.add_argument("--backoff-base", type=float, default=2.0,
                        help="first retry delay ceiling in seconds; doubles each attempt")
    args = parser.parse_args(argv)

    lock_timeout = args.lock_timeout or max(60.0, 3 * args.job_timeout)
    if lock_timeout <= args.job_timeout:
        parser.error("--lock-timeout must be larger than --job-timeout, or healthy jobs get stolen")

    init_db()
    kwargs = dict(
        poll_interval=args.poll_interval,
        job_timeout=args.job_timeout,
        lock_timeout=lock_timeout,
        backoff_base=args.backoff_base,
    )

    if args.processes == 1:
        run_worker(**kwargs)
        return 0

    procs = [
        mp.Process(target=run_worker, kwargs=kwargs, name=f"worker-{i + 1}")
        for i in range(args.processes)
    ]
    for p in procs:
        p.start()

    def shutdown(*_):  # forward Ctrl+C / `docker stop` so each worker finishes its job
        for p in procs:
            if p.is_alive():
                p.terminate()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    for p in procs:
        p.join()
    return 0


if __name__ == "__main__":
    sys.exit(main())
