"""
worker/worker_pool.py
------------------
PURPOSE: Phase 2 entry point for the worker container. Instead of one
         worker loop, runs WORKER_COUNT (default 3) independent worker
         PROCESSES in parallel, supervises them (restarting any that
         crash), runs crash-recovery once at startup, and shuts the whole
         pool down gracefully on SIGTERM/SIGINT.

HOW IT FITS IN THE SYSTEM:
  docker-compose.yml's worker service now runs `python -m worker.worker_pool`
  (previously `python -m worker.worker`). Each child process runs the
  UNCHANGED worker/worker.py main() loop — this file adds parallelism
  AROUND the existing worker, it doesn't reimplement it.

MULTIPROCESSING vs THREADING — WHY PROCESSES:
  Python's GIL (Global Interpreter Lock) allows only ONE thread to execute
  Python bytecode at a time within a process. Threads interleave; they
  never truly run Python code simultaneously on multiple CPU cores. For
  I/O-bound work (waiting on SMTP) threads are fine — a waiting thread
  releases the GIL — but our jobs include real CPU work (Pillow pixel
  encoding, pandas transforms) where threads would take turns on one core.
  multiprocessing.Process sidesteps the GIL entirely: each worker is a
  FULL OS process with its own Python interpreter, its own GIL, its own
  memory — three workers genuinely use three CPU cores at once.

  The extra isolation is itself a reliability feature: a segfault in a C
  extension (Pillow decoding a hostile image) kills ONE process; its two
  siblings keep serving jobs, and this supervisor restarts the casualty.
  A thread crashing that hard takes the whole process with it.

HOW 3 WORKERS SHARE ONE QUEUE SAFELY:
  They don't coordinate with each other at all — coordination is
  delegated to the datastores:
    Redis BRPOP is ATOMIC: when 3 workers block on the same list and a
      job arrives, Redis hands it to exactly ONE of them. Two workers can
      never pop the same list element.
    Postgres claim_job() (SELECT FOR UPDATE SKIP LOCKED) is the second
      gate: even if the same job_id somehow reaches two workers (manual
      re-push, requeue race), only one can flip it pending->running.
  Workers being ignorant of each other is what makes scaling trivial:
  WORKER_COUNT=10 changes nothing about correctness, only throughput.
"""

import asyncio
import multiprocessing
import os
import signal
import sys
import time

from worker.utils import (
    create_db_pool,
    create_redis_client,
    log_event,
    recover_stuck_jobs,
)

# How many worker processes to run. Env-tunable so scaling the pool is a
# .env edit + container restart, not a code change. 3 is a sensible
# default for a small machine: enough to demonstrate real parallelism and
# consume a burst, few enough to not starve Postgres of connections
# (each worker opens its own pool of up to 5 — see worker/utils.py).
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "3"))

# Set by the signal handler, read by the supervision loop. A plain module
# global (not a multiprocessing primitive) because it's only ever touched
# in THIS supervisor process — children are told to stop via SIGTERM.
_shutting_down = False


def _run_worker(worker_number: int) -> None:
    """
    Child-process entry point: become worker-N and run the standard
    worker loop forever.

    WHY WORKER_ID GOES INTO os.environ HERE: this line executes INSIDE
    the child process (multiprocessing invokes this function there), so
    each child gets its own private copy of the variable — worker-1's
    env is invisible to worker-2. worker/utils.py::log_event reads it on
    every log call, which is how `docker-compose logs` shows exactly
    which worker picked which job.
    """
    os.environ["WORKER_ID"] = f"worker-{worker_number}"

    # Import here (not at module top) so the import happens in the child.
    # With the default 'fork' start method on Linux this is a no-op
    # (children inherit parent imports), but it keeps this file correct
    # under the 'spawn' method too (macOS/Windows default), where children
    # start with a fresh interpreter and import everything themselves.
    from worker.worker import main

    # Each child runs its OWN asyncio event loop with its OWN Postgres
    # pool and Redis client. Sharing connections across processes is
    # never safe (two processes interleaving bytes on one socket =
    # corrupted protocol stream), so each worker connects independently.
    asyncio.run(main())


def _spawn(worker_number: int) -> multiprocessing.Process:
    """Create + start one worker child process."""
    process = multiprocessing.Process(
        target=_run_worker,
        args=(worker_number,),
        # name shows up in `ps` output and our own logs.
        name=f"worker-{worker_number}",
        # daemon=False (the default): daemon processes are killed abruptly
        # the instant the parent exits, mid-job. We want to control
        # shutdown ourselves (terminate -> join with timeout, below).
        daemon=False,
    )
    process.start()
    log_event("WORKER_SPAWNED", "-", extra=f"name={process.name} pid={process.pid}")
    return process


async def _startup_recovery() -> None:
    """
    Crash-recovery sweep — runs ONCE, before any worker starts.

    These are jobs that were being processed when a worker crashed — we
    requeue them automatically (see worker/utils.py::recover_stuck_jobs).

    WHY HERE AND NOT IN EACH WORKER: if all 3 workers swept the set on
    startup simultaneously, they could each requeue the same orphan —
    three copies of one job. Running it once in the supervisor, BEFORE
    the pool exists, makes the sweep race-free by construction. (And even
    if a duplicate somehow slipped through, claim_job() would let only
    one copy actually execute — defense in depth.)
    """
    pool = await create_db_pool()
    redis_client = create_redis_client()
    try:
        recovered = await recover_stuck_jobs(pool, redis_client)
        log_event("STARTUP_RECOVERY_DONE", "-", extra=f"requeued={recovered}")
    finally:
        await pool.close()
        await redis_client.aclose()


def _handle_shutdown_signal(signum, frame) -> None:
    """
    Signal handler for SIGTERM (docker stop / docker-compose down) and
    SIGINT (Ctrl+C). Only flips a flag — the actual teardown happens in
    the supervision loop, because signal handlers must stay tiny: they
    interrupt the program at an arbitrary instruction, so doing real work
    (joining processes, closing sockets) inside one invites deadlocks.
    """
    global _shutting_down
    _shutting_down = True
    log_event("SHUTDOWN_SIGNAL", "-", extra=f"signal={signal.Signals(signum).name}")


def main() -> None:
    """
    Supervisor lifecycle:
      1. Run crash recovery (requeue orphans from a previous run).
      2. Spawn WORKER_COUNT worker processes.
      3. Supervise: if a worker dies unexpectedly, log it and spawn a
         replacement — one poisoned job or C-extension segfault must
         never permanently shrink the pool.
      4. On SIGTERM/SIGINT: terminate all workers, wait for them to exit
         (with a timeout), then exit cleanly.
    """
    log_event("POOL_STARTING", "-", extra=f"worker_count={WORKER_COUNT}")

    # Register shutdown handlers BEFORE spawning children, so there is no
    # window where a docker stop could kill the supervisor and leave
    # orphaned worker processes running unsupervised.
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)

    # Step 1: recovery sweep (see _startup_recovery docstring).
    asyncio.run(_startup_recovery())

    # Step 2: spawn the pool. Numbering starts at 1 (worker-1..worker-3)
    # to match human expectations in logs.
    processes: dict[int, multiprocessing.Process] = {
        n: _spawn(n) for n in range(1, WORKER_COUNT + 1)
    }

    # Step 3: supervision loop. Wakes once per second — cheap (a few
    # is_alive() checks, which just read process state) and fast enough
    # that a crashed worker is replaced within ~1s.
    while not _shutting_down:
        for worker_number, process in processes.items():
            if not process.is_alive() and not _shutting_down:
                # exitcode 0 would mean the worker loop returned cleanly,
                # which worker.py never does voluntarily — any exit here
                # is a crash (unhandled exception, OOM-kill, segfault).
                log_event(
                    "WORKER_DIED",
                    "-",
                    extra=f"name={process.name} exitcode={process.exitcode} — respawning",
                )
                # The crashed worker's in-flight job (if any) is still in
                # jobs:processing — it will be recovered on the NEXT pool
                # restart. (A future improvement: run the recovery sweep
                # here too, now that we know a worker just died.)
                processes[worker_number] = _spawn(worker_number)
        time.sleep(1)

    # Step 4: graceful shutdown. terminate() sends SIGTERM to each child;
    # join(timeout) waits for it to actually exit so we don't leave
    # zombies behind. Workers may die mid-job here — that's exactly the
    # crash-recovery path: the job stays in jobs:processing and the next
    # startup's recovery sweep requeues it. Nothing is lost.
    log_event("POOL_STOPPING", "-", extra="terminating workers")
    for process in processes.values():
        process.terminate()
    for process in processes.values():
        process.join(timeout=10)
        if process.is_alive():
            # Refused to die within 10s (stuck in uninterruptible I/O?) —
            # escalate to SIGKILL. kill() cannot be ignored by the child.
            log_event("WORKER_KILL_ESCALATION", "-", extra=f"name={process.name}")
            process.kill()
            process.join()

    log_event("POOL_STOPPED", "-")
    sys.exit(0)


if __name__ == "__main__":
    main()
