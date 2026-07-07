
import asyncio
import json
import random

from worker.job_handlers import JOB_HANDLERS
from worker.utils import (
    MAX_RETRIES,
    QUEUE_KEY,
    WORKER_TIMEOUT,
    claim_job,
    create_db_pool,
    create_redis_client,
    log_event,
    mark_processing,
    move_to_dlq,
    publish_status,
    requeue_job,
    unmark_processing,
    update_job_completed,
    update_job_failed,
    update_job_retry,
)


async def execute_job(job_data: dict):
   
    handler = JOB_HANDLERS[job_data["type"]]

    if asyncio.iscoroutinefunction(handler):
        return await handler(job_data["payload"])
    else:
        
        return await asyncio.to_thread(handler, job_data["payload"])


async def process_job(job_data: dict, pool, redis_client) -> None:
    
    job_id = job_data["id"]

   
    if not await claim_job(pool, job_id):
        log_event("SKIPPED", job_id, extra="claim lost or job not pending")
        return


    await mark_processing(redis_client, job_id)
    log_event("STARTED", job_id, extra=f"type={job_data['type']}")

  

    
    await publish_status(
        redis_client,
        job_id=job_id,
        user_id=job_data.get("user_id"),
        status="running", 
        job_type=job_data["type"],
    )

   
    # STEP 3: EXECUTE, THEN HANDLE SUCCESS OR FAILURE
   
    try:
   
        result = await execute_job(job_data)

     
        await update_job_completed(pool, job_id, result)
        log_event("COMPLETED", job_id, extra=f"result={result}")

    
        await publish_status(
            redis_client,
            job_id=job_id,
            user_id=job_data.get("user_id"),
            status="completed",        
            job_type=job_data["type"],
            result=result,        
        )


    except Exception as exc:
        
        retry_count = job_data.get("retry_count", 0)

     
        error_history = job_data.get("error_history", [])
        error_history.append(error_message)

        if retry_count < MAX_RETRIES:
           
            # RETRY PATH: exponential backoff, then re-queue.
           
            # Exponential backoff WITH JITTER: (2^retry_count) + random(0,1).
            
            # WHY exponential (not a fixed delay): if the failure is a
            # temporarily overloaded downstream system, retrying
            # immediately just hammers it again; growing the delay on
            # each successive failure (1s, 2s, 4s) gives it room to
            # recover instead of piling on harder.
            #
            # WHY jitter — THE THUNDERING HERD PROBLEM: imagine Gmail
            # blips for 2 seconds and 500 email jobs fail in the same
            # instant. With PURE exponential backoff they all share the
            # same schedule, so all 500 retry at exactly t+1s — a
            # synchronized stampede that knocks the recovering service
            # straight back over, fails together again, stampedes again
            # at t+2s, forever. The failures stay perfectly correlated.
            # Adding random.uniform(0, 1) desynchronizes them: 500
            # retries smear across a full second instead of landing on
            # one millisecond. AWS's architecture blog ("Exponential
            # Backoff and Jitter") made this the industry-standard retry
            # recipe — every AWS SDK ships it by default.
            wait_seconds = (2 ** retry_count) + random.uniform(0, 1)
            log_event(
                "RETRY_SCHEDULED",
                job_id,
                extra=f"attempt={retry_count + 1}/{MAX_RETRIES} wait={wait_seconds:.2f}s error={error_message}",
            )

           
            await asyncio.sleep(wait_seconds)

            
            job_data["retry_count"] = retry_count + 1
            job_data["error_history"] = error_history

        
            await requeue_job(redis_client, job_data)

      
            await update_job_retry(pool, job_id, retry_count + 1, error_message)

            # Dashboard ko batao: attempt fail hua, retry hone wali hai
     
            await publish_status(
                redis_client,
                job_id=job_id,
                user_id=job_data.get("user_id"),
                status="failed",           
                job_type=job_data["type"],
                error=error_message,    
                retry_count=retry_count+1,     
            )




        else:
           
            # DEAD LETTER PATH: MAX_RETRIES exhausted, give up.
         
            
            await move_to_dlq(pool, job_id, job_data["type"], job_data["payload"], error_history)

        
            await update_job_failed(pool, job_id, error_message)

            log_event(
                "MOVED_TO_DLQ",
                job_id,
                extra=f"attempts={retry_count + 1} final_error={error_message}",
            )
                        
            await publish_status(
                redis_client,
                job_id=job_id,
                user_id=job_data.get("user_id"),
                status="dead",
                job_type=job_data["type"],
                error=error_message,         
            )


    finally:
      
        # STEP 4: REMOVE FROM THE IN-FLIGHT LEDGER — on EVERY exit path.
        # success -> job is done; retry -> job is back ON the queue (the
        # queue itself now guards it, not this ledger); DLQ -> job is
        # permanently parked. In all three cases no worker is holding it
        # anymore. `finally` (not a call at the end of each branch)
        # guarantees even an unexpected exception in the bookkeeping above
        # can't leave a phantom entry that recovery would later requeue.
        # The ONLY way a job_id survives in jobs:processing is a genuine
        # process death — exactly the signal recover_stuck_jobs() wants.
      
        await unmark_processing(redis_client, job_id)


async def main() -> None:
    """
    The worker's entry point: set up connections, then loop forever
    pulling and processing jobs.
    """
    log_event("WORKER_STARTING", "-")

    pool = await create_db_pool()
    redis_client = create_redis_client()

    log_event("WORKER_READY", "-", extra=f"queue={QUEUE_KEY} max_retries={MAX_RETRIES} poll_timeout={WORKER_TIMEOUT}s")

    try:
      
        while True:
         
            result = await redis_client.brpop(QUEUE_KEY, timeout=WORKER_TIMEOUT)

            if result is None:
                continue
            _, job_json = result

            
            try:
                job_data = json.loads(job_json)
            except json.JSONDecodeError as exc:
                log_event("MALFORMED_JOB_DROPPED", "-", extra=f"error={exc} raw={job_json!r}")
                continue

            job_id = job_data.get("id", "unknown")
            log_event("PICKED_UP", job_id, extra=f"type={job_data.get('type')}")

         
         
            try:
                await process_job(job_data, pool, redis_client)
            except Exception as exc:
                log_event("WORKER_LOOP_ERROR", job_id, extra=f"error={exc}")
    finally:
        
        await pool.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
