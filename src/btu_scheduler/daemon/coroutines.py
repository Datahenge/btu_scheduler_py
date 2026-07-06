"""btu_scheduler/daemon/coroutines.py"""

import asyncio
import json

import structlog

from btu_scheduler.lib import scheduler
from btu_scheduler.lib.config import load_config
from btu_scheduler.lib.structs import BtuTaskSchedule
from btu_scheduler.lib.utils import Stopwatch

log = structlog.get_logger(__name__)

# Redis key where incoming commands are delivered from the Frappe web server.
# Must match REDIS_COMMAND_QUEUE in btu/btu_api/scheduler.py.
REDIS_COMMAND_QUEUE = "btu:scheduler:commands"

async def internal_queue_consumer(shared_queue: asyncio.Queue[str]) -> None:
	"""
	Reads TSIKs from the internal coroutine Queue, and adds them to Python RQ.
	"""
	while True:
		if shared_queue.qsize():
			next_task_schedule_id = (
				await shared_queue.get()
			)  # NOTE: The coroutine will hang out here, doing nothing, until something shows up in the Queue.
			task_schedule: BtuTaskSchedule = await BtuTaskSchedule.init_from_schedule_key(next_task_schedule_id)
			if task_schedule:
				scheduler.add_task_schedule_to_rq(task_schedule)
				log.debug(
					f"IQM: Added task schedule to Redis Key 'btu_scheduler:task_execution_times'.  Size of internal queue is now {shared_queue.qsize()}"
				)
			else:
				log.error(
					f"IQM: Unable to construct a BtuTaskSchedule object from Task Schedule ID = {next_task_schedule_id}"
				)

		await asyncio.sleep(1)  # blocking request for just a moment


async def internal_queue_producer(shared_queue: asyncio.Queue[str]) -> None:
	"""
	Every N seconds, refill the Internal Queue with -all- Task Schedule IDs.

	This is a type of "safety net" for the BTU system.  By performing a "full refresh" of RQ,
	we can be confident that Tasks are always running.  Even if the RQ database is flushed or emptied,
	it will be refilled automatically after a while!

	As the queue is filled, Thread 1 handles consuming and procesing each TSIK.
	"""

	log.info("Initializing coroutine 'internal_queue_producer()' ...")
	stopwatch = Stopwatch()
	while True:
		elapsed_seconds = stopwatch.get_elapsed_seconds_total()  # calculate elapsed seconds since last Queue Repopulate
		if elapsed_seconds > load_config().full_refresh_internal_secs:  # If sufficient time has passed ...
			log.debug(
				f"Producer: {elapsed_seconds} seconds have elapsed.  Time for a full-write of Task Schedule Keys in Redis!"
			)
			result = await scheduler.queue_full_refill(shared_queue, check_rq=True)
			if result:
				log.debug(f"  * Internal queue contains a total of {shared_queue.qsize()} values.")
				scheduler.rq_print_scheduled_tasks()  # log the Task Schedule:
			stopwatch.reset()  # reset the stopwatch and begin a new countdown

		await asyncio.sleep(1)  # blocking request, yields controls to another coroutine for a while.


async def review_next_execution_times(shared_queue: asyncio.Queue[str]) -> None:
	"""
	----------------
	Thread #3:  Enqueue Tasks into RQ

	Every N seconds, examine the Next Execution Time for all scheduled RQ Jobs (this information is stored in RQ as a Unix timestamps)
	   If the Next Execution Time is in the past?  Then place the RQ Job into the appropriate queue.  RQ and Workers take over from there.
	  ----------------
	"""
	await asyncio.sleep(5)  # One-time delay of execution: this gives the other coroutines a chance to initialize.
	log.info(
		"Starting coroutine review_next_execution_times(), adding eligible RQ Jobs to RQ Queues at the appropriate time."
	)
	while True:
		log.debug("Thread 3: Attempting to add new Jobs to RQ...")
		# This thread requires a lock on the Internal Queue, so that after a Task runs, it can be rescheduled.
		stopwatch = Stopwatch()
		await scheduler.check_and_run_eligible_task_schedules(shared_queue)
		elapsed_seconds = stopwatch.get_elapsed_seconds_total()  # time just spent working on RQ database.
		# I want this thread to execute at roughly the same interval.
		# By subtracting the Time Elapsed above, from the desired Wait Time, we know how much longer the thread should sleep.
		await asyncio.sleep(
			load_config().scheduler_polling_interval - elapsed_seconds
		)  # wait N seconds before trying again.


async def _dispatch_redis_command(request_type: str, request_content: str, internal_queue: asyncio.Queue[str]) -> None:
	"""
	Execute a command that arrived via the Redis RPC queue.

	Called after the receipt ACK has already been sent, so this function
	can take as long as it needs without affecting the caller's wait time.
	"""
	log.info(f"Redis RPC: dispatching '{request_type}' with content '{request_content}'.")

	if request_type == "ping":
		log.info("Redis RPC: ping received.")
		return

	if request_type == "create_task_schedule":
		await internal_queue.put(request_content)
		log.info(f"Redis RPC: enqueued Task Schedule ID '{request_content}'.")
		return

	if request_type == "cancel_task_schedule":
		try:
			scheduler.rq_cancel_scheduled_task(request_content)
			scheduler.rq_print_scheduled_tasks()
			log.info(f"Redis RPC: cancelled Task Schedule '{request_content}'.")
		except Exception as ex:
			log.error(f"Redis RPC: error cancelling Task Schedule '{request_content}': {ex}")
		return

	log.warning(f"Redis RPC: unrecognised request_type '{request_type}'.")


async def redis_command_listener(internal_queue: asyncio.Queue[str]) -> None:
	"""
	Primary control-plane listener for the BTU Scheduler daemon.

	Monitors REDIS_COMMAND_QUEUE using a blocking BRPOP (run in a thread executor
	so it does not stall the asyncio event loop).  On receiving a command:

	  1. Immediately pushes a receipt ACK to the caller's response_key.
	     The Frappe web worker is blocking on BLPOP(response_key) and unblocks here.
	     This happens before any execution work, keeping the caller's wait near-instant.

	  2. Dispatches the command to _dispatch_redis_command(), which runs after the
	     caller has already received its acknowledgement.

	See docs/scheduler_redis_rpc.md for the full protocol description.
	"""
	from btu_scheduler.lib.btu_rq import create_connection

	redis_conn = create_connection()
	loop = asyncio.get_running_loop()

	log.info(f"Redis RPC command listener started, monitoring queue '{REDIS_COMMAND_QUEUE}'.")

	while True:
		try:
			# Blocking BRPOP with a 1-second timeout, run in a thread so the event loop
			# stays free for the scheduler's other coroutines during the wait.
			result = await loop.run_in_executor(None, lambda: redis_conn.blpop([REDIS_COMMAND_QUEUE], timeout=1))

			if result is None:
				continue  # nothing arrived within the 1-second window; loop back

			_, raw_message = result

			try:
				command = json.loads(raw_message)
			except json.JSONDecodeError:
				log.warning(f"Redis RPC: received non-JSON message, discarding: {raw_message!r}")
				continue

			request_type = command.get("request_type", "")
			request_content = command.get("request_content", "")
			response_key = command.get("response_key")

			# Step 1: ACK receipt BEFORE executing anything.
			# The Frappe web worker is blocked on BLPOP(response_key); this unblocks it.
			if response_key:
				ack = json.dumps(
					{
						"status": "ok",
						"request_type": request_type,
						"message": "Command received by BTU Scheduler.",
					}
				)
				redis_conn.lpush(response_key, ack)
				redis_conn.expire(response_key, 60)  # auto-clean orphaned keys if caller died

			# Step 2: Now execute the command (caller is already unblocked).
			await _dispatch_redis_command(request_type, request_content, internal_queue)

		except Exception as ex:
			log.error(f"Redis RPC listener unhandled error: {ex}")
			await asyncio.sleep(1)  # brief back-off before resuming
