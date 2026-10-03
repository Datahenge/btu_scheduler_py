"""btu_scheduler/lib/scheduler.py"""

import asyncio
from dataclasses import dataclass
from datetime import datetime as DateTimeType
from zoneinfo import ZoneInfo

import redis
import structlog

from btu_scheduler.lib.data_access import get_enabled_task_schedules
from btu_scheduler.lib.scheduled_store import get_scheduled_store
from btu_scheduler.lib.structs import BtuTaskSchedule

DST_FIRED_CACHE_TTL_SECS = 90_000  # 25 hours — long enough to outlast any DST transition
log = structlog.get_logger(__name__)


def _dst_fired_cache_key(task_schedule_id: str, utc_datetime: DateTimeType, cron_timezone: ZoneInfo) -> str:
	"""Redis key recording that a local time slot was executed (DST fall-back guard)."""
	local_dt = utc_datetime.astimezone(cron_timezone)
	return f"btu:fired:{task_schedule_id}:{local_dt.strftime('%Y-%m-%d:%H:%M')}"


@dataclass
class RQScheduledTask:
	task_schedule_id: str
	next_execution_as_unix_timestamp: int  # not supporting fractions of seconds.
	next_execution_as_datetime_utc: DateTimeType

	def to_key(self) -> str:
		"""Task Scheduled Instance Key (TSIK).  Example: TS-000003|1742677041"""
		return f"{self.task_schedule_id}|{self.next_execution_as_unix_timestamp}"

	@staticmethod
	def from_key(key: str) -> "RQScheduledTask":
		"""Parse a Task Scheduled Instance Key string: '{task_schedule_id}|{unix_timestamp}'."""
		parts = key.split("|")
		ts = int(parts[1])
		return RQScheduledTask(
			task_schedule_id=parts[0],
			next_execution_as_unix_timestamp=ts,
			next_execution_as_datetime_utc=DateTimeType.fromtimestamp(ts, tz=ZoneInfo("UTC")),
		)

	@staticmethod
	def from_tuple(task_schedule_id: str, unix_timestamp: int) -> "RQScheduledTask":
		ts = int(unix_timestamp)
		return RQScheduledTask(
			task_schedule_id=task_schedule_id,
			next_execution_as_unix_timestamp=ts,
			next_execution_as_datetime_utc=DateTimeType.fromtimestamp(ts, tz=ZoneInfo("UTC")),
		)

	@staticmethod
	def sort_list_by_id(list_of_rq_scheduled_task) -> list:
		return sorted(list_of_rq_scheduled_task, key=lambda x: x.task_schedule_id)

	@staticmethod
	def sort_list_by_next_datetime(list_of_rq_scheduled_task) -> list:
		return sorted(list_of_rq_scheduled_task, key=lambda x: x.next_execution_as_unix_timestamp)


def add_task_schedule_to_rq(task_schedule: BtuTaskSchedule):
	"""
	Developer Notes:

	1. This function's only caller is coroutine 'internal_queue_consumer'

	2. This function's concept was derived from the Python 'rq_scheduler' library.  In that library, the public
		entrypoint (from the website) was named a function 'cron()'.  That cron() function did a few things:

		* Created an RQ Job object in the Redis datbase.
		* Calculated the RQ Job's next execution time, in UTC.
		* Added a 'Z' key to Redis where the value of 'Score' is the next UTC Runtime, but expressed as a Unix Time.

			self.connection.zadd("rq:scheduler:scheduled_jobs", {job.id: to_unix(scheduled_time)})

	3. I am making a deliberate decision to -not- create an RQ Job at this time.  But instead, to create the RQ
		Job later, when it's time to actually run it.

		My reasoning is this: a Frappe web user might edit the definition of a Task between the time it was scheduled
		in RQ, and the time it actually executes.  This would make the RQ Job stale and invalid.  So anytime someone edits
		a BTU Task, I would have to rebuild all related Task Schedules.  Instead, by waiting until execution time, I only have
		to react to *Schedule* modifications in the Frappe web app; not Task modifications.

		The disadvantage: if the Frappe Web Server is not online and accepting REST API requests, when it's
		time to run a Task Schedule?  Then BTU Scheduler will fail: it cannot create a pickled RQ Job without the Frappe web server's APIs.

		Of course, if the Frappe web server is offline, that's usually an indication of a larger problem.  In which case, the
		BTU Task Schedule might fail anyway.  So overall, I think the benefits of waiting to create RQ Jobs outweighs the drawbacks.

	4. What if a race condition happens, where a newer Schedule arrives, before a previous Schedule has been sent to a Python RQ?
		A redis sorted set can only store the same key once.  If we make the Task Schedule ID the key, the newer "next date" will overwrite
		the previous one.

		To handle this, the Sorted Set "key" must be the concatentation of Task Schedule ID and Unix Time.
		I'm going to call this a TSIK (Task Scheduled Instance Key)
	"""

	# Notice the line below: Only retrieving the 1st value from the result list.  Later, it might be helpful to fetch
	# multiple Next Execution Times, because of time zone shifts around Daylight Savings.

	next_runtimes: list[DateTimeType] = task_schedule.get_next_runtimes()
	if not next_runtimes:
		return []
	rq_scheduled_task: RQScheduledTask = RQScheduledTask(
		task_schedule_id=task_schedule.id,
		next_execution_as_unix_timestamp=int(next_runtimes[0].timestamp()),  # force into an Integer
		next_execution_as_datetime_utc=next_runtimes[0],
	)

	# NOTE:  The response from add() is the number of records added.  Value 0 means the record
	#        already existed (or, in 'direct' mode, that Redis was unreachable), and no write happened.
	members_added = get_scheduled_store().add(
		rq_scheduled_task.to_key(), rq_scheduled_task.next_execution_as_unix_timestamp
	)

	if members_added > 0:
		messages = []
		messages.append(f"add_task_schedule_to_rq() : The response from 'zadd' = {members_added}")
		messages.append(f"Task Schedule ID {task_schedule.id} is being monitored for future execution.")
		messages.append(
			f"Next Execution Time (UTC) for Task Schedule {task_schedule.id} = {rq_scheduled_task.next_execution_as_datetime_utc}"
		)
		next_execution_time_local = rq_scheduled_task.next_execution_as_datetime_utc.astimezone(
			task_schedule.cron_timezone
		)
		messages.append(
			f"Next Execution Time ({task_schedule.cron_timezone}) for Task Schedule {task_schedule.id} = {next_execution_time_local}"
		)
		for each_message in messages:
			log.debug(each_message)

	# NOTE: At the conclusion of this function, if you examined the Redis database:
	#   1.  "Score" is the Next Execution Time (as a Unix timestamp)
	#   2.  "Member" is the BTU Task Schedule identifier.
	#   3.  This particular Task Schedule would not have an actual Python RQ Job yet.


def fetch_task_schedules_ready_for_rq(sched_before_unix_time: int) -> list:
	"""
	Read the BTU section of RQ, and return the Jobs that are scheduled to execute before a specific Unix Timestamp.
	"""
	# NOTE: Some cleverness below, courtesy of 'rq-scheduler' project.  For this particular key, the Z-score
	# represents the Unix Timestamp the Job is supposed to execute on.  By fetching ALL values below a certain
	# threshold (Timestamp), the program knows precisely which Task Schedules to enqueue.

	log.debug("fetch_task_schedules_ready_for_rq() : reviewing 'Next Execution Times' for each Task Schedule...")
	zranges: list = get_scheduled_store().range_le(sched_before_unix_time)
	if not zranges:
		return []

	if len(zranges) > 0:
		log.info(f"Found {len(zranges)} Task Schedules that qualify for immediate execution.")

	# The strings in the list are a concatenation: Task Schedule ID, pipe character, Unix Time.
	# Need to split off the trailing Unix Time, to obtain a list of Task Schedules.
	task_schedules_to_enqueue = [RQScheduledTask.from_key(each) for each in zranges]

	# Finally, return a list of Task Schedule identifiers:
	return task_schedules_to_enqueue


async def check_and_run_eligible_task_schedules(internal_queue: asyncio.Queue[str]):
	"""
	Examine the Next Execution Time for all scheduled RQ Jobs (this information is stored in RQ as a Unix timestamps)
	If the Next Execution Time is in the past?  Then place the RQ Job into the appropriate queue.  RQ and Workers take over from there.
	"""
	current_datetime_utc = DateTimeType.now(ZoneInfo("UTC"))
	current_timestamp = current_datetime_utc.timestamp()

	# Developer Note: This function is analgous to the 'rq-scheduler' Python function: 'Scheduler.enqueue_jobs()'
	for task_schedule_instance in fetch_task_schedules_ready_for_rq(current_timestamp):
		await run_immediate_scheduled_task(task_schedule_instance, internal_queue)


async def run_immediate_scheduled_task(task_schedule_instance: RQScheduledTask, internal_queue: asyncio.Queue[str]):
	"""
	Create a Python RQ Task and assign to a Queue, so the next available worker can run it.
	"""
	log.info(
		f">>>>> Time To Make The Donuts! (enqueuing Job '{task_schedule_instance.task_schedule_id}' for immediate execution)"
	)
	store = get_scheduled_store()

	# 1. Read the Task Schedule definition (SQL or Frappe REST, depending on connectivity_mode).
	try:
		task_schedule = await BtuTaskSchedule.init_from_schedule_key(task_schedule_instance.task_schedule_id)
	except Exception as ex:
		log.error(f"Unable to read Task Schedule definition. Error = {ex}")
		return

	if not task_schedule:
		log.error(f"Unable to read a BTU Task Schedule '{task_schedule_instance.task_schedule_id}'.")
		return

	# 2. Exit early if the Task Schedule is disabled (this should be a rare scenario, but definitely worth checking.)
	if not task_schedule.enabled:
		log.warning(f"Task Schedule {task_schedule.id} is disabled; BTU will neither execute nor re-queue.")
		return

	# 3. DST fall-back guard + post-enqueue cleanup — wrapped for connection resilience (direct mode only;
	#    the in-memory store used in webserver mode never raises ConnectionError).
	try:
		dst_cache_key = _dst_fired_cache_key(
			task_schedule.id,
			task_schedule_instance.next_execution_as_datetime_utc,
			task_schedule.cron_timezone,
		)
		if store.dst_fired_exists(dst_cache_key):
			local_slot = task_schedule_instance.next_execution_as_datetime_utc.astimezone(task_schedule.cron_timezone)
			log.warning(
				f"DST duplicate suppressed: Task Schedule {task_schedule.id} already fired for "
				f"local slot {local_slot:%Y-%m-%d %H:%M} ({task_schedule.cron_timezone}). Skipping re-fire."
			)
			store.remove(task_schedule_instance.to_key())
			await internal_queue.put(task_schedule_instance.task_schedule_id)
			return

		try:
			task_schedule.enqueue_for_next_available_worker()
		except Exception as ex:
			log.error(f"Error while attempting to queue job for execution: {ex}")
			return

		# Record this local slot as fired so DST fall-back cannot re-fire it.
		store.dst_fired_set(dst_cache_key, DST_FIRED_CACHE_TTL_SECS)

		# IMPORTANT: Remove this Task from the schedule store (so it doesn't accidentally get executed twice)
		if not store.remove(task_schedule_instance.to_key()):
			log.error(f"Unable to remove Task Schedule Instance '{task_schedule_instance.to_key()}' from the store.")
			return

		# Finally, recalculate the next Run Time.
		# Easy enough; just push the Task Schedule ID back into the -Internal- Queue!
		# It will get processed automatically during the next thread cycle.
		await internal_queue.put(task_schedule_instance.task_schedule_id)

	except redis.exceptions.ConnectionError:
		log.error(
			f"run_immediate_scheduled_task(): Lost Redis connection for Task Schedule {task_schedule_instance.task_schedule_id}."
		)


def rq_get_scheduled_tasks() -> list[RQScheduledTask]:
	"""
	Query the active ScheduledStore for all pending Task Schedule instances.
	"""
	list_of_tsik_string = get_scheduled_store().range_all()
	wrapped_result = [RQScheduledTask.from_key(each) for each in list_of_tsik_string]
	return wrapped_result


def rq_cancel_scheduled_task(task_schedule_id: str) -> None:
	"""
	Remove a Task Schedule from the ScheduledStore, to prevent it from executing in the future.
	"""
	# Members are keyed by TSIK ("{task_schedule_id}|{unix_timestamp}"), so removing by
	# Task Schedule ID alone requires "starts_with" logic.
	removed = get_scheduled_store().remove_by_prefix(task_schedule_id)

	if removed:
		log.info("Scheduled Task successfully removed from the store.")
	else:
		log.info("Scheduled Task not found in the store.")


def rq_print_scheduled_tasks():
	tasks: list[RQScheduledTask] = rq_get_scheduled_tasks()
	for result in sorted(tasks, key=lambda x: x.task_schedule_id):
		message: str = f"Task Schedule {result.task_schedule_id} is scheduled to occur later at {result.next_execution_as_datetime_utc}"
		log.info(message)


def clear_all_scheduled_tasks() -> bool:
	"""
	Clear all scheduled tasks from the active ScheduledStore.
	"""
	get_scheduled_store().clear()
	return True


async def queue_full_refill(internal_queue: asyncio.Queue[str], *, check_rq: bool = False) -> int:
	"""
	Queries the Frappe database, adding every active Task Schedule to BTU internal queue.

	When ``check_rq`` is True, log a warning if enabled schedules exist in SQL but none
	are present in Redis (the safety net should have repopulated RQ by then).
	"""
	rows_added = 0
	enabled_schedules = await get_enabled_task_schedules()
	if check_rq and enabled_schedules and not rq_get_scheduled_tasks():
		log.warning("Enabled Task Schedules exist in the database but none are scheduled in Redis.")
	if not enabled_schedules:
		return 0

	for each_row in enabled_schedules:  # each_row is a dictionary with 2 keys: 'name' and 'desc_short'
		await internal_queue.put(
			each_row["schedule_key"]
		)  # add the schedule_key ('name') of a BTU Task Schedule document.
		rows_added += 1
	if rows_added:
		log.debug(f"  * filled internal queue with {rows_added} Task Schedule identifiers.")
	return rows_added
