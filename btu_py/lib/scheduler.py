""" btu_py/lib/scheduler.py """

# pylint: disable=logging-fstring-interpolation

from dataclasses import dataclass
from datetime import datetime as DateTimeType
from zoneinfo import ZoneInfo

# from temporal_lib.core import localize_datetime

import btu_py
from btu_py import get_logger
from btu_py.lib.btu_rq import create_connection
from btu_py.lib.sql import get_enabled_task_schedules
from btu_py.lib.structs import BtuTaskSchedule
from btu_py.lib.utils import whatis


# static RQ_SCHEDULER_NAMESPACE_PREFIX: &'static str = "rq:scheduler_instance:";
# static RQ_KEY_SCHEDULER: &'static str = "rq:scheduler";
# static RQ_KEY_SCHEDULER_LOCK: &'static str = "rq:scheduler_lock";
RQ_KEY_SCHEDULED_TASKS = "btu_scheduler:task_execution_times"


@dataclass
class TSIK():
	"""
	Task Scheduled Instance Key
	Example:   TS-000003|1742489940
	"""
	key: str

	def task_schedule_id(self) -> str:
		return self.key.split("|")[0]

	def next_execution_as_unix_timestamp(self) -> int:
		"""
		Note: The timestamp is calculated from UTC.
		"""
		return int(self.key.split("|")[1])  # not allowing milliseconds; return an Integer.

	def next_execution_as_datetime_utc(self) -> DateTimeType:
		"""
		Task Schedule's next execution time, in UTC.
		"""
		# VERY IMPORTANT to specify the tz or it assumes local!
		result = DateTimeType.fromtimestamp(self.next_execution_as_unix_timestamp(), tz=ZoneInfo("UTC"))
		return result

	def __str__(self) -> str:
		return f"{self.task_schedule_id()} at {self.next_execution_as_datetime_utc()}"

	@staticmethod
	def from_tuple(task_schedule_id, next_execution_timestamp):
		return TSIK(
			f"{task_schedule_id}|{str(int(next_execution_timestamp))}"  # recase as Integer to throw out fractions of seconds.
		)


@dataclass
class RQScheduledTask():

	task_schedule_id: str
	next_execution_as_unix_timestamp: int  # not supporting fractions of seconds.
	next_execution_as_datetime_utc: DateTimeType

	def to_tsik(self) -> str:
		"""
		Example: TS-000003|1742677041
		"""
		return f"{self.task_schedule_id}|{self.next_execution_as_unix_timestamp}"

	@staticmethod
	def from_tsik(tsik: TSIK) -> object:

		if not isinstance(tsik, TSIK):
			raise TypeError(tsik)

		return RQScheduledTask(
			task_schedule_id=tsik.task_schedule_id(),
			next_execution_as_unix_timestamp=tsik.next_execution_as_unix_timestamp(),
			next_execution_as_datetime_utc=tsik.next_execution_as_datetime_utc()
		)

	@staticmethod
	def from_tuple(task_schedule_id: str, unix_timestamp: int):
		new_tsik = TSIK.from_tuple(task_schedule_id, int(unix_timestamp))
		return RQScheduledTask.from_tsik(new_tsik)

	@staticmethod
	def sort_list_by_id(list_of_rq_scheduled_task) -> list:
		return sorted(list_of_rq_scheduled_task, key=lambda x: x.task_schedule_id)

	@staticmethod
	def sort_list_by_next_datetime(list_of_rq_scheduled_task) -> list:
		return sorted(list_of_rq_scheduled_task, key=lambda x: x.next_execution_as_unix_timestamp)

	def next_execution_as_datetime_local(self):
		"""
		Returns the Next Execution Datetime in the local time zone.
		"""
		return self.next_execution_as_datetime_utc.astimezone(btu_py.get_config().timezone())


def add_task_schedule_to_rq(task_schedule: BtuTaskSchedule):
	'''
		Developer Notes:

		1. This function's only caller is couroutine 'internal_queue_consumer'

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
	'''

	# Notice the line below: Only retrieving the 1st value from the result vector.  Later, it might be helpful to fetch
	# multiple Next Execution Times, because of time zone shifts around Daylight Savings.

	next_runtimes: list[DateTimeType] = task_schedule.get_next_runtimes()
	if not next_runtimes:
		return []
	rq_scheduled_task: RQScheduledTask = RQScheduledTask(
		task_schedule_id=task_schedule.id,
		next_execution_as_unix_timestamp=int(next_runtimes[0].timestamp()),  # force into an Integer
		next_execution_as_datetime_utc=next_runtimes[0]
	)

	# print(f"Next Execution Time UTC: {rq_scheduled_task.next_execution_as_datetime_utc}")
	# print(f"Next Execution Timestamp: {rq_scheduled_task.next_execution_as_unix_timestamp}")
	# print(f"Next Execution TISK: {rq_scheduled_task.to_tsik()}")

	redis_conn = create_connection()
	if not redis_conn:
		return

	# NOTE:  Earlier versions of zadd accepted 3 values: "redis_key_name", data, score.
	#        Now you must pass 2: "redis_key_name" plus a dictionary:  {data1: score1, data2: score2}

	# NOTE:  The response from zadd is the number of records added.  Value 0 means the record already existed, and no write was necessary.

	members_added = redis_conn.zadd(
		RQ_KEY_SCHEDULED_TASKS,
		{ rq_scheduled_task.to_tsik() : rq_scheduled_task.next_execution_as_unix_timestamp }
	)

	if members_added > 0:
		messages = []
		messages.append(f"add_task_schedule_to_rq() : The response from 'zadd' = {members_added}")
		messages.append(f"Task Schedule ID {task_schedule.id} is being monitored for future execution.")
		messages.append(f"Next Execution Time (UTC) for Task Schedule {task_schedule.id} = {rq_scheduled_task.next_execution_as_datetime_utc}")
		# If application configuration has a good Time Zone string, print Next Execution Time in local time...
		if btu_py.get_config().timezone():
			next_execution_time_local = rq_scheduled_task.next_execution_as_datetime_utc.astimezone(btu_py.get_config().timezone())
			messages.append(f"Next Execution Time ({btu_py.get_config().timezone()}) for Task Schedule {task_schedule.id} = {next_execution_time_local}")
		for each_message in messages:
			get_logger().debug(each_message)

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

	# rq_print_scheduled_tasks(&app_config);

	get_logger().debug("fetch_task_schedules_ready_for_rq() : reviewing 'Next Execution Times' for each Task Schedule in Redis...")
	redis_conn = create_connection()
	if not redis_conn:
		get_logger().error("fetch_task_schedules_ready_for_rq(): Cannot establish connection to Redis; returning an empty list.")
		return []

	# TODO: As per Redis 6.2.0, the command 'zrangebyscore' is considered deprecated.
	# Please prefer using the ZRANGE command with the BYSCORE argument in new code.
	zranges: list = redis_conn.zrangebyscore(RQ_KEY_SCHEDULED_TASKS, 0, sched_before_unix_time)
	if not zranges:
		return []

	if len(zranges) > 0:
		get_logger().info(f"Found {len(zranges)} Task Schedules that qualify for immediate execution.")

	# The strings in the vector are a concatenation:  Task Schedule ID, pipe character, Unix Time.
	# Need to split off the trailing Unix Time, to obtain a list of Task Schedules.
	# NOTE: The syntax below is -very- "Rusty" (imo): maps the values returned by an iterator, using a closure function.
	task_schedules_to_enqueue = [ RQScheduledTask.from_tsik(TSIK(each)) for each in zranges ]

	# Finally, return a Vector of Task Schedule identifiers:
	return task_schedules_to_enqueue


async def check_and_run_eligible_task_schedules(internal_queue: object):
	"""
	Examine the Next Execution Time for all scheduled RQ Jobs (this information is stored in RQ as a Unix timestamps)
	If the Next Execution Time is in the past?  Then place the RQ Job into the appropriate queue.  RQ and Workers take over from there.
	"""
	current_datetime_utc = DateTimeType.now(ZoneInfo('UTC'))
	# get_logger().info(f"Current DateTime (UTC) is {current_datetime_utc}")
	current_timestamp = current_datetime_utc.timestamp()
	# get_logger().info(f"Current Timestamp (UTC) is {current_timestamp}")

	# Developer Note: This function is analgous to the 'rq-scheduler' Python function: 'Scheduler.enqueue_jobs()'
	for task_schedule_instance in fetch_task_schedules_ready_for_rq(current_timestamp):
		await run_immediate_scheduled_task(task_schedule_instance, internal_queue)


async def run_immediate_scheduled_task(task_schedule_instance: RQScheduledTask, internal_queue: object):
	"""
	Create a Python RQ Task and assign to a Queue, so the next available worker can run it.
	"""
	get_logger().info(f">>>>> Time To Make The Donuts! (enqueuing Redis Job '{task_schedule_instance.task_schedule_id}' for immediate execution)")
	redis_conn = create_connection()
	if not redis_conn:
		get_logger().error("Early exit from run_immediate_scheduled_task(); cannot establish a connection to Redis database.")
		return  # If cannot connect to Redis, do not panic the thread.  Instead, return an empty Vector.

	# 1. Read the SQL database to construct a BTU Task Schedule struct.
	try:
		task_schedule = await BtuTaskSchedule.init_from_schedule_key(task_schedule_instance.task_schedule_id)
	except Exception as ex:
		get_logger().error(f"Unable to read Task Schedule from the SQL database. Error = {ex}")
		return

	if not task_schedule:
		get_logger().error(f"Unable to read a BTU Task Schedule '{task_schedule_instance.task_schedule_id}' from SQL database.")
		return		

	# 2. Exit early if the Task Schedule is disabled (this should be a rare scenario, but definitely worth checking.)
	if not task_schedule.enabled:
		get_logger().warning(f"Task Schedule {task_schedule.id} is disabled in SQL database; BTU will neither execute nor re-queue.")
		return

	try:
		task_schedule.enqueue_for_next_available_worker()
		get_logger().info(f"Successfully enqueued: '{task_schedule.id}'")
	except Exception as ex:
		get_logger().error(f"Error while attempting to queue job for execution: {ex}")
		return

	# IMPORTANT: Remove this Task from the BTU Schedule Key (so it doesn't accidentally get executed twice)
	redis_result = redis_conn.zrem(RQ_KEY_SCHEDULED_TASKS, str(task_schedule_instance.to_tsik()))
	if redis_result != 1:
		get_logger().error(f"Unable to remove Task Schedule Instance using 'zrem'.  Response from Redis = {redis_result}")
		return

	# Finally, recalculate the next Run Time.
	#	  Easy enough; just push the Task Schedule ID back into the -Internal- Queue!
	#	  It will get processed automatically during the next thread cycle.
	await internal_queue.put(task_schedule_instance.task_schedule_id)


def rq_get_scheduled_tasks() -> list[RQScheduledTask]:
	"""
	Query Redis for the values held in key RQ_KEY_SCHEDULED_TASKS
	"""
	redis_conn = create_connection()
	if not redis_conn:
		get_logger().warning("In lieu of a Redis Connection, returning an empty vector.")
		return []

	redis_result: tuple = redis_conn.zscan(RQ_KEY_SCHEDULED_TASKS)  # (0, [('TS-000007|1742607180', 1742607180.0), ('TS-000007|1742607360', 1742607360.0) ])
	list_of_tsik_string = [ each[0] for each in redis_result[1] ]

	wrapped_result = [ RQScheduledTask.from_tsik(TSIK(each)) for each in list_of_tsik_string ]  # list of RQSchedule Task;  Map It?
	return wrapped_result


def rq_cancel_scheduled_task(task_schedule_id: str) -> tuple:
	"""
	Remove a Task Schedule from the Redis database, to prevent it from executing in the future.
	"""
	# As of changes made May 21st 2022, the members in the Ordered Set 'btu_scheduler:task_execution_times'
	# are not just Task Schedule ID's.  The Unix Time is a suffix.  Removing members now requires some "starts_with" logic.

	with create_connection() as redis_conn:

		# First, list all the keys using 'zrange btu_scheduler:task_execution_times 0 -1'
		all_task_schedules = redis_conn.zrange(RQ_KEY_SCHEDULED_TASKS, 0, -1)
		removed: bool = False

		for each_row in all_task_schedules:
			if each_row.starts_with(task_schedule_id):
				redis_result = redis_conn.zrem(RQ_KEY_SCHEDULED_TASKS, each_row)
				whatis(redis_result)
				removed = True

	if removed:
		get_logger().info("Scheduled Task successfully removed from Redis Queue.")
	else:
		get_logger().info("Scheduled Task not found in Redis Queue.")


def rq_print_scheduled_tasks(to_stdout: bool):

	tasks: list[RQScheduledTask] = rq_get_scheduled_tasks()
	for result in sorted(tasks, key=lambda x: x.task_schedule_id):
		next_datetime_local = result.next_execution_as_datetime_local()
		message: str = f"Task Schedule {result.task_schedule_id} is scheduled to occur later at {next_datetime_local}"
		if to_stdout:
			print(f"{message}")
		else:
			get_logger().info(message)



async def queue_full_refill(internal_queue: object) -> int:
	"""
	Queries the Frappe database, adding every active Task Schedule to BTU internal queue.
	"""
	btu_py.get_logger().debug(f"  * before refill, the queue contains {internal_queue.qsize()} values.")
	rows_added = 0
	enabled_schedules =  await (get_enabled_task_schedules())
	btu_py.get_logger().debug(f"  * queue_full_refill() found {len(enabled_schedules)} enabled Task Schedules.")
	for each_row in enabled_schedules:  # each_row is a dictionary with 2 keys: 'name' and 'desc_short'
		await internal_queue.put(each_row['schedule_key'])  # add the schedule_key ('name') of a BTU Task Schedule document.
		rows_added += 1
	if rows_added:
		btu_py.get_logger().debug(f"  * filled internal queue with {rows_added} Task Schedule identifiers.")
	return rows_added


#	add_task_to_rq(
#		cron_string,				# A cron string (e.g. "0 0 * * 0")
#		func=func,				  # Python function to be queued
#		args=[arg1, arg2],		  # Arguments passed into function when executed
#		kwargs={'foo': 'bar'},	  # Keyword arguments passed into function when executed
#		repeat=10,				  # Repeat this number of times (None means repeat forever)
#		queue_name=queue_name,	  # In which queue the job should be put in
#		meta={'foo': 'bar'},		# Arbitrary pickleable data on the job itself
#		use_local_timezone=False	# Interpret hours in the local timezone
#	)
