""" btu_py/lib/scheduler.py """

# pylint: disable=logging-fstring-interpolation

# import asyncio
from dataclasses import dataclass
from datetime import datetime as DateTimeType
# from typing import Union  # pylint: disable=unused-import
from zoneinfo import ZoneInfo

import temporal_lib

import btu_py
from btu_py import get_logger
from btu_py.lib.rq import create_connection, enqueue_job_immediate
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
		naive_value = DateTimeType.fromtimestamp(self.next_datetime_as_unix_timestamp)
		return temporal_lib.core.localize_datetime(naive_value, ZoneInfo("UTC"))

	def __str__(self) -> str:
		return f"{self.task_schedule_id()} at {self.next_execution_as_datetime_utc()}"

	@staticmethod
	def from_tuple(task_schedule_id, next_execution_timestamp):
		return TSIK(
			f"{task_schedule_id}|{str(next_execution_timestamp)}"
		)


@dataclass
class RQScheduledTask():

	task_schedule_id: str
	next_execution_as_unix_timestamp: str
	next_execution_as_datetime_utc: DateTimeType

	def to_tsik(self) -> str:
		return print(f"{self.task_schedule_id}|{self.next_datetime_unix}")

	@staticmethod
	def from_tsik(tsik: TSIK) -> object:

		return RQScheduledTask(
			task_schedule_id=tsik.task_schedule_id(),
			next_execution_as_unix_timestamp=tsik.next_execution_as_unix_timestamp(),
			next_execution_as_datetime_utc=tsik.next_execution_as_datetime_utc()
		)

	@staticmethod
	def from_tuple(task_schedule_id: str, unix_timestamp: int):
		new_tsik = TSIK.from_tuple(task_schedule_id, unix_timestamp)
		return RQScheduledTask.from_tsik(new_tsik)

	@staticmethod
	def sort_list_by_id(list_of_rq_scheduled_task) -> list:
		# Consumes the current VecRQScheduledTask, and returns another that is sorted by Task Schedule ID.

		# TODO: Sort by the ID
		return list_of_rq_scheduled_task

	@staticmethod
	def sort_list_by_next_datetime(list_of_rq_scheduled_task) -> list:
		# Consumes the current VecRQScheduledTask, and returns another that is sorted by Task Schedule ID.

		# TODO: Sort by the next_datetime
		return list_of_rq_scheduled_task


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

	next_runtimes = task_schedule.get_next_runtimes()
	if not next_runtimes:
		return

	rq_scheduled_task: RQScheduledTask = RQScheduledTask(
		task_schedule_id=task_schedule.id,
		next_execution_as_unix_timestamp=next_runtimes[0].timestamp(),
		next_execution_as_datetime_utc=next_runtimes[0]
	)

	# Establish connection to Redis, and perform a ZADD
	redis_conn = create_connection()
	if not redis_conn:
		return

	result = redis_conn.zadd(
		RQ_KEY_SCHEDULED_TASKS,
		rq_scheduled_task.to_tsik(),
		rq_scheduled_task.next_datetime_unix
	)
	# whatis(result)

	match result:

		case 'redis':
			get_logger().debug(f"Result from 'zadd' is Ok, with the following payload: {result}")
			# Developer Note: I believe a result of 1 means Redis wrote a new record.
			#				 A result of 0 means the record already existed, and no write was necessary.

			message1 = f"Task Schedule ID {task_schedule.id} is being monitored for future execution."
			# If application configuration has a good Time Zone string, print Next Execution Time in local time...
			if btu_py.get_config_data().timezone():
				message2 = f"Next Execution Time ({btu_py.get_config_data().timezone()}) for Task Schedule {task_schedule} = {rq_scheduled_task.next_datetime_utc.with_timezone(btu_py.get_config_data().timezone())}"
				message3 = f"Next Execution Time (UTC) for Task Schedule {task_schedule.id} = {rq_scheduled_task.next_datetime_utc}"
				get_logger().debug(message1, message2, message3)
			else:
				# Otherwise, just print in UTC
				message3: f"Next Execution Time (UTC) for Task Schedule {task_schedule.id} = {rq_scheduled_task.next_datetime_utc}"
				get_logger().debug(message1, message3)

		case _:
			raise IOError(f"Result from redis 'zadd' is Err, with the following payload: {result}")

	# Developer Notes:
	#    If you were to examine Redis at this time.
	#    "Score" is the Next Execution Time (as a Unix timestamp),
	#    "Member" is the BTU Task Schedule identifier.
	#    We haven't created an RQ Jobs for this Task Schedule yet.


def fetch_task_schedules_ready_for_rq(sched_before_unix_time: int) -> list:
	"""
	Read the BTU section of RQ, and return the Jobs that are scheduled to execute before a specific Unix Timestamp.
	"""

	# Developer Notes: Some cleverness below, courtesy of 'rq-scheduler' project.  For this particular key, the Z-score
	# represents the Unix Timestamp the Job is supposed to execute on.
	# By fetching ALL values below a certain threshold (Timestamp), the program knows precisely which Task Schedules to enqueue...

	# rq_print_scheduled_tasks(&app_config);

	get_logger().debug("Reviewing the 'Next Execution Times' for each Task Schedule in Redis...")

	redis_conn = create_connection()
	if not redis_conn:
		get_logger().debug("In lieu of a Redis Connection, returning an empty vector.")
		return []  # If cannot connect to Redis, do not panic the thread.  Instead, return an empty Vector.

	# TODO: As per Redis 6.2.0, the command 'zrangebyscore' is considered deprecated.
	# Please prefer using the ZRANGE command with the BYSCORE argument in new code.
	zranges: list = redis_conn.zrangebyscore(RQ_KEY_SCHEDULED_TASKS, 0, sched_before_unix_time)
	if not zranges:
		return []

	if len(zranges) > 0:
		get_logger().info(f"Found {zranges.len()} Task Schedules that qualify for immediate execution.")

	# The strings in the vector are a concatenation:  Task Schedule ID, pipe character, Unix Time.
	# Need to split off the trailing Unix Time, to obtain a list of Task Schedules.
	# NOTE: The syntax below is -very- "Rusty" (imo): maps the values returned by an iterator, using a closure function.
	task_schedules_to_enqueue = [ RQScheduledTask.from_tsik(TSIK(each)) for each in zranges ]

	# Finally, return a Vector of Task Schedule identifiers:
	return task_schedules_to_enqueue


def check_and_run_eligible_task_schedules(internal_queue: object):
	"""
	Examine the Next Execution Time for all scheduled RQ Jobs (this information is stored in RQ as a Unix timestamps)
	If the Next Execution Time is in the past?  Then place the RQ Job into the appropriate queue.  RQ and Workers take over from there.
	"""

	# Developer Note: This function is analgous to the 'rq-scheduler' Python function: 'Scheduler.enqueue_jobs()'
	task_schedule_instances: list = fetch_task_schedules_ready_for_rq(DateTimeType.now(ZoneInfo('UTC')).timestamp())

	for task_schedule_instance in task_schedule_instances:
		get_logger().info(f"Time to make the donuts! (enqueuing Redis Job '{task_schedule_instance.task_schedule_id}' for immediate execution)")
		run_immediate_scheduled_task(task_schedule_instance, internal_queue)
		# error!("Error while attempting to run Task Schedule {} : {}", task_schedule_instance.task_schedule_id, err);


def run_immediate_scheduled_task(task_schedule_instance: object, internal_queue: object):

	# 0. First remove the Task from the Schedule (so it doesn't get executed twice)
	redis_conn = create_connection()
	if not redis_conn:
		get_logger().warning("Early exit from run_immediate_scheduled_task(); cannot establish a connection to Redis database.")
		return  # If cannot connect to Redis, do not panic the thread.  Instead, return an empty Vector.

	redis_result = redis_conn.zrem(RQ_KEY_SCHEDULED_TASKS, task_schedule_instance.to_tsik())
	if redis_result != 1:
		get_logger().error(f"Unable to remove Task Schedule Instance using 'zrem'.  Response from Redis = {redis_result}")

	# 1. Read the MariaDB database to construct a BTU Task Schedule struct.
	task_schedule = BtuTaskSchedule.init_from_schedule_key(task_schedule_instance.task_schedule_id)
	if not task_schedule:
		raise IOError("Unable to read Task Schedule from MariaDB database.")

	# 2. Exit early if the Task Schedule is disabled (this should be a rare scenario, but definitely worth checking.)
	if task_schedule.enabled:
		get_logger().warning(f"Task Schedule {task_schedule.id} is disabled in SQL database; BTU will neither execute nor re-queue.")
		raise RuntimeError(f"Task Schedule {task_schedule.id} is disabled in SQL database; BTU will not execute or re-queue.")

	# 3. Create an RQ Job from the BtuTask struct.
	rq_job = task_schedule.to_rq_job()
	get_logger().debug(f"Created an RQJob object: {rq_job}")

	# 4. Save the new Job into Redis.
	rq_job.save_to_redis()

	# 5. Enqueue that job for immediate execution.
	try:
		enqueue_job_immediate(rq_job.job_key_short)
		get_logger().info(f"Successfully enqueued: '{rq_job.job_key_short}'")
	except Exception as ex:
		get_logger().error(f"Error while attempting to queue job for execution: {ex}")

	# 6. Recalculate the next Run Time.
	#	  Easy enough; just push the Task Schedule ID back into the -Internal- Queue!
	#	  It will get processed automatically during the next thread cycle.
	internal_queue.push_back(task_schedule_instance.task_schedule_id)


def rq_get_scheduled_tasks() -> list:
	"""
	Call RQ and request the list of values in "btu_scheduler:job_execution_times"
	"""
	redis_conn = create_connection()
	if not redis_conn:
		get_logger().debug("In lieu of a Redis Connection, returning an empty vector.")
		return []

	redis_result: [] = redis_conn.zscan(RQ_KEY_SCHEDULED_TASKS)  # list of tuples
	number_results = redis_result.len()
	wrapped_result = [ RQScheduledTask.from_tsik(each) for each in redis_result ]  # list of RQSchedule Task;  Map It?
	if number_results != wrapped_result.len():
		message_string = f"Unexpected Error: Number values in Redis: {number_results}.  Number values in VecRQScheduledTask: {wrapped_result}"
		get_logger().error(message_string)
		raise RuntimeError(message_string)
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

	tasks: list = rq_get_scheduled_tasks()
	local_time_zone = btu_py.get_config_data().timezone()

	print(f"There are {tasks.len()} BTU Tasks scheduled for automatic execution.")
	for result in sorted(tasks, lambda x: x.id):
		next_datetime_local = result.next_datetime_utc.with_timezone(local_time_zone)
		message: str = f"Task Schedule {result.task_schedule_id} is scheduled to occur later at {next_datetime_local}"
		if to_stdout:
			print(f"{message}")
		else:
			get_logger().info(message)



async def queue_full_refill(internal_queue: object) -> int:
	"""
	Queries the Frappe database, adding every active Task Schedule to BTU internal queue.
	"""
	rows_added = 0
	enabled_schedules =  await (get_enabled_task_schedules())
	print(f"Found {len(enabled_schedules)} enabled Task Schedules.")
	for each_row in enabled_schedules:  # each_row is a dictionary with 2 keys: 'name' and 'desc_short'
		await internal_queue.put(each_row['schedule_key'])  # add the schedule_key ('name') of a BTU Task Schedule document.
		rows_added += 1

	print(f"queue_full_refill() - Added {rows_added} rows to daemon's internal queue.")
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
