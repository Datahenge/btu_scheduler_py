""" btu_py/lib/rq.py """

from __future__ import annotations  # Defers evalulation of type annonations; hopefully unnecessary once Python 3.14 is released.
from dataclasses import dataclass
from datetime import datetime as DateTimeType
from typing import Union
import uuid
from zoneinfo import ZoneInfo

import redis
import rq

# BTU
from btu_py import get_config, get_logger
from btu_py.lib.utils import whatis


NoneType = type(None)
RQ_JOB_PREFIX = "rq:job:my_postgres_site::"

def datetime_to_rq_date_string(some_datetime):
	return some_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def create_connection():
	"""
	Creates a connection to the Redis database.
	"""
	if not get_config().as_dictionary():
		raise RuntimeError("Application configuration is not loaded.")

	return redis.Redis(
		host= get_config().as_dictionary()["rq_host"],
		port= get_config().as_dictionary()["rq_port"],
		decode_responses=True
	)


@dataclass
class RQJobWrapper():
	"""
	Wrapper for the third-party RQ Job object.
	"""
	job_key: str
	job_key_short: str
	created_at: DateTimeType
	data: bytes
	description: str
	ended_at: Union[NoneType, DateTimeType]
	enqueued_at: Union[NoneType, DateTimeType]
	exc_info: Union[NoneType, str]
	last_heartbeat: str
	meta: Union[NoneType, bytes]
	origin: str
	result_ttl: Union[NoneType, str]
	started_at: Union[NoneType, str]
	status: Union[NoneType, str]  # not initially populated
	timeout: int
	worker_name: str
	rq_job: rq.Job

	@staticmethod
	def new_with_defaults() -> RQJobWrapper:

		# example: 11f83e81-83ea-4df2-aa7e-cd12d8dec779
		uuid_string: str = uuid.uuid4() # .to_hyphenated().to_string();
		return RQJobWrapper (
			job_key = f"{RQ_JOB_PREFIX}:{uuid_string}",
			job_key_short = uuid_string,
			created_at = DateTimeType.now(ZoneInfo("UTC")),
			description = "",
			data = None,
			ended_at = None,
			enqueued_at = None,  # not initially populated
			exc_info = None,
			last_heartbeat = DateTimeType.now(ZoneInfo("UTC")),  # to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
			meta = None,
			origin = "default",  # temporarily
			result_ttl = None,
			started_at = None,
			status = "queued",
			timeout = 3600,  # default of 3600 seconds (1 hour)
			worker_name = "",
			rq_job = None
		)

	def create_only(self):
		"""
		Save the RQ struct to the Redis database, but do not enqueue.
		"""
		redis_conn = create_connection()

		# NOTE: Cannot pass NoneTypes here
		values_dict = {
			"status": self.status,
			"worker_name": self.worker_name,
			"ended_at": self.ended_at or "",
			"result_ttl": self.result_ttl or "",
			"enqueued_at":  self.enqueued_at or "",
			"last_heartbeat": datetime_to_rq_date_string(self.last_heartbeat) if self.last_heartbeat else "",
			"origin": self.origin,
			"description": self.description,
			"started_at": self.started_at or "",
			"created_at": datetime_to_rq_date_string(self.created_at),
			"timeout": self.timeout
		}

		# When using hset_multiple, the values must all be of the same Type.
		# In the case below, an Array of Tuples, where the Tuple is (&str, &String)
		print(f"Writing Job Key {self.job_key} ...")
		redis_conn.hmset(self.job_key, values_dict)
		redis_conn.hset(self.job_key, "data", self.data)
		if self.meta:
			redis_conn.hset(self.job_key, "meta", self.meta)

	def create_and_enqueue(self):
		self.create_only()
		enqueue_job_immediate(self.job_key)


def enqueue_job_immediate(existing_job_id: str):
	"""
	Add a pre-existing RQ Job to a queue, so a worker can pick it up.
	"""
	redis_conn = create_connection()

	from rq.registry import StartedJobRegistry
	registry = StartedJobRegistry('default', connection=redis_conn)
	queued_job_ids = registry.get_queue().job_ids
	print(f"queued_job_ids: {queued_job_ids}")

	this_job = rq.job.Job.fetch(existing_job_id, connection=redis_conn)

	# First, add the Queue name to 'rq:queues' (it could be there already)
	queue_key: str = f"rq:queue:{this_job.origin}"
	some_result = redis_conn.sadd("rq:queues", queue_key)
	if not some_result:
		get_logger().error("Error during enqueue_job_immediate()")
		raise IOError(some_result)

	# Then add the Job's ID to the queue.
	# NOTE: The return value of 'rpush' is an integer, representing the length of the List, after the completion of the push operation.
	push_result = redis_conn.rpush(queue_key, existing_job_id)
	whatis(push_result)
	if not push_result:
		raise IOError(push_result)
	get_logger.info(f"Enqueued RQ Job '{existing_job_id}' for immediate execution. Length of list after 'rpush' operation: {push_result}")
