"""btu_scheduler/lib/btu_rq.py"""

# NOTE: Deliberately naming this "btu_rq" to distinguish from the Third Party library namespace "rq"

from __future__ import (
	annotations,
)  # Defers evalulation of type annonations; hopefully unnecessary once Python 3.14 is released.

import uuid
from dataclasses import dataclass
from datetime import datetime as DateTimeType
from typing import Union
from zoneinfo import ZoneInfo

import redis
import rq
import structlog

# BTU
from btu_scheduler.lib.config import load_config

NoneType = type(None)
log = structlog.get_logger(__name__)


def datetime_to_rq_date_string(some_datetime):
	return some_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def create_connection(decode_responses=True):
	"""
	Creates a connection to the Redis database.
	"""
	config = load_config()
	return redis.StrictRedis(
		host=config.rq_host,
		port=config.rq_port,
		decode_responses=decode_responses,
	)



@dataclass
class RQJobWrapper:
	"""
	Wrapper for the third-party RQ Job object.
	"""

	job_key: str
	job_key_short: str
	fully_qualified_key: str  # includes the prefix rq::job
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
	rq_job_object: rq.Job

	@staticmethod
	def new_with_defaults() -> RQJobWrapper:
		uuid_string: str = uuid.uuid4()  # example: 11f83e81-83ea-4df2-aa7e-cd12d8dec779
		new_job_key = f"{load_config().jobs_site_prefix}|{uuid_string}"
		return RQJobWrapper(
			job_key=new_job_key,  # erp.farmtopeople.com|11f83e81-83ea-4df2-aa7e-cd12d8dec779
			fully_qualified_key=f"rq:job:{new_job_key}",
			job_key_short=uuid_string,
			created_at=DateTimeType.now(ZoneInfo("UTC")),
			description="",
			data=None,
			ended_at=None,
			enqueued_at=None,  # not initially populated
			exc_info=None,
			last_heartbeat=DateTimeType.now(ZoneInfo("UTC")),
			meta=None,
			origin="default",  # temporarily
			result_ttl=None,
			started_at=None,
			status="queued",  # techically not enqueued until later, but there is no other initial Status to choose from.
			timeout=3600,  # default of 3600 seconds (1 hour)
			worker_name="",
			rq_job_object=None,
		)

