""" btu_py/lib/structs/__init__.py """

# pylint: disable=too-many-instance-attributes

from dataclasses import dataclass
from datetime import datetime as DateTimeType
import requests
from typing import Union
from zoneinfo import ZoneInfo


from btu_py import get_config_data
from btu_py.lib import btu_cron
from btu_py.lib.btu_rq import RQJobWrapper
from btu_py.lib.sql import get_task_by_id, get_task_schedule_by_id
from btu_py.lib.structs.sanchez import get_pickled_function_from_web
from btu_py.lib.utils import whatis, get_frappe_base_url

NoneType = type(None)


@dataclass
class BtuTask():
	task_key: str
	desc_short: str
	desc_long: str
	arguments: Union[NoneType, str]
	path_to_function: str  # example:  btu.manual_tests.ping_with_wait
	max_task_duration: int # example:  600 seconds

	@staticmethod
	async def init_from_task_key(task_key: str):
		task_data: dict = await get_task_by_id(task_key)  # read from the SQL Database
		if not task_data:
			raise IOError(f"No SQL row returned by get_task_by_id() for primary key = '{task_key}'")

		return BtuTask(
			task_key=task_data["task_key"],
			desc_short=task_data["desc_short"],
			desc_long=task_data["desc_long"],
			arguments=task_data["arguments"],
			path_to_function=task_data["path_to_function"],
			max_task_duration=task_data["max_task_duration"]
		)

	async def convert_to_wrapped_rq_job(self) -> RQJobWrapper:
		"""
		Use a BTU Task record to construct an RQ Job Wrapper; don't modify Redis yet.
		"""
		wrapped_job = RQJobWrapper.new_with_defaults()
		wrapped_job.description = self.desc_short
		byte_result = get_pickled_function_from_web(self.task_key, None)
		wrapped_job.data = byte_result
		wrapped_job.timeout = self.max_task_duration
		return wrapped_job


@dataclass
class BtuTaskSchedule():
	id: str
	task_key: str
	task_description: str
	enabled: bool
	queue_name: str
	argument_overrides: Union[NoneType, str]
	schedule_description: str
	cron_string: str
	cron_timezone: ZoneInfo
	redis_job_id: Union[NoneType, str] = None  # Not all schedules will have a Redis Job yet

	@staticmethod
	async def init_from_schedule_key(schedule_key: str) -> object:
		schedule_data: dict = await get_task_schedule_by_id(schedule_key)  # read from the SQL Database
		if not schedule_data:
			raise IOError(f"No SQL row returned by get_task_schedule_by_id() for primary key = '{schedule_key}'")

		return BtuTaskSchedule(
			id=schedule_data["name"],
			task_key=schedule_data["task"],
			task_description=schedule_data["task_description"],
			enabled=schedule_data["enabled"],
			queue_name=schedule_data["queue_name"],
			argument_overrides=schedule_data["argument_overrides"],
			schedule_description=schedule_data["schedule_description"],
			cron_string=schedule_data["cron_string"],
			cron_timezone=schedule_data["cron_timezone"],
		)

	async def to_rq_job_wrapper(self):
		"""
		Given a BTU Task Schedule, construct an instance of RQJobWrapper; does not modify Redis.
		"""
		wrapped_job = RQJobWrapper.new_with_defaults()
		wrapped_job.description = self.task_description
		wrapped_job.origin = self.queue_name

		task = await BtuTask.init_from_task_key(self.task_key)
		wrapped_job.data =await get_pickled_function_from_web(self.task_key, self.id)
		wrapped_job.timeout = task.max_task_duration
		return wrapped_job

	def get_next_runtimes(self, from_utc_datetime=None, number_results=1) -> list[DateTimeType]:

		return btu_cron.tz_cron_to_utc_datetimes(
				self.cron_string,
				self.cron_timezone,
				from_utc_datetime,
				number_results
		)

	def enqueue_for_next_available_worker(self):
		"""
		Call Frappe website to immediately enqueue a Task as an RQ Job.
		"""

		config_data = get_config_data()
		url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.enqueue_for_next_available_worker"
		headers = {
			"Authorization": config_data.webserver_token,
			"Content-Type": "application/json",
		}
		# If Frappe is running via gunicorn, in DNS Multi-tenancy mode, then we have to pass a "Host" header.
		if config_data.webserver_host_header:
			headers["Host"] = config_data.webserver_host_header

		response = requests.post(
			url=url,
			headers=headers,
			params = {
				"task_schedule_key": self.id
			},
			timeout=30
		)

		if response.status_code != 200:
			raise IOError(f"Unexpected response code from Frappe Framework web server: {response.status_code}")

		print(response.json)
