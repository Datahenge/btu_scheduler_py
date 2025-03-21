

# pylint: disable=too-many-instance-attributes

from dataclasses import dataclass
from typing import Union
from zoneinfo import ZoneInfo

# Third Party
from rq.job import Job

from btu_py.lib.config import AppConfig
from btu_py.lib.sql import get_task_schedule_by_id
from btu_py.lib.structs.sanchez import get_pickled_function_from_web
from btu_py.lib.utils import whatis

NoneType = type(None)


class RQJob():
	'''
	# example: 11f83e81-83ea-4df2-aa7e-cd12d8dec779
	let uuid_string: String = Uuid::new_v4().to_hyphenated().to_string();
	RQJob {
		job_key: format!("{}:{}", RQ_JOB_PREFIX, uuid_string),  // str(uuid4())
		job_key_short: uuid_string,
		created_at: chrono::offset::Utc::now(),
		description: "".to_owned(),
		data: Vec::new(),
		ended_at: None,
		enqueued_at: None,  // not initially populated
		exc_info: None,
		last_heartbeat: chrono::offset::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
		meta: None,
		origin: "erpnext-mybench:default".to_owned(),  // begin with the queue named 'default'
		result_ttl: None,
		started_at: None,
		status: None,
		timeout: 3600,  // default of 3600 seconds (1 hour)
		worker_name: "".to_owned(),
	}
	'''


@dataclass
class BtuTask():
	task_key: str
	desc_short: str
	desc_long: str
	arguments: Union[NoneType, str]
	path_to_function: str  # example:  btu.manual_tests.ping_with_wait
	max_task_duration: int # example:  600 seconds

	@staticmethod
	async def init_from_sql(task_key: str):
		pass

	async def convert_to_rqjob(self):

		job = Job.create(
			"foo"
		)
		'''
		let mut new_job: RQJob = RQJob::new_with_defaults();
		new_job.description = self.desc_short.clone();
		match crate::get_pickled_function_from_web(&self.task_key, None, app_config) {
			Ok(byte_result) => {
				new_job.data = byte_result;
			}
			Err(error_message) => {
				panic!("Error while requesting pickled Python function:\n{}", error_message);
			}
		}
		new_job.timeout = self.max_task_duration;
		new_job
		'''


@dataclass
class BtuTaskSchedule():
	id: str
	task: str
	task_description: str
	enabled: bool
	queue_name: str
	argument_overrides: Union[NoneType, str]
	schedule_description: str
	cron_string: str
	cron_timezone: ZoneInfo
	redis_job_id: Union[NoneType, str] = None  # Not all schedules will have a Redis Job yet

	@staticmethod
	async def init_from_task_key(task_key: str) -> object:
		task_data: dict = await get_task_schedule_by_id(task_key)  # read from the SQL Database
		return BtuTaskSchedule(
			id=task_data["name"],
			task=task_data["task"],
			task_description=task_data["task_description"],
			enabled=task_data["enabled"],
			queue_name=task_data["queue_name"],
			argument_overrides=task_data["argument_overrides"],
			schedule_description=task_data["schedule_description"],
			cron_string=task_data["cron_string"],
			cron_timezone=task_data["cron_timezone"],
		)

	def to_rq_job(self):
		rq_job = RQJob()  # RQJob::new_with_defaults();
		rq_job.description = self.task_description
		rq_job.origin = self.queue_name
		byte_result = get_pickled_function_from_web(self.task, self.id)
		whatis(byte_result)
		# return Err::<RQJob, anyhow::Error>(anyhow_macro!("Error while requesting pickled Python function:\n{}", error_message));
		return rq_job

	def get_next_runtimes(self):
		pass
