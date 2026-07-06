"""
Unit tests for BtuTask and BtuTaskSchedule dataclass construction and helpers.

The init_from_*_key() factory methods require SQL and are not tested here.
Direct construction and get_next_runtimes() require no running services.
"""

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from btu_scheduler.lib.structs import BtuTask, BtuTaskSchedule

UTC = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")


def _task(**overrides) -> BtuTask:
	defaults = dict(
		task_key="TASK-001",
		desc_short="Short description",
		desc_long="A longer description for testing purposes",
		arguments=None,
		path_to_function="btu.manual_tests.ping_now",
		max_task_duration=60,
	)
	defaults.update(overrides)
	return BtuTask(**defaults)


def _schedule(**overrides) -> BtuTaskSchedule:
	defaults = dict(
		id="TS-000001",
		task_key="TASK-001",
		task_description="Test Task",
		enabled=True,
		queue_name="default",
		argument_overrides=None,
		schedule_description="Every midnight UTC",
		cron_string="0 0 * * *",
		cron_timezone=UTC,
	)
	defaults.update(overrides)
	return BtuTaskSchedule(**defaults)


class TestBtuTaskConstruction(unittest.TestCase):

	def test_fields_are_stored(self):
		task = _task()
		self.assertEqual(task.task_key, "TASK-001")
		self.assertEqual(task.path_to_function, "btu.manual_tests.ping_now")
		self.assertEqual(task.max_task_duration, 60)

	def test_arguments_accepts_none(self):
		self.assertIsNone(_task(arguments=None).arguments)

	def test_arguments_accepts_json_string(self):
		task = _task(arguments='{"n": 3}')
		self.assertEqual(task.arguments, '{"n": 3}')


class TestBtuTaskScheduleConstruction(unittest.TestCase):

	def test_fields_are_stored(self):
		s = _schedule()
		self.assertEqual(s.id, "TS-000001")
		self.assertEqual(s.task_key, "TASK-001")
		self.assertTrue(s.enabled)
		self.assertEqual(s.queue_name, "default")

	def test_redis_job_id_defaults_to_none(self):
		self.assertIsNone(_schedule().redis_job_id)

	def test_redis_job_id_accepts_value(self):
		s = _schedule(redis_job_id="rq-abc-123")
		self.assertEqual(s.redis_job_id, "rq-abc-123")

	def test_disabled_schedule_is_valid(self):
		self.assertFalse(_schedule(enabled=False).enabled)

	def test_argument_overrides_accepts_none(self):
		self.assertIsNone(_schedule(argument_overrides=None).argument_overrides)

	def test_cron_timezone_stored_as_zoneinfo(self):
		s = _schedule(cron_timezone=ZoneInfo("America/Los_Angeles"))
		self.assertEqual(str(s.cron_timezone), "America/Los_Angeles")


class TestBtuTaskScheduleGetNextRuntimes(unittest.TestCase):
	"""
	get_next_runtimes() delegates to btu_cron.tz_cron_to_utc_datetimes.
	These tests confirm the delegation is wired correctly and the result is sane.
	"""

	START = datetime(2026, 1, 15, 13, 1, 0, tzinfo=UTC)

	def test_returns_one_result_by_default(self):
		s = _schedule(cron_string="0 18 * * *", cron_timezone=EASTERN)
		results = s.get_next_runtimes(from_utc_datetime=self.START)
		self.assertEqual(len(results), 1)

	def test_result_is_strictly_after_start(self):
		s = _schedule(cron_string="0 18 * * *", cron_timezone=EASTERN)
		result = s.get_next_runtimes(from_utc_datetime=self.START)[0]
		self.assertGreater(result, self.START)

	def test_result_is_utc_aware(self):
		s = _schedule(cron_string="0 18 * * *", cron_timezone=EASTERN)
		result = s.get_next_runtimes(from_utc_datetime=self.START)[0]
		self.assertEqual(result.utcoffset(), timedelta(0))

	def test_number_results_parameter(self):
		s = _schedule(cron_string="0 18 * * *", cron_timezone=EASTERN)
		results = s.get_next_runtimes(from_utc_datetime=self.START, number_results=3)
		self.assertEqual(len(results), 3)

	def test_results_are_in_ascending_order(self):
		s = _schedule(cron_string="0 18 * * *", cron_timezone=EASTERN)
		results = s.get_next_runtimes(from_utc_datetime=self.START, number_results=3)
		self.assertLess(results[0], results[1])
		self.assertLess(results[1], results[2])


if __name__ == "__main__":
	unittest.main()
