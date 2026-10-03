"""
Unit tests for SchedulerSettings.connectivity_mode validation.

Constructs SchedulerSettings directly with keyword arguments, bypassing env/dotenv
loading, so these tests do not depend on any process environment.
"""

import unittest

from pydantic import ValidationError

from btu_scheduler.lib.config import SchedulerSettings

DIRECT_MODE_FIELDS = {
	"full_refresh_internal_secs": 30,
	"scheduler_polling_interval": 30,
	"sql_type": "postgres",
	"sql_host": "127.0.0.1",
	"sql_port": 5432,
	"sql_database": "frappe",
	"sql_user": "frappe",
	"sql_password": "secret",
	"rq_host": "127.0.0.1",
	"rq_port": 11000,
	"webserver_ip": "127.0.0.1",
	"webserver_port": 8000,
	"webserver_token": "token 1:2",
}

WEBSERVER_MODE_FIELDS = {
	"full_refresh_internal_secs": 30,
	"scheduler_polling_interval": 30,
	"webserver_ip": "erp.example.com",
	"webserver_port": 443,
	"webserver_token": "token 1:2",
}


class TestConnectivityModeDefault(unittest.TestCase):
	def test_defaults_to_direct(self):
		settings = SchedulerSettings(**DIRECT_MODE_FIELDS)
		self.assertEqual(settings.connectivity_mode, "direct")


class TestDirectModeRequiresSqlAndRedis(unittest.TestCase):
	def test_all_fields_present_succeeds(self):
		settings = SchedulerSettings(connectivity_mode="direct", **DIRECT_MODE_FIELDS)
		self.assertEqual(settings.sql_host, "127.0.0.1")

	def test_missing_sql_host_raises(self):
		fields = {**DIRECT_MODE_FIELDS, "sql_host": None}
		with self.assertRaises(ValidationError):
			SchedulerSettings(connectivity_mode="direct", **fields)

	def test_missing_rq_host_raises(self):
		fields = {**DIRECT_MODE_FIELDS, "rq_host": None}
		with self.assertRaises(ValidationError):
			SchedulerSettings(connectivity_mode="direct", **fields)

	def test_error_message_names_env_var(self):
		fields = {**DIRECT_MODE_FIELDS, "sql_host": None}
		with self.assertRaises(ValidationError) as ctx:
			SchedulerSettings(connectivity_mode="direct", **fields)
		self.assertIn("BTU_SCHEDULER_SQL_HOST", str(ctx.exception))


class TestWebserverModeDoesNotRequireSqlOrRedis(unittest.TestCase):
	def test_no_sql_or_redis_fields_succeeds(self):
		settings = SchedulerSettings(connectivity_mode="webserver", **WEBSERVER_MODE_FIELDS)
		self.assertEqual(settings.connectivity_mode, "webserver")
		self.assertIsNone(settings.sql_host)
		self.assertIsNone(settings.rq_host)

	def test_webserver_still_requires_webserver_fields(self):
		fields = dict(WEBSERVER_MODE_FIELDS)
		del fields["webserver_ip"]
		with self.assertRaises(ValidationError):
			SchedulerSettings(connectivity_mode="webserver", **fields)
