"""
Smoke tests for the btu-scheduler CLI using click.testing.CliRunner.

Tests here only exercise commands that require no running services
(Redis, SQL, Frappe web server).  Commands that require connectivity
are tested in integration tests elsewhere.
"""

import unittest

from click.testing import CliRunner

from btu_scheduler import __version__
from btu_scheduler.cli import entry_point


class TestAboutCommand(unittest.TestCase):
	def test_exits_zero(self):
		result = CliRunner().invoke(entry_point, ["about"])
		self.assertEqual(result.exit_code, 0)

	def test_outputs_version_string(self):
		result = CliRunner().invoke(entry_point, ["about"])
		self.assertIn(__version__, result.output)

	def test_mentions_btu_scheduler(self):
		result = CliRunner().invoke(entry_point, ["about"])
		self.assertIn("btu-scheduler", result.output)


class TestHelpAndVersion(unittest.TestCase):
	def test_root_help_exits_zero(self):
		result = CliRunner().invoke(entry_point, ["--help"])
		self.assertEqual(result.exit_code, 0)

	def test_root_help_mentions_run_daemon(self):
		result = CliRunner().invoke(entry_point, ["--help"])
		self.assertIn("run-daemon", result.output)

	def test_root_help_mentions_config(self):
		result = CliRunner().invoke(entry_point, ["--help"])
		self.assertIn("config", result.output)

	def test_version_flag_exits_zero(self):
		result = CliRunner().invoke(entry_point, ["--version"])
		self.assertEqual(result.exit_code, 0)

	def test_version_flag_outputs_version(self):
		result = CliRunner().invoke(entry_point, ["--version"])
		self.assertIn(__version__, result.output)


class TestConfigPathCommand(unittest.TestCase):
	"""config path resolves the .env file location without loading config."""

	def test_exits_zero(self):
		result = CliRunner().invoke(entry_point, ["config", "path"])
		self.assertEqual(result.exit_code, 0)

	def test_output_contains_btu_scheduler(self):
		result = CliRunner().invoke(entry_point, ["config", "path"])
		output = result.output.strip()
		self.assertTrue(
			"btu-scheduler" in output or "btu_scheduler" in output,
			msg=f"Expected a btu-scheduler config path, got: {output!r}",
		)

	def test_output_ends_with_env(self):
		result = CliRunner().invoke(entry_point, ["config", "path"])
		self.assertTrue(result.output.strip().endswith(".env"))


class TestTestSubcommand(unittest.TestCase):
	def test_help_exits_zero(self):
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertEqual(result.exit_code, 0)

	def test_help_mentions_redis(self):
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertIn("Redis", result.output)

	def test_help_mentions_sql(self):
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertIn("SQL", result.output)

	def test_help_does_not_list_tcp(self):
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertNotIn("tcp", result.output.lower())

	def test_takes_no_arguments(self):
		# The old command required a COMMAND argument; new command takes none.
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertNotIn("COMMAND", result.output)


class TestListScheduledTasks(unittest.TestCase):
	"""list-scheduled-tasks requires a live Redis connection; test only the no-Redis path."""

	def test_redis_unavailable_exits_nonzero(self):
		from unittest.mock import patch

		import redis

		with patch("btu_scheduler.lib.scheduled_store.create_connection") as mock_conn:
			mock_conn.return_value.zscan.side_effect = redis.exceptions.ConnectionError("Connection refused")
			result = CliRunner().invoke(entry_point, ["list-scheduled-tasks"])
		self.assertNotEqual(result.exit_code, 0)

	def test_redis_unavailable_shows_clean_message(self):
		from unittest.mock import patch

		import redis

		with patch("btu_scheduler.lib.scheduled_store.create_connection") as mock_conn:
			mock_conn.return_value.zscan.side_effect = redis.exceptions.ConnectionError("Connection refused")
			result = CliRunner().invoke(entry_point, ["list-scheduled-tasks"])
		self.assertIn("Cannot connect to Redis", result.output)
		self.assertNotIn("Traceback", result.output)


class TestConnectivityModeGuard(unittest.TestCase):
	"""
	clear-scheduled-tasks and list-scheduled-tasks inspect/mutate the scheduled-task
	store directly, which only makes sense in connectivity_mode=direct (Redis, shared
	with the running daemon) — not connectivity_mode=webserver (in-process memory inside
	the daemon, invisible to a separate CLI invocation). See _require_direct_mode().
	"""

	def _webserver_mode_settings(self):
		from types import SimpleNamespace

		return SimpleNamespace(connectivity_mode="webserver")

	def test_clear_scheduled_tasks_blocked_in_webserver_mode(self):
		from unittest.mock import patch

		with patch("btu_scheduler.lib.config.load_config", return_value=self._webserver_mode_settings()):
			result = CliRunner().invoke(entry_point, ["clear-scheduled-tasks"])
		self.assertNotEqual(result.exit_code, 0)
		self.assertIn("connectivity_mode=direct", result.output)

	def test_list_scheduled_tasks_blocked_in_webserver_mode(self):
		from unittest.mock import patch

		with patch("btu_scheduler.lib.config.load_config", return_value=self._webserver_mode_settings()):
			result = CliRunner().invoke(entry_point, ["list-scheduled-tasks"])
		self.assertNotEqual(result.exit_code, 0)
		self.assertIn("connectivity_mode=direct", result.output)


if __name__ == "__main__":
	unittest.main()
