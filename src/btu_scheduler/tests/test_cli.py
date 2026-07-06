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

	def test_help_lists_redis_choice(self):
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertIn("redis", result.output)

	def test_help_lists_sql_choice(self):
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertIn("sql", result.output)

	def test_help_does_not_list_tcp_choices(self):
		result = CliRunner().invoke(entry_point, ["test", "--help"])
		self.assertNotIn("tcp-", result.output)

	def test_invalid_choice_exits_nonzero(self):
		result = CliRunner().invoke(entry_point, ["test", "tcp-echo"])
		self.assertNotEqual(result.exit_code, 0)


if __name__ == "__main__":
	unittest.main()
