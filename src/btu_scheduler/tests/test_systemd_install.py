"""
Unit tests for lib/systemd_install.py's pure logic: collecting, validating, and
rendering settings for `btu install-systemd`. No root, subprocess, or filesystem access
required — subprocess-touching functions (create_service_user, enable_service) are
exercised by hand on a real VPS, not here.
"""

import unittest

from btu_scheduler.lib.systemd_install import (
	InstallError,
	collect_settings,
	find_btu_executable,
	render_env_file,
	render_unit_file,
	validate_settings,
)

DIRECT_ENV = {
	"BTU_SCHEDULER_WEBSERVER_IP": "erp.example.com",
	"BTU_SCHEDULER_WEBSERVER_PORT": "443",
	"BTU_SCHEDULER_WEBSERVER_TOKEN": "token abc:def",
	"BTU_SCHEDULER_SQL_DATABASE": "frappe",
	"BTU_SCHEDULER_SQL_USER": "frappe",
	"BTU_SCHEDULER_SQL_PASSWORD": "secret",
}

WEBSERVER_ENV = {
	"BTU_SCHEDULER_WEBSERVER_IP": "erp.example.com",
	"BTU_SCHEDULER_WEBSERVER_PORT": "443",
	"BTU_SCHEDULER_WEBSERVER_TOKEN": "token abc:def",
}


def _no_prompt(prompt_text, default, is_secret):
	raise AssertionError(f"prompter should not be called in non-interactive tests (asked: {prompt_text})")


class TestCollectSettingsNonInteractive(unittest.TestCase):
	def test_direct_mode_uses_env_values(self):
		values = collect_settings("direct", env=DIRECT_ENV, non_interactive=True, prompter=_no_prompt)
		self.assertEqual(values["sql_database"], "frappe")
		self.assertEqual(values["webserver_ip"], "erp.example.com")

	def test_direct_mode_fills_defaults_for_unset_optional_fields(self):
		values = collect_settings("direct", env=DIRECT_ENV, non_interactive=True, prompter=_no_prompt)
		self.assertEqual(values["sql_host"], "127.0.0.1")  # has a default, not in DIRECT_ENV
		self.assertEqual(values["rq_password"], "")  # optional, blank default — must not error

	def test_direct_mode_missing_required_raises(self):
		env = {k: v for k, v in DIRECT_ENV.items() if k != "BTU_SCHEDULER_SQL_DATABASE"}
		with self.assertRaises(InstallError):
			collect_settings("direct", env=env, non_interactive=True, prompter=_no_prompt)

	def test_webserver_mode_does_not_require_sql_fields(self):
		values = collect_settings("webserver", env=WEBSERVER_ENV, non_interactive=True, prompter=_no_prompt)
		self.assertNotIn("sql_host", values)
		self.assertNotIn("sql_password", values)

	def test_webserver_mode_missing_webserver_ip_raises(self):
		env = {k: v for k, v in WEBSERVER_ENV.items() if k != "BTU_SCHEDULER_WEBSERVER_IP"}
		with self.assertRaises(InstallError):
			collect_settings("webserver", env=env, non_interactive=True, prompter=_no_prompt)


class TestCollectSettingsInteractive(unittest.TestCase):
	def test_does_not_prompt_for_values_already_in_env(self):
		# Interactive mode always prompts for anything not covered by `env` (even fields
		# with a default — the default is just pre-filled, not auto-accepted), but must
		# never re-prompt for a field WEBSERVER_ENV already supplies.
		asked = []

		def prompter(prompt_text, default, is_secret):
			asked.append(prompt_text)
			return default or "prompted-value"

		collect_settings("webserver", env=WEBSERVER_ENV, non_interactive=False, prompter=prompter)
		self.assertNotIn("Frappe web server host or public DNS name", asked)  # WEBSERVER_IP is in WEBSERVER_ENV
		self.assertNotIn(
			"Frappe API token (format: token api_key:api_secret)", asked
		)  # WEBSERVER_TOKEN is in WEBSERVER_ENV

	def test_prompts_for_truly_missing_value(self):
		env = {k: v for k, v in WEBSERVER_ENV.items() if k != "BTU_SCHEDULER_WEBSERVER_TOKEN"}

		def prompter(prompt_text, default, is_secret):
			return "token typed:bysomeone"

		values = collect_settings("webserver", env=env, non_interactive=False, prompter=prompter)
		self.assertEqual(values["webserver_token"], "token typed:bysomeone")


class TestValidateSettings(unittest.TestCase):
	def test_valid_direct_values_build_settings(self):
		values = collect_settings("direct", env=DIRECT_ENV, non_interactive=True, prompter=_no_prompt)
		settings = validate_settings(values)
		self.assertEqual(settings.connectivity_mode, "direct")

	def test_invalid_sql_type_raises_install_error(self):
		values = collect_settings("direct", env=DIRECT_ENV, non_interactive=True, prompter=_no_prompt)
		values["sql_type"] = "oracle"
		with self.assertRaises(InstallError):
			validate_settings(values)


class TestRenderEnvFile(unittest.TestCase):
	def test_webserver_mode_omits_sql_lines(self):
		values = collect_settings("webserver", env=WEBSERVER_ENV, non_interactive=True, prompter=_no_prompt)
		settings = validate_settings(values)
		rendered = render_env_file(settings)
		self.assertNotIn("SQL_", rendered)
		self.assertIn("BTU_SCHEDULER_CONNECTIVITY_MODE=webserver", rendered)

	def test_direct_mode_includes_sql_lines(self):
		values = collect_settings("direct", env=DIRECT_ENV, non_interactive=True, prompter=_no_prompt)
		settings = validate_settings(values)
		rendered = render_env_file(settings)
		self.assertIn("BTU_SCHEDULER_SQL_DATABASE=frappe", rendered)

	def test_token_with_space_is_not_quoted(self):
		# EnvironmentFile format is plain KEY=VALUE — no shell quoting even for spaces.
		values = collect_settings("webserver", env=WEBSERVER_ENV, non_interactive=True, prompter=_no_prompt)
		settings = validate_settings(values)
		rendered = render_env_file(settings)
		self.assertIn("BTU_SCHEDULER_WEBSERVER_TOKEN=token abc:def", rendered)
		self.assertNotIn('"token abc:def"', rendered)

	def test_blank_optional_fields_are_omitted(self):
		values = collect_settings("direct", env=DIRECT_ENV, non_interactive=True, prompter=_no_prompt)
		settings = validate_settings(values)
		rendered = render_env_file(settings)
		self.assertNotIn("RQ_PASSWORD", rendered)
		self.assertNotIn("WEBSERVER_HOST_HEADER", rendered)

	def test_password_with_embedded_quote_is_quoted_and_escaped(self):
		env = dict(DIRECT_ENV)
		env["BTU_SCHEDULER_SQL_PASSWORD"] = 'pass"word'
		values = collect_settings("direct", env=env, non_interactive=True, prompter=_no_prompt)
		settings = validate_settings(values)
		rendered = render_env_file(settings)
		self.assertIn('BTU_SCHEDULER_SQL_PASSWORD="pass\\"word"', rendered)

	def test_password_with_leading_whitespace_is_quoted(self):
		env = dict(DIRECT_ENV)
		env["BTU_SCHEDULER_SQL_PASSWORD"] = " leadingspace"
		values = collect_settings("direct", env=env, non_interactive=True, prompter=_no_prompt)
		settings = validate_settings(values)
		rendered = render_env_file(settings)
		self.assertIn('BTU_SCHEDULER_SQL_PASSWORD=" leadingspace"', rendered)


class TestFindBtuExecutable(unittest.TestCase):
	"""
	A bootstrap script may invoke `btu` by absolute path (e.g.
	/opt/btu-scheduler/.venv/bin/btu install-systemd) rather than via PATH lookup —
	find_btu_executable() must prefer sys.argv[0] in that case, not just shutil.which,
	or ExecStart= could end up pointing at the wrong (or no) installation.
	"""

	def test_prefers_argv0_when_it_is_a_real_file(self):
		import pathlib
		import sys
		from unittest.mock import patch

		# find_btu_executable() returns a .resolve()'d path, which can differ from the raw
		# __file__ string if any ancestor directory is a symlink — compare resolved to resolved.
		with patch.object(sys, "argv", [__file__]):
			self.assertEqual(find_btu_executable(), str(pathlib.Path(__file__).resolve()))

	def test_falls_back_to_which_when_argv0_is_not_a_file(self):
		import sys
		from unittest.mock import patch

		with patch.object(sys, "argv", ["btu"]), patch("shutil.which", return_value="/usr/local/bin/btu"):
			self.assertEqual(find_btu_executable(), "/usr/local/bin/btu")

	def test_raises_install_error_when_nothing_found(self):
		import sys
		from unittest.mock import patch

		with patch.object(sys, "argv", ["btu"]), patch("shutil.which", return_value=None):
			with self.assertRaises(InstallError):
				find_btu_executable()


class TestRenderUnitFile(unittest.TestCase):
	def test_contains_exec_start_with_run_daemon(self):
		rendered = render_unit_file(
			service_user="btu-scheduler",
			exec_path="/opt/btu-scheduler/.venv/bin/btu",
			env_file="/etc/btu-scheduler/btu-scheduler.env",
			connectivity_mode="direct",
		)
		self.assertIn("ExecStart=/opt/btu-scheduler/.venv/bin/btu run-daemon", rendered)

	def test_contains_environment_file_directive(self):
		rendered = render_unit_file(
			service_user="btu-scheduler",
			exec_path="/usr/local/bin/btu",
			env_file="/etc/btu-scheduler/btu-scheduler.env",
			connectivity_mode="direct",
		)
		self.assertIn("EnvironmentFile=/etc/btu-scheduler/btu-scheduler.env", rendered)

	def test_user_and_group_match_service_user(self):
		rendered = render_unit_file(
			service_user="custom-user", exec_path="/usr/local/bin/btu", env_file="/etc/x.env", connectivity_mode="direct"
		)
		self.assertIn("User=custom-user", rendered)
		self.assertIn("Group=custom-user", rendered)

	def test_direct_mode_depends_on_mariadb_and_redis_by_default(self):
		rendered = render_unit_file(
			service_user="btu-scheduler", exec_path="/usr/local/bin/btu", env_file="/etc/x.env", connectivity_mode="direct"
		)
		self.assertIn("mariadb.service", rendered)
		self.assertIn("redis-server.service", rendered)

	def test_direct_mode_depends_on_postgresql_when_sql_type_is_postgres(self):
		rendered = render_unit_file(
			service_user="btu-scheduler",
			exec_path="/usr/local/bin/btu",
			env_file="/etc/x.env",
			connectivity_mode="direct",
			sql_type="postgres",
		)
		self.assertIn("postgresql.service", rendered)
		self.assertNotIn("mariadb.service", rendered)

	def test_webserver_mode_does_not_depend_on_mariadb_or_redis(self):
		rendered = render_unit_file(
			service_user="btu-scheduler", exec_path="/usr/local/bin/btu", env_file="/etc/x.env", connectivity_mode="webserver"
		)
		self.assertNotIn("mariadb.service", rendered)
		self.assertNotIn("postgresql.service", rendered)
		self.assertNotIn("redis-server.service", rendered)
