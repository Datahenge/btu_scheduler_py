"""btu_scheduler/lib/systemd_install.py

Generates the EnvironmentFile and systemd unit used to run the BTU Scheduler daemon
as a service, and (optionally) enables it. Backs the `btu install-systemd` CLI command.

Collected values are validated through the real SchedulerSettings model before anything
is written to disk, so a bad combination fails with the same error the daemon itself
would give — not a separate, possibly-inconsistent check.
"""

import pathlib
import pwd
import shutil
import subprocess
import sys
from dataclasses import dataclass

from pydantic import ValidationError

from btu_scheduler.lib.config import SchedulerSettings

DEFAULT_SERVICE_USER = "btu-scheduler"
DEFAULT_ENV_FILE = "/etc/btu-scheduler/btu-scheduler.env"
DEFAULT_UNIT_FILE = "/etc/systemd/system/btu-scheduler.service"
DEFAULT_SERVICE_NAME = "btu-scheduler"


@dataclass(frozen=True)
class FieldSpec:
	name: str  # SchedulerSettings field name
	prompt: str
	default: str | None
	is_secret: bool
	required: bool  # must end up non-empty
	direct_mode_only: bool  # only collected/written when connectivity_mode == "direct"


FIELD_SPECS: list[FieldSpec] = [
	FieldSpec("full_refresh_internal_secs", "Full refresh interval (seconds)", "30", False, True, False),
	FieldSpec("scheduler_polling_interval", "Scheduler polling interval (seconds)", "30", False, True, False),
	FieldSpec("log_level", "Log level", "INFO", False, True, False),
	FieldSpec("webserver_ip", "Frappe web server host or public DNS name", None, False, True, False),
	FieldSpec("webserver_port", "Frappe web server port", "8000", False, True, False),
	FieldSpec("webserver_token", "Frappe API token (format: token api_key:api_secret)", None, True, True, False),
	FieldSpec("webserver_host_header", "Host header (blank unless multi-tenant)", "", False, False, False),
	FieldSpec("sql_type", "SQL type (postgres or mariadb)", "mariadb", False, True, True),
	FieldSpec("sql_host", "SQL host", "127.0.0.1", False, True, True),
	FieldSpec("sql_port", "SQL port", "3306", False, True, True),
	FieldSpec("sql_database", "SQL database name", None, False, True, True),
	FieldSpec("sql_user", "SQL user", None, False, True, True),
	FieldSpec("sql_password", "SQL password", None, True, True, True),
	FieldSpec("rq_host", "Redis host", "127.0.0.1", False, True, True),
	FieldSpec("rq_port", "Redis port", "11000", False, True, True),
	FieldSpec("rq_password", "Redis password (blank if none)", "", True, False, True),
]


class InstallError(Exception):
	"""Raised for any install-systemd failure that should abort with a clean message."""


def collect_settings(
	connectivity_mode: str,
	*,
	env: dict[str, str],
	non_interactive: bool,
	prompter,
) -> dict[str, str]:
	"""
	Resolve every field BTU_SCHEDULER_* value needed for `connectivity_mode`.

	Precedence per field: `env` (an already-exported BTU_SCHEDULER_<FIELD> value) wins;
	otherwise prompt via `prompter(prompt_text, default, is_secret)` unless
	`non_interactive`, in which case a present default is used and a missing *required*
	value raises InstallError.
	"""
	values: dict[str, str] = {"connectivity_mode": connectivity_mode}

	for spec in FIELD_SPECS:
		if spec.direct_mode_only and connectivity_mode != "direct":
			continue

		env_key = f"BTU_SCHEDULER_{spec.name.upper()}"
		if env.get(env_key):
			values[spec.name] = env[env_key]
			continue

		if non_interactive:
			if spec.default is not None or not spec.required:
				values[spec.name] = spec.default or ""
				continue
			raise InstallError(
				f"Missing required value for {env_key}. Set it as an environment variable before "
				f"running with --non-interactive."
			)

		values[spec.name] = prompter(spec.prompt, spec.default, spec.is_secret)

	return values


def validate_settings(values: dict[str, str]) -> SchedulerSettings:
	"""Build a real SchedulerSettings from collected values, surfacing pydantic's own errors."""
	try:
		return SchedulerSettings(**values)
	except ValidationError as ex:
		raise InstallError(f"Collected settings failed validation:\n{ex}") from ex


def _quote_env_value(value: str) -> str:
	"""
	Quote `value` for a systemd EnvironmentFile if needed. systemd's parser (systemd >= 246)
	is shell-quote-aware: an unquoted value is taken as the literal rest of the line with
	outer whitespace stripped, but a value containing a quote or backslash character needs
	an explicit, escaped quoted string so it isn't misparsed (e.g. truncated at an unbalanced
	embedded "). Plain values are left bare, matching previous output exactly.
	"""
	if value == value.strip() and not any(c in value for c in "\"'\\"):
		return value
	escaped = value.replace("\\", "\\\\").replace('"', '\\"')
	return f'"{escaped}"'


def render_env_file(settings: SchedulerSettings) -> str:
	"""
	Render settings as a systemd EnvironmentFile: KEY=VALUE per line, quoting values that
	need it (see _quote_env_value). Only fields relevant to settings.connectivity_mode are
	included.
	"""
	lines = [f"BTU_SCHEDULER_CONNECTIVITY_MODE={settings.connectivity_mode}"]
	for spec in FIELD_SPECS:
		if spec.direct_mode_only and settings.connectivity_mode != "direct":
			continue
		value = getattr(settings, spec.name)
		if value in (None, ""):
			continue
		lines.append(f"BTU_SCHEDULER_{spec.name.upper()}={_quote_env_value(str(value))}")
	return "\n".join(lines) + "\n"


def render_unit_file(*, service_user: str, exec_path: str, env_file: str, connectivity_mode: str, sql_type: str | None = None) -> str:
	after_units = ["network-online.target"]
	if connectivity_mode == "direct":
		after_units.append("postgresql.service" if sql_type == "postgres" else "mariadb.service")
		after_units.append("redis-server.service")

	return f"""[Unit]
Description=BTU Scheduler daemon
After={" ".join(after_units)}
Wants=network-online.target

[Service]
Type=simple
User={service_user}
Group={service_user}
EnvironmentFile={env_file}
ExecStart={exec_path} run-daemon
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
"""


def find_btu_executable() -> str:
	"""
	Locate the `btu` executable to put in ExecStart=.

	Prefers sys.argv[0] — the script that's actually running right now — since a caller
	may have invoked it by absolute path (e.g. a bootstrap script doing
	`/opt/btu-scheduler/.venv/bin/btu install-systemd`) rather than via PATH lookup,
	which `shutil.which("btu")` alone would miss or could resolve to a different copy.
	"""
	argv0 = pathlib.Path(sys.argv[0]).resolve()
	if argv0.is_file():
		return str(argv0)

	exec_path = shutil.which("btu")
	if not exec_path:
		raise InstallError(
			"Could not find the 'btu' executable on PATH. Is btu-scheduler installed "
			"in the environment this command is running from?"
		)
	return exec_path


def user_exists(username: str) -> bool:
	try:
		pwd.getpwnam(username)
		return True
	except KeyError:
		return False


def create_service_user(username: str) -> None:
	subprocess.run(
		["useradd", "--system", "--no-create-home", "--shell", "/usr/sbin/nologin", username],
		check=True,
	)


def enable_service(service_name: str) -> None:
	if not shutil.which("systemctl"):
		raise InstallError("systemctl not found — this host does not appear to use systemd.")
	subprocess.run(["systemctl", "daemon-reload"], check=True)
	subprocess.run(["systemctl", "enable", "--now", service_name], check=True)
