"""btu_scheduler/cli.py"""

# Standard Library
import asyncio
import os
import pprint
import shlex
import subprocess

# Third Party
import click

from btu_scheduler import __version__
from btu_scheduler._vendor.config_logging import ConfigurationError

_SECRET_FIELDS = frozenset({"sql_password", "webserver_token", "rq_password"})


def _redacted_config_dict(settings) -> dict:
	data = settings.model_dump(mode="json")
	for key in _SECRET_FIELDS:
		if data.get(key):
			data[key] = "***"
	return data


def _show_config(settings) -> None:
	click.echo()
	click.echo(pprint.pformat(_redacted_config_dict(settings), indent=4, compact=False))
	click.echo()


def _require_config(load_fn):
	"""
	Call load_fn() and return the result. On failure, print a human-readable
	message listing the missing BTU_SCHEDULER_* environment variables and exit.
	"""
	try:
		return load_fn()
	except ConfigurationError as exc:
		raise click.ClickException(str(exc)) from exc


# ========
# Click Group and the starting point for the CLI
# ========
@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__)
def entry_point():
	"""
	CLI interface for BTU Scheduler
	"""


# ========
# Click commands begin here.
# ========


@entry_point.command("about")
def cmd_about():
	"""
	About the btu-scheduler application.
	"""
	click.echo(f"btu-scheduler version {__version__}")
	click.echo("Copyright (C) 2025-2026")
	click.echo("BTU Scheduler is a background daemon for creating RQ Jobs from BTU Task Schedules.")


@entry_point.command("config")
@click.argument("command", type=click.Choice(["show", "edit", "path"], case_sensitive=False))
def cmd_config(command):
	"""
	Configuration of btu-scheduler CLI.
	"""
	from btu_scheduler.lib.config import get_env_file_path, load_config

	match command.split():
		case ["show"]:
			_show_config(_require_config(load_config))
		case ["path"]:
			click.echo(get_env_file_path())
		case ["edit"]:
			env_path = get_env_file_path()
			env_path.parent.mkdir(parents=True, exist_ok=True)
			if not env_path.exists():
				env_path.touch()
			editor = shlex.split(os.environ.get("EDITOR", "/usr/bin/editor"))
			subprocess.run([*editor, str(env_path)], check=False)
		case _:
			raise click.ClickException(f"Subcommand '{command}' not recognized.")


def _require_direct_mode(action: str) -> None:
	"""
	Several CLI commands inspect/mutate the scheduled-task store directly. In
	connectivity_mode=direct that store is Redis — shared with the running daemon, so a
	separate CLI invocation can see and change it. In connectivity_mode=webserver it's
	in-process memory inside the daemon (see lib/scheduled_store.py); a CLI invocation
	has its own, unrelated instance, so these commands would silently no-op or report
	nothing without this guard.
	"""
	from btu_scheduler.lib.config import load_config

	settings = _require_config(load_config)
	if settings.connectivity_mode != "direct":
		raise click.ClickException(
			f"'{action}' requires connectivity_mode=direct. In connectivity_mode=webserver, the "
			"scheduled-task store is in-process memory inside the running daemon — a separate CLI "
			"invocation cannot see or change it. Use 'btu test' to check connectivity, or check the "
			"daemon's logs (e.g. 'journalctl -u btu-scheduler') for its current state."
		)


@entry_point.command("clear-scheduled-tasks")
def cli_clear_scheduled_tasks():
	"""
	Clear all scheduled tasks from the Redis database. Requires connectivity_mode=direct.
	"""
	import redis

	from btu_scheduler.lib.scheduler import clear_all_scheduled_tasks

	_require_direct_mode("clear-scheduled-tasks")

	try:
		clear_all_scheduled_tasks()
	except redis.exceptions.ConnectionError as ex:
		raise click.ClickException(f"Cannot connect to Redis: {ex}") from ex
	except redis.exceptions.AuthenticationError as ex:
		raise click.ClickException(f"Redis authentication failed: {ex}") from ex
	click.echo("All scheduled tasks cleared from Redis database.")


@entry_point.command("list-scheduled-tasks")
def cli_list_scheduled_tasks():
	"""
	List Schedule IDs already in the scheduler queue. Requires connectivity_mode=direct.
	"""
	import redis

	from btu_scheduler.lib.scheduler import rq_print_scheduled_tasks

	_require_direct_mode("list-scheduled-tasks")

	try:
		rq_print_scheduled_tasks()
	except redis.exceptions.ConnectionError as ex:
		raise click.ClickException(f"Cannot connect to Redis: {ex}") from ex
	except redis.exceptions.AuthenticationError as ex:
		raise click.ClickException(f"Redis authentication failed: {ex}") from ex


@entry_point.command("install-systemd")
@click.option(
	"--mode",
	"connectivity_mode",
	type=click.Choice(["direct", "webserver"]),
	default="direct",
	show_default=True,
	help="connectivity_mode to write into the environment file.",
)
@click.option(
	"--service-user",
	default="btu-scheduler",
	show_default=True,
	help="System account the daemon runs as; created automatically if missing.",
)
@click.option(
	"--env-file",
	default="/etc/btu-scheduler/btu-scheduler.env",
	show_default=True,
)
@click.option(
	"--unit-file",
	default=None,
	help="Path to write the systemd unit file. Defaults to /etc/systemd/system/<service-name>.service.",
)
@click.option("--service-name", default="btu-scheduler", show_default=True)
@click.option(
	"--non-interactive",
	is_flag=True,
	default=False,
	help="Fail instead of prompting; every required BTU_SCHEDULER_* value must already be set.",
)
@click.option("--enable/--no-enable", default=True, help="Run systemctl daemon-reload + enable --now afterward.")
@click.option("--create-user/--no-create-user", default=True, help="Create --service-user if it doesn't exist.")
@click.option("--force", is_flag=True, default=False, help="Overwrite an existing env/unit file without asking.")
@click.option(
	"--dry-run",
	is_flag=True,
	default=False,
	help="Print the env file and unit file to stdout instead of writing/enabling anything. No root required.",
)
def cli_install_systemd(
	connectivity_mode,
	service_user,
	env_file,
	unit_file,
	service_name,
	non_interactive,
	enable,
	create_user,
	force,
	dry_run,
):
	"""
	Generate the EnvironmentFile and systemd unit for running the daemon as a service.

	Must be run as root (except --dry-run). Prompts for any required BTU_SCHEDULER_*
	value not already present in the environment (pass --non-interactive to require them
	all up front). Collected values are validated the same way the daemon itself
	validates them before anything is written to disk.
	"""
	import os
	import pathlib

	from btu_scheduler.lib import systemd_install as si

	if not dry_run and os.geteuid() != 0:
		raise click.ClickException(
			"install-systemd must be run as root (it creates a system user, writes to "
			"/etc, and installs a systemd unit). Re-run with sudo, or pass --dry-run to preview "
			"the generated files without needing root."
		)

	if unit_file is None:
		# Keep the unit file's path in sync with --service-name by default, so that
		# `systemctl enable --now <service-name>` below finds the file this command just wrote.
		unit_file = f"/etc/systemd/system/{service_name}.service"

	env_path = pathlib.Path(env_file)
	unit_path = pathlib.Path(unit_file)

	if not dry_run and not force:
		for path in (env_path, unit_path):
			if path.exists() and not click.confirm(f"{path} already exists. Overwrite?", default=False):
				raise click.ClickException(f"Aborted: {path} already exists. Pass --force to overwrite without asking.")

	def prompter(prompt_text: str, default: str | None, is_secret: bool) -> str:
		# default=None (no default available) makes click.prompt keep re-asking until the
		# user types something non-blank, instead of silently accepting an empty Enter-press.
		reply = click.prompt(prompt_text, default=default, hide_input=is_secret, show_default=bool(default))
		return reply

	try:
		raw_values = si.collect_settings(
			connectivity_mode, env=dict(os.environ), non_interactive=non_interactive, prompter=prompter
		)
		settings = si.validate_settings(raw_values)
		exec_path = si.find_btu_executable()
		unit_content = si.render_unit_file(
			service_user=service_user,
			exec_path=exec_path,
			env_file=str(env_path),
			connectivity_mode=settings.connectivity_mode,
			sql_type=settings.sql_type,
		)
		env_content = si.render_env_file(settings)

		if dry_run:
			click.echo(f"# {env_path}\n{env_content}")
			click.echo(f"# {unit_path}\n{unit_content}")
			click.echo("Dry run only — nothing was written, no user created, nothing enabled.")
			return

		if create_user and not si.user_exists(service_user):
			click.echo(f"Creating system user '{service_user}'...")
			si.create_service_user(service_user)

		env_path.parent.mkdir(parents=True, exist_ok=True)
		env_path.write_text(env_content)
		env_path.chmod(0o600)
		try:
			import grp
			import pwd as pwd_module

			uid = pwd_module.getpwnam(service_user).pw_uid
			gid = grp.getgrnam(service_user).gr_gid
			os.chown(env_path, uid, gid)
		except KeyError:
			click.echo(f"Warning: could not resolve uid/gid for '{service_user}'; leaving {env_path} owned by root.")

		unit_path.write_text(unit_content)

		click.echo(f"Wrote {env_path} (mode 600, owned by {service_user}).")
		click.echo(f"Wrote {unit_path}.")

		if enable:
			si.enable_service(service_name)
			click.echo(f"Enabled and started {service_name}.service.")
			click.echo(f"Check status with: systemctl status {service_name}")
			click.echo(f"Follow logs with:  journalctl -u {service_name} -f")
		else:
			click.echo("Skipped enabling the service (--no-enable). Run manually with:")
			click.echo("  systemctl daemon-reload && systemctl enable --now " + service_name)

	except si.InstallError as ex:
		raise click.ClickException(str(ex)) from ex
	except subprocess.CalledProcessError as ex:
		raise click.ClickException(
			f"Command {ex.cmd} failed with exit code {ex.returncode}. The env/unit files above may "
			"already have been written to disk even though enabling the service failed."
		) from ex


@entry_point.command("run-daemon")
def cli_run_daemon():
	"""
	Run the BTU scheduler daemon.
	"""
	from btu_scheduler.daemon import main

	asyncio.run(main())


@entry_point.command("test")
def cli_test():
	"""
	Run all diagnostic tests sequentially: Redis, SQL, Frappe HTTP, pickler, RQ hello-world.

	Redis/SQL/RQ checks are skipped when BTU_SCHEDULER_CONNECTIVITY_MODE=webserver,
	since that mode never connects to them directly.
	"""
	from btu_scheduler.lib.config import load_config
	from btu_scheduler.lib.diagnostics import (
		diagnose_frappe_ping,
		diagnose_pickler,
		diagnose_redis,
		diagnose_redis_version,
		diagnose_rq_hello_world,
		diagnose_rq_workers,
		diagnose_sql,
	)

	passed = 0
	failed = 0
	connectivity_mode = _require_config(load_config).connectivity_mode

	if connectivity_mode == "direct":
		click.echo("\n--- Redis ---")
		try:
			diagnose_redis()
			click.echo("Connection OK.")
			passed += 1
		except Exception as ex:
			click.echo(f"FAILED: {ex}")
			failed += 1

		click.echo("\n--- Redis Version ---")
		try:
			diagnose_redis_version()
			passed += 1
		except Exception as ex:
			click.echo(f"FAILED: {ex}")
			failed += 1

		click.echo("\n--- SQL ---")
		try:
			asyncio.run(diagnose_sql())
			passed += 1
		except Exception as ex:
			click.echo(f"FAILED: {ex}")
			failed += 1
	else:
		click.echo("\n--- Redis / SQL ---")
		click.echo("Skipped: connectivity_mode='webserver' does not use direct Redis or SQL access.")

	click.echo("\n--- Frappe HTTP ---")
	try:
		diagnose_frappe_ping()
		passed += 1
	except Exception as ex:
		click.echo(f"FAILED: {ex}")
		failed += 1

	click.echo("\n--- Pickler ---")
	try:
		diagnose_pickler()
		passed += 1
	except Exception as ex:
		click.echo(f"FAILED: {ex}")
		failed += 1

	if connectivity_mode == "direct":
		click.echo("\n--- RQ Hello World ---")
		try:
			diagnose_rq_hello_world()
			passed += 1
		except Exception as ex:
			click.echo(f"FAILED: {ex}")
			failed += 1

		click.echo("\n--- RQ Workers ---")
		try:
			diagnose_rq_workers()
			passed += 1
		except Exception as ex:
			click.echo(f"FAILED: {ex}")
			failed += 1
	else:
		click.echo("\n--- RQ Hello World / RQ Workers ---")
		click.echo("Skipped: connectivity_mode='webserver' does not use direct Redis access.")

	click.echo("\n--- Systemd ---")
	import shutil as _shutil
	import subprocess as _subprocess

	if not _shutil.which("systemctl"):
		click.echo(
			"Not applicable: systemctl not found on this host (not a systemd system, or running in a container)."
		)
	else:
		service_name = "btu-scheduler"
		for check in ("is-enabled", "is-active"):
			result = _subprocess.run(["systemctl", check, service_name], capture_output=True, text=True, check=False)
			click.echo(f"{check} {service_name}: {result.stdout.strip() or result.stderr.strip()}")

	click.echo(f"\n{passed}/{passed + failed} tests passed.")
	if failed:
		raise SystemExit(1)
