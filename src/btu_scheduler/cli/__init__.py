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


@entry_point.command("clear-scheduled-tasks")
def cli_clear_scheduled_tasks():
	"""
	Clear all scheduled tasks from the Redis database.
	"""
	from btu_scheduler.lib.scheduler import clear_all_scheduled_tasks

	if clear_all_scheduled_tasks():
		click.echo("All scheduled tasks cleared from Redis database.")
	else:
		click.echo("Error: Unable to clear scheduled tasks from Redis database.")


@entry_point.command("list-scheduled-tasks")
def cli_list_scheduled_tasks():
	"""
	List Schedule IDs already in the scheduler queue.
	"""
	from btu_scheduler.lib.scheduler import rq_print_scheduled_tasks

	rq_print_scheduled_tasks()


@entry_point.command("run-daemon")
def cli_run_daemon():
	"""
	Run the BTU scheduler daemon.
	"""
	from btu_scheduler.daemon import main

	asyncio.run(main())


test_choices: list = [
	"frappe-ping",
	"pickler",
	"redis",
	"sql",
	"test-rq-hello-world",
]


@entry_point.command("test")
@click.argument("command", type=click.Choice(test_choices, case_sensitive=False))
def cli_test(command):
	"""
	Run a diagnostic test.
	"""
	match command:
		case "frappe-ping":
			import requests

			from btu_scheduler.lib.diagnostics import diagnose_frappe_ping

			try:
				diagnose_frappe_ping()
			except requests.exceptions.ConnectionError as ex:
				click.echo(ex)

		case "pickler":
			from btu_scheduler.lib.diagnostics import diagnose_pickler

			diagnose_pickler()

		case "redis":
			from btu_scheduler.lib.diagnostics import diagnose_redis

			try:
				diagnose_redis()
				click.echo("Redis connection successful.")
			except Exception as ex:
				click.echo(f"Error: {ex}")

		case "sql":
			from btu_scheduler.lib.diagnostics import diagnose_sql

			asyncio.run(diagnose_sql(quiet=False))

		case "test-rq-hello-world":
			from btu_scheduler.lib.diagnostics import diagnose_rq_hello_world

			diagnose_rq_hello_world()
