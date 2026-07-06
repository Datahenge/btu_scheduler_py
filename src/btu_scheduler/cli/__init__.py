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
	import redis

	from btu_scheduler.lib.scheduler import clear_all_scheduled_tasks

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
	List Schedule IDs already in the scheduler queue.
	"""
	import redis

	from btu_scheduler.lib.scheduler import rq_print_scheduled_tasks

	try:
		rq_print_scheduled_tasks()
	except redis.exceptions.ConnectionError as ex:
		raise click.ClickException(f"Cannot connect to Redis: {ex}") from ex
	except redis.exceptions.AuthenticationError as ex:
		raise click.ClickException(f"Redis authentication failed: {ex}") from ex


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
	"""
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

	click.echo(f"\n{passed}/{passed + failed} tests passed.")
	if failed:
		raise SystemExit(1)
