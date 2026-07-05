"""btu_scheduler/cli.py"""

# Standard Library
import asyncio
import os
import subprocess
import sys

# Third Party
import click

# Package
import btu_scheduler
from btu_scheduler import __version__
from btu_scheduler._vendor.config_logging import ConfigurationError

VERBOSE_MODE = False


def _require_config(load_fn):
	"""
	Call load_fn() and return the result. On failure, print a human-readable
	message listing the missing BTU_SCHEDULER_* environment variables and exit.
	"""
	try:
		return load_fn()
	except ConfigurationError as exc:
		print(exc, file=sys.stderr)
		sys.exit(1)


# ========
# Click Group and the starting point for the CLI
# ========
@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__)
@click.option(
	"--verbose",
	"-vb",
	is_flag=True,
	default=False,
	help="Prefix to any command for verbosity.",
)
def entry_point(verbose):
	"""
	CLI interface for BTU: Python Edition
	"""
	if verbose:
		global VERBOSE_MODE
		VERBOSE_MODE = True
		click.echo(f"Verbose mode is {'on' if verbose else 'off'}.")


# ========
# Click commands begin here.
# ========


@entry_point.command("about")
def cmd_about():
	"""
	About the btu-scheduler application.
	"""
	print(f"btu-scheduler version {__version__}")
	print("Copyright (C) 2025")
	print("A Python-based alternative to the original BTU Scheduler.")


@entry_point.command("config")
@click.argument("command", type=click.Choice(["show", "edit", "path"], case_sensitive=False))
def cmd_config(command):
	"""
	Configuration of btu-scheduler CLI.
	"""
	from btu_scheduler.lib.config import get_env_file_path, load_config

	match command.split():
		case ["show"]:
			btu_scheduler.shared_config.set(_require_config(load_config))
			btu_scheduler.get_config().print_config()
		case ["path"]:
			print(get_env_file_path())
		case ["edit"]:
			env_path = get_env_file_path()
			env_path.parent.mkdir(parents=True, exist_ok=True)
			if not env_path.exists():
				env_path.touch()
			editor = os.environ.get("EDITOR", "/usr/bin/editor")
			os.system(f"{editor} {env_path}")
		case _:
			print(f"Subcommand '{command}' not recognized.")


@entry_point.command("clear-scheduled-tasks")
def cli_clear_scheduled_tasks():
	"""
	Clear all scheduled tasks from the Redis database.
	"""
	from btu_scheduler.lib.scheduler import clear_all_scheduled_tasks

	if clear_all_scheduled_tasks():
		print("All scheduled tasks cleared from Redis database.")
	else:
		print("Error: Unable to clear scheduled tasks from Redis database.")


@entry_point.command("list-scheduled-tasks")
def cli_list_scheduled_tasks():
	"""
	List Schedule IDs already in the scheduler queue.
	"""
	from btu_scheduler.lib.scheduler import rq_print_scheduled_tasks

	rq_print_scheduled_tasks(to_stdout=True)


@entry_point.command("run-daemon")
@click.option("--debug", is_flag=True, default=False, help="Throw exceptions to help debugging.")
def cli_run_daemon(debug):
	"""
	Run the BTU scheduler daemon.
	"""
	if debug:
		print("TODO: Change the logger to Debug Mode.")

	from btu_scheduler.daemon import main

	asyncio.run(main())


test_choices: list = [
	"frappe-ping",
	"pickler",
	"redis",
	"slack",
	"sql",
	"tcp-echo",
	"tcp-ping",
	"tcp-create-task-schedule",
	"tcp-cancel-task-schedule",
	"test-rq-hello-world",
]


@entry_point.command("test")
@click.argument("command", type=click.Choice(test_choices, case_sensitive=False))
@click.argument("task_schedule_id", required=False)
def cli_test(command, task_schedule_id):
	"""
	Run a test.

	For the TCP schedule-related tests, you may pass a Task Schedule ID as
	a second argument, for example:

	\b
	  btu test tcp-create-task-schedule TS-000123
	  btu test tcp-cancel-task-schedule TS-000123
	"""
	match command:
		case "frappe-ping":
			import requests

			from btu_scheduler.lib.tests import test_frappe_ping

			try:
				test_frappe_ping()
			except requests.exceptions.ConnectionError as ex:
				print(ex)

		case "pickler":
			from btu_scheduler.lib.tests import test_pickler

			test_pickler()

		case "redis":
			from btu_scheduler.lib.tests import test_redis

			try:
				test_redis()
				print("Redis connection successful.")
			except Exception as ex:
				print(f"Error: {ex}")

		case "slack":
			from btu_scheduler.lib.tests import test_slack

			test_slack()

		case "sql":
			from btu_scheduler.lib.tests import test_sql

			asyncio.run(test_sql(quiet=False))

		case "tcp-echo":
			from btu_scheduler.lib.tests import test_tcp_socket_echo

			test_tcp_socket_echo()
			print("TCP socket echo test completed.")

		case "tcp-ping":
			from btu_scheduler.lib.tests import test_tcp_socket_ping

			test_tcp_socket_ping()
			print("TCP socket ping test completed.")

		case "tcp-create-task-schedule":
			from btu_scheduler.lib.tests import test_tcp_socket_create_task_schedule

			if not task_schedule_id:
				print("Error: You must provide a Task Schedule ID, e.g. 'btu test tcp-create-task-schedule TS-000123'.")
				return
			test_tcp_socket_create_task_schedule(task_schedule_id)
			print("TCP socket create_task_schedule test completed.")

		case "tcp-cancel-task-schedule":
			from btu_scheduler.lib.tests import test_tcp_socket_cancel_task_schedule

			if not task_schedule_id:
				print("Error: You must provide a Task Schedule ID, e.g. 'btu test tcp-cancel-task-schedule TS-000123'.")
				return
			test_tcp_socket_cancel_task_schedule(task_schedule_id)
			print("TCP socket cancel_task_schedule test completed.")

		case "test-rq-hello-world":
			from btu_scheduler.lib.tests import test_rq_hello_world

			test_rq_hello_world()

		case _:
			test_choices_string = "\n    ".join(test_choices)
			print(f"Unhandled subcommand '{command}'.  Please choose one of:\n    {test_choices_string}\n")


@entry_point.command("service-status")
def cli_service_status():
	"""
	Check the status of various systemd services
	"""
	# Falcon
	command_list = ["sudo", "systemctl", "status", "btu_scheduler.service"]
	subprocess.run(command_list, check=False, stderr=subprocess.STDOUT)

	# Frappe Workers
	command_list = [
		"sudo",
		"systemctl",
		"status",
	]
	subprocess.run(command_list, check=False, stderr=subprocess.STDOUT)
