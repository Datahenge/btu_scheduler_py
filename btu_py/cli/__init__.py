""" btu_py/cli.py """

# Standard Library
import asyncio
from getpass import getuser
import json
import logging
import os
import pathlib
import socket
import ssl
import subprocess
import sys

# Third Party
import click

# Package
import btu_py
from btu_py import __version__
from btu_py.lib.utils import get_datetime_string, whatis

VERBOSE_MODE = False
logging.basicConfig(level=logging.ERROR)

# ========
# Click Group and the starting point for the CLI
# ========
@click.group(context_settings={ "help_option_names": ['-h', '--help']})
@click.version_option(version=__version__)
@click.option('--verbose', '-vb', is_flag=True, default=False, help='Prefix to any command for verbosity.')
def entry_point(verbose):
	"""
	CLI interface for BTU: Python Edition
	"""
	if verbose:
		global VERBOSE_MODE  # pylint: disable=global-statement
		VERBOSE_MODE = True
		click.echo(f"Verbose mode is {'on' if verbose else 'off'}.")

# ========
# Click commands begin here.
# ========

@entry_point.command('about')
def cmd_about():
	"""
	About the btu-py application.
	"""
	print(f"btu-py version {__version__}")
	print("Copyright (C) 2025")
	print("A Python-based alternative to the original BTU Scheduler.")


@entry_point.command('config')
@click.argument('command', type=click.Choice(['show', 'edit'], case_sensitive=False))
def cmd_config(command):
	"""
	Configuration of btu-py CLI.
	"""
	from btu_py.lib.config import AppConfig
	btu_py.shared_config.set(AppConfig())

	match command.split():
		case ['show']:
			btu_py.get_config().print_config()
		case [ 'edit']:
			editor = '/usr/bin/editor'  # On Linux this is a link, configured by 'alternatives'
			if not editor:
				raise RuntimeError("No value is set for Linux environment variable $EDITOR.")
			os.system(f"{editor} {btu_py.get_config().get_config_file_path()}")
		case _:
			print(f"Subcommand '{command}' not recognized.")


@entry_point.command('clear-scheduled-tasks')
def cli_clear_scheduled_tasks():
	"""
	Clear all scheduled tasks from the Redis database.
	"""
	from btu_py.lib.scheduler import clear_all_scheduled_tasks
	if clear_all_scheduled_tasks():
		print("All scheduled tasks cleared from Redis database.")
	else:
		print("Error: Unable to clear scheduled tasks from Redis database.")


@entry_point.command('run-daemon')
@click.option('--debug', is_flag=True, default=False, help='Throw exceptions to help debugging.')
def cli_run_daemon(debug):
	"""
	Run the BTU scheduler daemon.
	"""
	if debug:
		print("TODO: Change the logger to Debug Mode.")

	from btu_py.daemon import main
	asyncio.run(main())


test_choices: list = [
	'frappe-ping',
	'pickler',
	'redis',
	'slack',
	'sql',
	'tcp-echo',
	'tcp-ping',
	'tcp-create-task-schedule',
	'tcp-cancel-task-schedule',
	'test-rq-hello-world',
	'unix-socket-async',
	'unix-socket-sync',
]
@entry_point.command('test')
@click.argument('command', type=click.Choice(test_choices, case_sensitive=False))
@click.argument('task_schedule_id', required=False)
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

		case 'frappe-ping':
			from btu_py.lib.tests import test_frappe_ping
			test_frappe_ping()

		case 'pickler':
			from btu_py.lib.tests import test_pickler
			test_pickler()

		case 'redis':
			from btu_py.lib.tests import test_redis
			try:
				test_redis()
				print("Redis connection successful.")
			except Exception as ex:
				print(f"Error: {ex}")

		case 'slack':
			from btu_py.lib.tests import test_slack
			test_slack()

		case 'sql':
			from btu_py.lib.tests import test_sql
			asyncio.run(test_sql(quiet=False))

		case 'tcp-echo':
			from btu_py.lib.tests import test_tcp_socket_echo
			test_tcp_socket_echo()
			print("TCP socket echo test completed.")

		case 'tcp-ping':
			from btu_py.lib.tests import test_tcp_socket_ping
			test_tcp_socket_ping()
			print("TCP socket ping test completed.")

		case 'tcp-create-task-schedule':
			from btu_py.lib.tests import test_tcp_socket_create_task_schedule
			if not task_schedule_id:
				print("Error: You must provide a Task Schedule ID, e.g. 'btu test tcp-create-task-schedule TS-000123'.")
				return
			test_tcp_socket_create_task_schedule(task_schedule_id)
			print("TCP socket create_task_schedule test completed.")

		case 'tcp-cancel-task-schedule':
			from btu_py.lib.tests import test_tcp_socket_cancel_task_schedule
			if not task_schedule_id:
				print("Error: You must provide a Task Schedule ID, e.g. 'btu test tcp-cancel-task-schedule TS-000123'.")
				return
			test_tcp_socket_cancel_task_schedule(task_schedule_id)
			print("TCP socket cancel_task_schedule test completed.")

		case 'test-rq-hello-world':
			from btu_py.lib.tests import test_rq_hello_world
			test_rq_hello_world()

		case 'unix-socket-async':
			from btu_py.lib.tests import test_unix_socket_async
			asyncio.run(test_unix_socket_async())

		case 'unix-socket-sync':
			from btu_py.lib.tests import test_unix_socket_sync
			test_unix_socket_sync()

		case _:
			test_choices_string = '\n    '.join(test_choices)
			print(f"Unhandled subcommand '{command}'.  Please choose one of:\n    {test_choices_string}\n")


@entry_point.command('service-status')
def cli_service_status():
	"""
	Check the status of various systemd services
	"""
	# Falcon
	command_list = [
		"sudo",
		"systemctl",
		"status",
		"btu_scheduler.service"
	]
	subprocess.run(command_list, check=False, stderr=subprocess.STDOUT)

	# Frappe Workers
	command_list = [
		"sudo",
		"systemctl",
		"status",
	]
	subprocess.run(command_list, check=False, stderr=subprocess.STDOUT)


@entry_point.command('logs')
@click.argument('command', type=click.Choice(['truncate', 'show'], case_sensitive=False))
def cli_logs(command):
	match command.split():

		case ['truncate']:
			for each_file in (
				"/etc/btu_scheduler/logs/worker.log",
			):
				try:
					print(f"DOES NOT WORK YET Truncating log file '{each_file}' ...")
					with open(each_file, 'w', encoding="utf-8"):
						pass
				except Exception as ex:
					print(f"Error: {ex}")

		case ['show']:
			with open('/etc/btu_scheduler/logs/main.log', encoding='utf-8') as file:
				for line in (file.readlines() [-100:]):
					print(line, end ='')

		case _:
			print(f"Subcommand '{command}' not recognized.")
