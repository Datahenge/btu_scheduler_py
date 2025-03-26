""" btu_py/cli.py """

# Standard Library
import asyncio
import json
import logging
import os
import pathlib
import ssl
import subprocess
import sys

# Third Party
import click

# Package
import btu_py
from btu_py import __version__
from btu_py.lib.utils import get_datetime_string, send_message_to_slack, whatis

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


test_choices: set = {
	'byte-encoding', 'frappe-ping', 'redis',
	'slack', 'sql', 'pickler', 'test1'
}
@entry_point.command('test')
@click.argument('command', type=click.Choice(test_choices, case_sensitive=False))
def cli_test(command):
	"""
	Run a test.
	"""
	match command:

		case 'frappe-ping':
			from btu_py.lib.tests import test_frappe_ping
			test_frappe_ping()

		case 'redis':
			from btu_py.lib.tests import test_redis
			try:
				test_redis()
				print("Redis connection successful.")
			except Exception as ex:
				print(f"Error: {ex}")

		case 'sql':
			from btu_py.lib.tests import test_sql
			asyncio.run(test_sql())

		case 'slack':
			from btu_py.lib.tests import test_slack
			test_slack()

		case 'test1':
			from btu_py.lib.tests import test_rq_hello_world
			test_rq_hello_world()

		case 'pickler':
			from btu_py.lib.tests import test_pickler
			test_pickler()

		case 'byte-encoding':
			from btu_py.lib.tests import test_create_redis_job
			asyncio.run(test_create_redis_job())

		case _:
			print(f"Unhandled subcommand {type}")


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


@entry_point.command('prepare')
def cli_prepare():

	linux_user =  os.getlogin()
	socket_parent = pathlib.Path("/run/btu_daemon")

	print("Warning: You may need to elevate to root, so it can alter permissions for the Unix Domain Socket file.\n")

	if not socket_parent.exists():
		print(f"Creating new directory: {socket_parent}")
		subprocess.run(['sudo', 'mkdir', socket_parent.name], capture_output=True, text=True, check=True)

	if socket_parent.exists():
		print("\u2713 Path exists.")

	print(f"Granting Linux user {linux_user} full permissions to Socket file's parent directory.")
	subprocess.run(['sudo', 'chown', 'sysop:sysop', '/run/btu_daemon'], capture_output=True, text=True, check=True)

	result = subprocess.run(['ls', '-la', socket_parent.name], capture_output=True, text=True, check=True)
	print(result.stdout)
