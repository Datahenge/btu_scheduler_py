""" btu_py/cli.py """

# Standard Library
import asyncio
import json
import logging	# Enable debug logging
import os
import ssl
import subprocess
import sys

# Third Party
import click

# Package
from btu_py import __version__
from btu_py.lib.config import AppConfig
from btu_py.lib.utils import get_datetime_string, send_message_to_slack, whatis  # pylint: disable=unused-import

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

	try:
		AppConfig.init_config_from_files()
	except Exception as ex:  # pylint: disable=broad-exception-caught
		print(f"Unhandled exception in function entry_point() : {ex}")
		sys.exit(1)

# ========
# Click commands begin here.
# ========

@entry_point.command('about')
def cmd_about():
	"""
	About the ftp_py_docker application.
	"""
	print(f"ftp-docker version {__version__}")
	print("Copyright (C) 2024")
	print("\nConfigured images:")
	image_uids = [ each['key'] for each in AppConfig.get_all_images() ]
	for each_uid in image_uids:
		print(f"	{each_uid}")


@entry_point.command('config')
@click.argument('command', type=click.Choice(['show', 'edit'], case_sensitive=False))
def cmd_config(command):
	"""
	Configuration of ftp-docker.
	"""
	match command.split():
		case ['show']:
			AppConfig.print_config()
		case [ 'edit']:
			editor = '/usr/bin/editor'  # On Linux this is a link, configured by 'alternatives'
			if not editor:
				raise RuntimeError("No value is set for Linux environment variable $EDITOR.")
			os.system(f"{editor} {AppConfig.get_config_file_path()}")
		case _:
			print(f"Subcommand '{command}' not recognized.")


@entry_point.command('run-daemon')
@click.option('--debug', is_flag=True, default=False, help='Throw exceptions to help debugging.')
def cli_run_daemon(debug):
	"""
	Interface with Docker images.
	"""
	if debug:
		print("TODO: Add a debug mode")

	from btu_py.daemon import main
	asyncio.run(main())


@entry_point.command('test')
@click.argument('command', type=click.Choice(['redis', 'slack', 'sql', 'temp'], case_sensitive=False))
def cli_test(command):
	"""
	Run a test.
	"""
	match command:
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
			from slack_sdk import WebClient
			# Test One
			client = WebClient(ssl=ssl._create_unverified_context())  # pylint: disable=protected-access
			api_response = client.api_test()
			if api_response.get('ok', False):
				print("\u2713 First test successful.")
			else:
				print("\u2717 First failed.")

			# Test Two
			message = f"{get_datetime_string()} : This is a test initiated by the 'btu-py' CLI application.\nNothing to see here; move along!"
			if send_message_to_slack(AppConfig, message):
				print("\u2713 Second test successful.  Please examine Slack to find a test message.")
			else:
				print("\u2717 Second test failed.")
			return

		case 'temp':
			from btu_py.lib.structs import BtuTaskSchedule
			from btu_py.lib.sql import get_enabled_tasks
			asyncio.run(
				BtuTaskSchedule.init_from_task_key('TS-000002')
			)
			asyncio.run(
				get_enabled_tasks()
			)

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
		"ftp_docker_falcon.service"
	]
	subprocess.run(command_list, check=False, stderr=subprocess.STDOUT)

	# Dramatiq Worker
	command_list = [
		"sudo",
		"systemctl",
		"status",
		"ftp_docker_worker.service"
	]
	subprocess.run(command_list, check=False, stderr=subprocess.STDOUT)

	# RabbitMQ
	command_list = [
		"sudo docker container ls | grep rabbitmq"
	]
	subprocess.run(command_list, check=False, stderr=subprocess.STDOUT, shell=True)


@entry_point.command('logs')
@click.argument('command', type=click.Choice(['truncate', 'show-worker', 'show-falcon', 'show-build'], case_sensitive=False))
def cli_logs(command):
	match command.split():

		case ['truncate']:
			for each_file in (
				"/etc/ftp-docker/logs/worker.log",
				"/etc/ftp-docker/logs/falcon.log",
				"/etc/ftp-docker/logs/docker_build.log"
			):
				try:
					print(f"Truncating log file '{each_file}' ...")
					with open(each_file, 'w', encoding="utf-8"):
						pass
				except Exception as ex:
					print(f"Error: {ex}")

		case ['show-worker']:
			with open('/etc/ftp-docker/logs/worker.log', encoding='utf-8') as file:
				for line in (file.readlines() [-100:]):
					print(line, end ='')

		case ['show-falcon']:
			with open('/etc/ftp-docker/logs/falcon.log', encoding='utf-8') as file:
				for line in (file.readlines() [-100:]):
					print(line, end ='')

		case ['show-build']:
			with open('/etc/ftp-docker/logs/docker_build.log', encoding='utf-8') as file:
				for line in (file.readlines() [-100:]):
					print(line, end ='')

		case _:
			print(f"Subcommand '{command}' not recognized.")
