""" btu_py/daemon/__init__.py """

# pylint: disable=logging-fstring-interpolation

import asyncio

import btu_py
from btu_py.lib import config
from btu_py.lib.tests import test_redis, test_sql
from btu_py.lib.scheduler import queue_full_refill
from btu_py.lib.utils import is_port_in_use


async def main():
	"""
	Main coroutine for the daemon.
	"""
	# NOTE : To start daemon call 'asyncio.run(main()'
	from . coroutines import (
		get_tcp_socket_port,
		internal_queue_consumer,
		internal_queue_producer,
		review_next_execution_times,
		unix_domain_socket_listener,
		tcp_socket_listener
	)

	btu_py.shared_config.set(config.AppConfig())
	btu_py.get_logger().debug("Initialized configuration in Main Thread.")
	unix_socket_enabled = not bool(btu_py.get_config().as_dictionary().get("disable_unix_socket", False))
	tcp_socket_enabled = not bool(btu_py.get_config().as_dictionary().get("disable_tcp_socket", False))

	# Make sure Redis is available.
	try:
		test_redis()  # Synchronous function.
	except Exception as ex:
		btu_py.get_logger().error(f"Unable to connect to Frappe Redis queue: {ex}")
		return

	await test_sql(quiet=True)


	# Make sure port 8888 is available
	if tcp_socket_enabled and is_port_in_use(get_tcp_socket_port()):
		btu_py.get_logger().error(f"Port {get_tcp_socket_port()} is already in use.")
		return

	internal_queue = asyncio.Queue()

	print("-------------------------------------")
	print("BTU Scheduler: by Datahenge LLC")
	print("-------------------------------------")
	print("\nThis daemon performs the following functions:\n")
	print("* Performs the role of a Scheduler, enqueuing BTU Task Schedules in Python RQ whenever it's time to run them.")
	print(f"* Performs a full-refresh of BTU Task Schedules every {btu_py.get_config_data().full_refresh_internal_secs} seconds.")

	# Unix Socket
	if unix_socket_enabled:
		print("* Listens on Unix Domain Socket for requests from the Frappe BTU web application.")
	else:
		print("Warning: Unix Domain Socket is disabled.")

	# TCP Socket
	if tcp_socket_enabled:
		print("* Listens on TCP Socket for requests from the Frappe BTU web application.")
	else:
		print("Warning: TCP Socket is disabled.")

	# Immediately on startup, Scheduler daemon should populate its internal queue with all BTU Task Schedule identifiers.
	_ = await queue_full_refill(internal_queue)

	# handle the failure of any tasks in the group
	try:
		# create a taskgroup
		async with asyncio.TaskGroup() as group:
			task1 = group.create_task(internal_queue_consumer(internal_queue), name="Internal Queue - Consumer")
			task2 = group.create_task(internal_queue_producer(internal_queue), name="Internal Queue - Producer")
			task3 = group.create_task(review_next_execution_times(internal_queue), name="Review Next Execution Times")
			if unix_socket_enabled:
				task4 = group.create_task(unix_domain_socket_listener(), name="Unix Socket Listener")
			elif tcp_socket_enabled:
				task4 = group.create_task(tcp_socket_listener(), name="TCP Socket Listener")

		# Wait until all tasks are concluded (forever)
		btu_py.get_logger().info(f"All tasks have completed now: {task1.result()}, {task2.result()}, {task3.result()}, {task4.result()}")
	except Exception as ex:
		raise ex
