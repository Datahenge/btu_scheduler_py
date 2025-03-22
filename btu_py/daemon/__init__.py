""" btu_py/daemon/__init__.py """

# pylint: disable=logging-fstring-interpolation

import asyncio

import btu_py
from btu_py.lib import config
from btu_py.lib.tests import test_redis, test_sql
from btu_py.lib.scheduler import queue_full_refill


# NOTE : To start daemon call 'asyncio.run(main()'

async def main():
	"""
	Main coroutine for the daemon.
	"""
	from . coroutines import (
		internal_queue_consumer,
		internal_queue_producer,
		review_next_execution_times,
		unix_domain_socket_listener,
	)

	btu_py.shared_config.set(config.AppConfig())
	btu_py.get_logger().info("Initialized configuration in Main Thread.")

	test_redis()  # Synchronous function.
	await test_sql()
	internal_queue = asyncio.Queue()

	print("-------------------------------------")
	print("BTU Scheduler: by Datahenge LLC")
	print("-------------------------------------")
	print("\nThis daemon performs the following functions:\n")
	print("1. Performs the role of a Scheduler, enqueuing BTU Task Schedules in Python RQ whenever it's time to run them.")
	print("2. Performs a full-refresh of BTU Task Schedules every {} seconds.", btu_py.get_config_data().full_refresh_internal_secs)
	print("3. Listens on Unix Domain Socket for requests from the Frappe BTU web application.\n")

	# Immediately on startup, Scheduler daemon should populate its internal queue with all BTU Task Schedule identifiers.
	_ = await queue_full_refill(internal_queue)

	# handle the failure of any tasks in the group
	try:
		# create a taskgroup
		async with asyncio.TaskGroup() as group:
			task1 = group.create_task(internal_queue_consumer(internal_queue), name="Internal Queue - Consumer")
			task2 = group.create_task(internal_queue_producer(internal_queue), name="Internal Queue - Producer")
			task3 = group.create_task(review_next_execution_times(internal_queue), name="Review Next Execution Times")
			task4 = group.create_task(unix_domain_socket_listener(), name="Unix Socket Listener")

		# Wait until all tasks are concluded (forever)
		btu_py.get_logger().info(f"All tasks have completed now: {task1.result()}, {task2.result()}, {task3.result()}, {task4.result()}")
	except* Exception as ex:
		raise ex
