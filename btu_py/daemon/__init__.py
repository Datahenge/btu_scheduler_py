""" btu_py/daemon/__init__.py """

# pylint: disable=logging-fstring-interpolation

import asyncio
from btu_py.lib.config import AppConfig
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

	AppConfig.init_config_from_files()
	test_redis()  # Synchronous function.
	await test_sql()
	internal_queue = asyncio.Queue()

	print("-------------------------------------")
	print("BTU Scheduler: by Datahenge LLC")
	print("-------------------------------------")
	print("\nThis daemon performs the following functions:\n")
	print("1. Performs the role of a Scheduler, enqueuing BTU Task Schedules in Python RQ whenever it's time to run them.")
	print("2. Performs a full-refresh of BTU Task Schedules every {} seconds.", AppConfig.dot.full_refresh_internal_secs)
	print("3. Listens on Unix Domain Socket for requests from the Frappe BTU web application.\n")
	AppConfig.logger().info("Main Thread started")

	# TODO: Would be lovely if the main thread knew about the child threads status?

	# Immediately on startup, Scheduler daemon should populate its internal queue with all BTU Task Schedule identifiers.
	rows_added = await queue_full_refill(internal_queue)
	if rows_added:
		AppConfig.logger().info(f"Filled internal queue with {rows_added} Task Schedule identifiers.")

	# coroutine9 = asyncio.create_task(alarm(internal_queue))	 # schedule the alarm task in the background

	coroutine1 = asyncio.create_task(internal_queue_consumer(internal_queue), name="Internal Queue - Consumer")
	coroutine2 = asyncio.create_task(internal_queue_producer(internal_queue), name="Internal Queue - Producer")
	coroutine3 = asyncio.create_task(review_next_execution_times(internal_queue), name="Review Next Execution Times")
	coroutine4 = asyncio.create_task(unix_domain_socket_listener(), name="Unix Socket Listener")

	# pylint: disable=protected-access
	while True:
		print("=== Thread Status === ")
		print(f"1. {coroutine1.get_name()} : {coroutine1._state}")
		print(f"2. {coroutine2.get_name()} : {coroutine2._state}")
		print(f"3. {coroutine3.get_name()} : {coroutine3._state}")
		print(f"4. {coroutine4.get_name()} : {coroutine4._state}")
		print("======================")

		if "FINISHED" in (coroutine1._state, coroutine2._state, coroutine3._state, coroutine4._state):
			raise RuntimeError("One of the coroutines has stopped unexpectedly; please troubleshoot.")
		await asyncio.sleep(5)
