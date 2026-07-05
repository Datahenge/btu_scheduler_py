"""btu_scheduler/daemon/__init__.py"""

import asyncio

import btu_scheduler
from btu_scheduler.lib.scheduler import queue_full_refill
from btu_scheduler.lib.tests import test_redis, test_sql
from btu_scheduler.lib.utils import is_port_in_use


async def main():
	"""
	Main coroutine for the daemon.
	"""
	# NOTE : To start daemon call 'asyncio.run(main()'
	from .coroutines import (
		get_tcp_socket_port,
		internal_queue_consumer,
		internal_queue_producer,
		redis_command_listener,
		review_next_execution_times,
		set_tcp_internal_queue,
		tcp_socket_listener,
	)

	btu_scheduler.initialize_shared_config(handle_signals=True)
	btu_scheduler.get_logger().debug("Initialized configuration in Main Thread.")
	tcp_socket_enabled = not btu_scheduler.get_config().disable_tcp_socket
	redis_rpc_enabled = not btu_scheduler.get_config().disable_redis_rpc

	# Make sure Redis is available.
	try:
		test_redis()  # Synchronous function.
	except Exception as ex:
		btu_scheduler.get_logger().error(f"Unable to connect to Frappe Redis queue: {ex}")
		return

	await test_sql(quiet=True)

	# Make sure port 8888 is available
	if tcp_socket_enabled and is_port_in_use(get_tcp_socket_port()):
		btu_scheduler.get_logger().error(f"Port {get_tcp_socket_port()} is already in use.")
		return

	internal_queue = asyncio.Queue()
	set_tcp_internal_queue(internal_queue)

	print("-------------------------------------")
	print("BTU Scheduler: by Datahenge LLC")
	print("-------------------------------------")
	print("\nThis daemon performs the following functions:\n")
	print(
		"* Performs the role of a Scheduler, enqueuing BTU Task Schedules in Python RQ whenever it's time to run them."
	)
	print(
		f"* Performs a full-refresh of BTU Task Schedules every {btu_scheduler.get_config_data().full_refresh_internal_secs} seconds."
	)

	# Redis RPC (primary control-plane)
	if redis_rpc_enabled:
		print("* Listens for commands via Redis RPC (primary control channel).")
	else:
		print("Warning: Redis RPC command listener is disabled.")

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
			task1 = group.create_task(
				internal_queue_consumer(internal_queue),
				name="Internal Queue - Consumer",
			)
			task2 = group.create_task(
				internal_queue_producer(internal_queue),
				name="Internal Queue - Producer",
			)
			task3 = group.create_task(
				review_next_execution_times(internal_queue),
				name="Review Next Execution Times",
			)
			if redis_rpc_enabled:
				group.create_task(redis_command_listener(), name="Redis RPC Command Listener")
			if tcp_socket_enabled:
				group.create_task(tcp_socket_listener(), name="TCP Socket Listener")

		# Wait until all tasks are concluded (forever)
		btu_scheduler.get_logger().info(f"All tasks have completed now: {task1.result()}, {task2.result()}, {task3.result()}")
	except Exception as ex:
		raise ex
