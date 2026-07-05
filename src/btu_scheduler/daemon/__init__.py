"""btu_scheduler/daemon/__init__.py"""

import asyncio

import structlog

from btu_scheduler.lib.config import bootstrap_scheduler
from btu_scheduler.lib.scheduler import queue_full_refill
from btu_scheduler.lib.diagnostics import diagnose_redis, diagnose_sql
from btu_scheduler.lib.utils import is_port_in_use

log = structlog.get_logger(__name__)


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

	settings, _bootstrap_log = bootstrap_scheduler(handle_signals=True)
	log.debug("Initialized configuration in Main Thread.")
	tcp_socket_enabled = not settings.disable_tcp_socket
	redis_rpc_enabled = not settings.disable_redis_rpc

	# Make sure Redis is available.
	try:
		diagnose_redis()  # Synchronous function.
	except Exception as ex:
		log.error(f"Unable to connect to Frappe Redis queue: {ex}")
		return

	await diagnose_sql(quiet=True)

	# Make sure port 8888 is available
	if tcp_socket_enabled and is_port_in_use(get_tcp_socket_port()):
		log.error(f"Port {get_tcp_socket_port()} is already in use.")
		return

	internal_queue = asyncio.Queue()
	set_tcp_internal_queue(internal_queue)

	log.info("BTU Scheduler daemon starting", company="Datahenge LLC")
	log.info("Scheduler enqueues due BTU Task Schedules in Python RQ.")
	log.info("Full refresh interval configured.", seconds=settings.full_refresh_internal_secs)

	# Redis RPC (primary control-plane)
	if redis_rpc_enabled:
		log.info("Redis RPC command listener is enabled.")
	else:
		log.warning("Redis RPC command listener is disabled.")

	# TCP Socket
	if tcp_socket_enabled:
		log.info("TCP socket listener is enabled.")
	else:
		log.warning("TCP socket listener is disabled.")

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
		log.info(f"All tasks have completed now: {task1.result()}, {task2.result()}, {task3.result()}")
	except Exception:
		raise
