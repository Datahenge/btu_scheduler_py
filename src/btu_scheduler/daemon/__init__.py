"""btu_scheduler/daemon/__init__.py"""

import asyncio

import structlog

from btu_scheduler.lib.config import bootstrap_scheduler
from btu_scheduler.lib.scheduler import queue_full_refill
from btu_scheduler.lib.diagnostics import diagnose_redis, diagnose_sql

log = structlog.get_logger(__name__)


async def main():
	"""
	Main coroutine for the daemon.
	"""
	# NOTE : To start daemon call 'asyncio.run(main()'
	from .coroutines import (
		internal_queue_consumer,
		internal_queue_producer,
		redis_command_listener,
		review_next_execution_times,
	)

	settings, _bootstrap_log = bootstrap_scheduler(handle_signals=True)
	log.debug("Initialized configuration in Main Thread.")

	# Make sure Redis is available.
	try:
		diagnose_redis()  # Synchronous function.
	except Exception as ex:
		log.error(f"Unable to connect to Frappe Redis queue: {ex}")
		return

	await diagnose_sql(quiet=True)

	internal_queue = asyncio.Queue()

	log.info("BTU Scheduler daemon starting", company="Datahenge LLC")
	log.info("Scheduler enqueues due BTU Task Schedules in Python RQ.")
	log.info("Full refresh interval configured.", seconds=settings.full_refresh_internal_secs)

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
			group.create_task(redis_command_listener(internal_queue), name="Redis RPC Command Listener")

		# Wait until all tasks are concluded (forever)
		log.info(f"All tasks have completed now: {task1.result()}, {task2.result()}, {task3.result()}")
	except Exception:
		raise
