"""btu_scheduler/daemon/__init__.py"""

import asyncio
import threading

import structlog

from btu_scheduler.lib.config import bootstrap_scheduler
from btu_scheduler.lib.scheduler import queue_full_refill
from btu_scheduler.lib.diagnostics import diagnose_redis, diagnose_sql

log = structlog.get_logger(__name__)


async def _wait_for_shutdown(shutdown_event: threading.Event) -> None:
	"""Block until SIGTERM/SIGINT sets the bootstrap shutdown event."""
	loop = asyncio.get_running_loop()
	await loop.run_in_executor(None, shutdown_event.wait)


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

	tasks = [
		asyncio.create_task(internal_queue_consumer(internal_queue), name="Internal Queue - Consumer"),
		asyncio.create_task(internal_queue_producer(internal_queue), name="Internal Queue - Producer"),
		asyncio.create_task(review_next_execution_times(internal_queue), name="Review Next Execution Times"),
		asyncio.create_task(redis_command_listener(internal_queue), name="Redis RPC Command Listener"),
	]
	shutdown_task = asyncio.create_task(_wait_for_shutdown(settings.shutdown_event), name="Shutdown Watcher")

	try:
		done, _pending = await asyncio.wait(
			[*tasks, shutdown_task],
			return_when=asyncio.FIRST_COMPLETED,
		)

		for task in done:
			if task is shutdown_task:
				log.info("BTU Scheduler daemon shutting down")
				continue
			if not task.cancelled() and (exc := task.exception()) is not None:
				raise exc
	finally:
		for task in (*tasks, shutdown_task):
			task.cancel()
		await asyncio.gather(*tasks, shutdown_task, return_exceptions=True)

	log.info("BTU Scheduler daemon stopped")
