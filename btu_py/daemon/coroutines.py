""" btu_py/daemon/couroutines.py """

# pylint: disable=logging-fstring-interpolation


import asyncio
import os
import pathlib

import btu_py
from btu_py import get_logger
from btu_py.lib.utils import Stopwatch, whatis
from btu_py.lib.structs import BtuTaskSchedule
from btu_py.lib import scheduler


async def internal_queue_consumer(shared_queue):
	"""
	Reads TSIKs from the internal couroutine Queue, and adds them to Python RQ.
	"""
	while True:
		if shared_queue.qsize():
			get_logger().debug(f"IQM: Number of items in Queue = {shared_queue.qsize()}")
			next_task_schedule_id = await shared_queue.get()  # NOTE: The coroutine will hang out here, doing nothing, until something shows up in the Queue.
			get_logger().info(f"IQM: The next Task Schedule ID = {next_task_schedule_id}")
			task_schedule: BtuTaskSchedule = await BtuTaskSchedule.init_from_schedule_key(next_task_schedule_id)
			if task_schedule:
				scheduler.add_task_schedule_to_rq(task_schedule)
			get_logger().debug(f"IQM: Added task schedule to Redis Key 'btu_scheduler:task_execution_times'.  Size of internal queue is now {shared_queue.qsize()}")

		await asyncio.sleep(1)  # blocking request for just a moment


async def internal_queue_producer(shared_queue):
	"""
	Every N seconds, refill the Internal Queue with -all- Task Schedule IDs.

	This is a type of "safety net" for the BTU system.  By performing a "full refresh" of RQ,
	we can be confident that Tasks are always running.  Even if the RQ database is flushed or emptied,
	it will be refilled automatically after a while!
	
	As the queue is filled, Thread 1 handles consuming and procesing each TSIK.
	"""

	btu_py.get_logger().info("Initializing coroutine 'internal_queue_producer()' ...")
	stopwatch = Stopwatch()
	while True:
		elapsed_seconds = stopwatch.get_elapsed_seconds_total()  # calculate elapsed seconds since last Queue Repopulate
		if elapsed_seconds > btu_py.get_config_data().full_refresh_internal_secs:  # If sufficient time has passed ...
			btu_py.get_logger().info(f"Producer: {elapsed_seconds} seconds have elapsed.  It's time for a full-refresh of the Task Schedules in Redis!")
			result = await scheduler.queue_full_refill(shared_queue)
			if result:
				btu_py.get_logger().debug(f"  * Internal queue contains a total of {shared_queue.qsize()} values.")
				stopwatch.reset()  # reset the stopwatch and begin a new countdown.
				scheduler.rq_print_scheduled_tasks(False)  # log the Task Schedule:
			else:
				raise RuntimeError(f"Error while repopulating the internal queue! {result}")

		await asyncio.sleep(1)  # blocking request, yields controls to another coroutine for a while.


async def review_next_execution_times(shared_queue):
	"""
	----------------
	Thread #3:  Enqueue Tasks into RQ
	
	Every N seconds, examine the Next Execution Time for all scheduled RQ Jobs (this information is stored in RQ as a Unix timestamps)
	   If the Next Execution Time is in the past?  Then place the RQ Job into the appropriate queue.  RQ and Workers take over from there.
	  ----------------
	"""
	await asyncio.sleep(10)  # One-time delay of execution: this gives the other coroutines a chance to initialize.
	btu_py.get_logger().info("--> Thread '3_Scheduler' has launched.  Eligible RQ Jobs will be placed into RQ Queues at the appropriate time.")
	while True:
		btu_py.get_logger().debug("Thread 3: Attempting to add new Jobs to RQ...")
		# This thread requires a lock on the Internal Queue, so that after a Task runs, it can be rescheduled.
		stopwatch = Stopwatch()
		await scheduler.check_and_run_eligible_task_schedules(shared_queue)
		elapsed_seconds = stopwatch.get_elapsed_seconds_total()  # time just spent working on RQ database.
		# I want this thread to execute at roughly the same interval.
		# By subtracting the Time Elapsed above, from the desired Wait Time, we know how much longer the thread should sleep.
		await asyncio.sleep(btu_py.get_config().data.scheduler_polling_interval - elapsed_seconds)  # wait N seconds before trying again.


async def handle_echo_client(reader, writer):
	"""
	Unix Socket server handler: echos client's request back to them.
	"""
	get_logger().info("A client connected to the BTU Daemon Unix Socket.")

	msg_bytes = await reader.readline(encoding="utf-8")	# read the message from the client
	whatis(msg_bytes)
	get_logger().info(f"Got: {msg_bytes.decode().strip()}")	# report the message
	await asyncio.sleep(1)	# wait a moment

	# report progress
	get_logger().info('Echoing message...')
	writer.write(msg_bytes)	# send the message back
	await writer.drain()	# wait for the buffer to empty
	writer.close()	# close the connection
	await writer.wait_closed()
	print('Closing connection')	# close the connection


async def unix_domain_socket_listener():
	"""
	A simple Unix Domain Socket listener to process user requests.
	"""
	socket_path = btu_py.get_config_data().socket_path
	if pathlib.Path(socket_path).exists():
		try:
			os.unlink(socket_path)  # remove any preexisting socket files.
		except OSError as ex:
			print(f"Error in unix_domain_socket_listener() : {ex}")
			raise ex

	server = await asyncio.start_unix_server(handle_echo_client, socket_path)  # create a new server object
	async with server:
		# report message
		get_logger().info(f"SOCKET: Unix Domain Socket listening for incoming connections via file '{socket_path}'")
		await server.serve_forever()  # accept connections

	# close the connection
	# connection.close()
	# remove the socket file
	# os.unlink(socket_path)
