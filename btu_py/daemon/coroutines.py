""" btu_py/daemon/couroutines.py """

# pylint: disable=logging-fstring-interpolation

import asyncio
import socket
import os

from btu_py.lib.config import AppConfig
from btu_py.lib.utils import get_datetime_string, Stopwatch
from btu_py.lib.structs import BtuTaskSchedule
from btu_py.lib import scheduler

async def alarm(shared_queue):

	print(f"shared queue = {shared_queue}")
	while True:
		for i in range(5):
			print(f"This is couroutine alarm, tick number {i}")
			await asyncio.sleep(1)  # blocking request for just a moment

		print("Beep boop beep, end of Nth loop.")


async def internal_queue_consumer(shared_queue):
	"""
	Reads TSIKs from the internal couroutine Queue, and adds them to Python RQ.
	"""
	while True:
		print(f"IQM: This is 'internal_queue_manager' at time = {get_datetime_string()}")
		print(f"IQM: Number of items in Queue = {shared_queue.qsize()}")
		if shared_queue.qsize():
			next_task_schedule_id = await shared_queue.get()  # NOTE: The coroutine will hang out here, doing nothing, until something shows up in the Queue.
			task_schedule = BtuTaskSchedule.get_task_schedule_by_id(next_task_schedule_id)
			if task_schedule:
				scheduler.add_task_schedule_to_rq(task_schedule)
			print(f"IQM: Added Task Schedule to RQ.  Items remaining in Queue = {shared_queue.qsize()}")

		await asyncio.sleep(1)  # blocking request for just a moment


async def internal_queue_producer(shared_queue):
	"""
	Every N seconds, refill the Internal Queue with -all- Task Schedule IDs.

	This is a type of "safety net" for the BTU system.  By performing a "full refresh" of RQ,
	we can be confident that Tasks are always running.  Even if the RQ database is flushed or emptied,
	it will be refilled automatically after a while!
	
	As the queue is filled, Thread 1 handles consuming and procesing each TSIK.
	"""

	print("Starting thread number 2: the internal queue Producer ...")
	stopwatch = Stopwatch()
	while True:
		AppConfig.logger().debug(f"Thread 2: Attempting to Auto-Refill the Internal Queue (current time is {get_datetime_string()})...")
		elapsed_seconds = stopwatch.get_elapsed_seconds_total()  # calculate elapsed seconds since last Queue Repopulate
		if elapsed_seconds > AppConfig.dot.full_refresh_internal_secs:  # If sufficient time has passed ...
			AppConfig.logger().info(f"{elapsed_seconds} seconds have elapsed.  It's time for a full-refresh of the Task Schedules in Redis!")
			AppConfig.logger().debug(f"  * Before refill, the queue contains {shared_queue.qsize()} values.")
			result = await scheduler.queue_full_refill(shared_queue)
			if result:
				AppConfig.logger().debug(f"  * Added {result} values to the internal FIFO queue.")
				AppConfig.logger().debug(f"  * Internal queue contains a total of {shared_queue.qsize()} values.")
				stopwatch.reset()  # reset the stopwatch and begin a new countdown.
				scheduler.rq_print_scheduled_tasks(False)  # log the Task Schedule:
			else:
				raise RuntimeError(f"Error while repopulating the internal queue! {result}")

		await asyncio.sleep(0.75)  # blocking request, yields controls to another coroutine for a while.


async def review_next_execution_times(shared_queue):
	"""
	----------------
	Thread #3:  Enqueue Tasks into RQ
	
	Every N seconds, examine the Next Execution Time for all scheduled RQ Jobs (this information is stored in RQ as a Unix timestamps)
	   If the Next Execution Time is in the past?  Then place the RQ Job into the appropriate queue.  RQ and Workers take over from there.
	  ----------------
	"""
	await asyncio.sleep(10)  # One-time delay of execution: this gives the other coroutines a chance to initialize.
	AppConfig.logger().info("--> Thread '3_Scheduler' has launched.  Eligible RQ Jobs will be placed into RQ Queues at the appropriate time.")
	while True:
		AppConfig.logger().debug("Thread 3: Attempting to add new Jobs to RQ...")
		# This thread requires a lock on the Internal Queue, so that after a Task runs, it can be rescheduled.
		stopwatch = Stopwatch()
		scheduler.check_and_run_eligible_task_schedules(shared_queue)
		elapsed_seconds = stopwatch.get_elapsed_seconds_total()  # time just spent working on RQ database.
		# I want this thread to execute at roughly the same interval.
		# By subtracting the Time Elapsed above, from the desired Wait Time, we know how much longer the thread should sleep.
		await asyncio.sleep(AppConfig.dot.scheduler_polling_interval - elapsed_seconds)  # wait N seconds before trying again.


async def unix_domain_socket_listener():
	"""
	A simple Unix Domain Socket listener to process user requests.
	"""
	socket_path = AppConfig.dot.socket_path
	# Remove any preexisting socket file
	try:
		os.unlink(socket_path)
	except OSError as ex:
		print(f"Error in unix_domain_socket_listener() : {ex}")
		if os.path.exists(socket_path):
			raise OSError(f"Unix socket file already exists: {socket_path}") from ex

	server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)	# Create a new Unix socket server
	server.bind(socket_path)	# Bind the socket to the path
	server.listen(1)
	# accept connections
	print('Daemon is listening for incoming connections on Unix socket...')

	while True:
		try:
			connection, client_address = server.accept()
			print('Connection from', str(connection).split(", ")[0][-4:])

			# receive data from the client
			while True:
				data = connection.recv(1024)
				if not data:
					break
				print('Received data:', data.decode())

				# Send a response back to the client
				response = 'Hello from the server!'
				connection.sendall(response.encode())

				# handle_client_request()

		except Exception as ex:
			print(f"ERROR during unix_domain_socket_listener() loop : {ex}")

		await asyncio.sleep(1.25)  # blocking request for just a moment

	# close the connection
	# connection.close()
	# remove the socket file
	# os.unlink(socket_path)


	# May need to update socket file permissions
	# match ipc_stream::update_socket_file_permissions(&unlocked_app_config.socket_path, &unlocked_app_config.socket_file_group_owner) {
