""" btu_py/daemon/couroutines.py """

# pylint: disable=logging-fstring-interpolation

import asyncio
import os
import pathlib
# import socket

import btu_py
from btu_py import get_logger
from btu_py.lib.utils import Stopwatch
from btu_py.lib.structs import BtuTaskSchedule
from btu_py.lib import scheduler


def get_tcp_socket_port() -> int:
	"""
	Get the TCP socket port from the configuration.
	"""
	return btu_py.get_config_data().get('tcp_socket_port', None)


async def internal_queue_consumer(shared_queue):
	"""
	Reads TSIKs from the internal couroutine Queue, and adds them to Python RQ.
	"""
	while True:
		if shared_queue.qsize():
			# get_logger().debug(f"IQM: Number of items in Queue = {shared_queue.qsize()}")
			next_task_schedule_id = await shared_queue.get()  # NOTE: The coroutine will hang out here, doing nothing, until something shows up in the Queue.
			# get_logger().info(f"IQM: The next Task Schedule ID = {next_task_schedule_id}")
			task_schedule: BtuTaskSchedule = await BtuTaskSchedule.init_from_schedule_key(next_task_schedule_id)
			if task_schedule:
				scheduler.add_task_schedule_to_rq(task_schedule)
				get_logger().debug(f"IQM: Added task schedule to Redis Key 'btu_scheduler:task_execution_times'.  Size of internal queue is now {shared_queue.qsize()}")
			else:
				get_logger().error(f"IQM: Unable to construct a BtuTaskSchedule object from Task Schedule ID = {next_task_schedule_id}")

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
			btu_py.get_logger().debug(f"Producer: {elapsed_seconds} seconds have elapsed.  Time for a full-write of Task Schedule Keys in Redis!")
			result = await scheduler.queue_full_refill(shared_queue)
			if result:
				btu_py.get_logger().debug(f"  * Internal queue contains a total of {shared_queue.qsize()} values.")
				scheduler.rq_print_scheduled_tasks(False)  # log the Task Schedule:
			else:
				btu_py.get_logger().warning("No Task Schedules found in the database.  Unable to repopulate the internal queue.")
			stopwatch.reset()  # reset the stopwatch and begin a new countdown

		await asyncio.sleep(1)  # blocking request, yields controls to another coroutine for a while.


async def review_next_execution_times(shared_queue):
	"""
	----------------
	Thread #3:  Enqueue Tasks into RQ
	
	Every N seconds, examine the Next Execution Time for all scheduled RQ Jobs (this information is stored in RQ as a Unix timestamps)
	   If the Next Execution Time is in the past?  Then place the RQ Job into the appropriate queue.  RQ and Workers take over from there.
	  ----------------
	"""
	await asyncio.sleep(5)  # One-time delay of execution: this gives the other coroutines a chance to initialize.
	btu_py.get_logger().info("Starting coroutine review_next_execution_times(), adding eligible RQ Jobs to RQ Queues at the appropriate time.")
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
	get_logger().info("Unix Socket: New client connection; applying handler 'handle_echo_client'")

	try:
		msg_bytes = await reader.readline()	# read the message from the client
		if not msg_bytes:
			get_logger().info("Unix Socket: Client closed connection before sending data.")
			return

		decoded_bytes: str = msg_bytes.decode().strip()

		get_logger().debug(f"Unix Socket: Datatype of decoded_bytes: {type(decoded_bytes)}")	# report the message
		get_logger().info(f"Unix Socket: Received this data string: '{decoded_bytes}'")	# report the message

		try:
			writer.write(msg_bytes)	# send the message back (this is a synchronous, blocking call)
			await writer.drain()	# wait for the buffer to empty

			get_logger().info("Unix Socket: Successfully echoed the message back to the client.  Closing connection.")  # Close the connection
		except (ConnectionResetError, ConnectionError, BrokenPipeError, OSError) as conn_ex:
			# Client closed connection before we could send response - this is normal, not an error
			get_logger().debug(f"Unix Socket: Client closed connection during response: {conn_ex}")
		except Exception as ex:
			get_logger().error(f"Unix Socket: Error sending response to client: {ex}")
		finally:
			# Always try to close the writer, even if there was an error
			try:
				writer.close()
				await writer.wait_closed()
			except Exception as close_ex:
				get_logger().debug(f"Unix Socket: Error closing writer (connection may already be closed): {close_ex}")
	except (ConnectionResetError, ConnectionError, BrokenPipeError, OSError) as conn_ex:
		# Client closed connection during read - this is normal, not an error
		get_logger().debug(f"Unix Socket: Client closed connection during read: {conn_ex}")
		try:
			writer.close()
		except Exception:
			pass  # Connection already closed
	except Exception as ex:
		get_logger().error(f"Unix Socket: Error in handle_echo_client() : {ex}")
		# Don't re-raise - handle gracefully to prevent "Task exception was never retrieved" errors
		try:
			writer.close()
		except Exception:
			pass  # Connection may already be closed


async def unix_domain_socket_listener():
	"""
	A simple Unix Domain Socket listener to process user requests.
	"""
	socket_path = pathlib.Path(btu_py.get_config_data().socket_path)
	if not socket_path.parent.exists():
		raise OSError(f"The parent directory for the socket file ({socket_path.parent}) does not exist.")

	if socket_path.exists():
		try:
			os.unlink(socket_path)  # remove any preexisting socket files.
		except OSError as ex:
			print(f"Error in unix_domain_socket_listener() : {ex}")
			raise ex
		except Exception as ex:
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


async def handle_tcp_echo(reader, writer):
	"""
	TCP Socket server handler: echos client's request back to them.
	"""
	data = await reader.read(100)
	# message = data.decode('utf-8')  # Explicitly specify UTF-8 encoding
	addr = writer.get_extra_info('peername')
	get_logger().info(f"TCP Socket: Received {data} from {addr}")
	response = 'Hello, Mars'
	writer.write(response.encode('utf-8'))  # Explicitly set UTF-8 encoding for the response
	await writer.drain()
	writer.close()
	await writer.wait_closed()


async def tcp_socket_listener():
	"""
	A simple TCP Socket listener to process user requests.
	"""
	port_number = get_tcp_socket_port()
	try:
		server = await asyncio.start_server(handle_tcp_echo, '0.0.0.0', port_number)
		#addr = server.sockets[0].getsockname()
		async with server:
			get_logger().info(f"Starting TCP listener on port number {port_number} ...")
			await server.serve_forever()
	except OSError as ex:
		if "Address already in use" in str(ex):
			print(f"Port {port_number} is already in use. Please choose a different port.")
		else:
			raise ex
