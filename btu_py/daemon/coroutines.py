""" btu_py/daemon/couroutines.py """

import asyncio


async def alarm(shared_queue):
	while True:
		for i in range(5):
			print(f"This is couroutine alarm, tick number {i}")
			await asyncio.sleep(1)  # blocking request for just a moment

		print("Beep boop beep, end of Nth loop.")


async def internal_queue_manager(shared_queue):
	while True:
		for i in range(5):
			print(f"IQM: This is 'internal_queue_manager' tick number {i}")
			print(f"IQM: Number of items in Queue = {shared_queue.qsize()}")
			await asyncio.sleep(1)  # blocking request for just a moment
