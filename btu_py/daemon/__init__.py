""" btu_py/daemon/__init__.py """

import asyncio
from btu_py.lib.config import AppConfig
from btu_py.lib.tests import test_redis, test_sql

# NOTE : To start daemon call 'asyncio.run(main()'

async def main():
	"""
	Main coroutine for the daemon.
	"""
	from . coroutines import alarm, internal_queue_manager

	AppConfig.init_config_from_files()
	test_redis()  # Synchronous function.
	await test_sql()
	internal_queue = asyncio.Queue()

	coroutine1 = asyncio.create_task(alarm(internal_queue))	 # schedule the alarm task in the background
	coroutine2 = asyncio.create_task(internal_queue_manager(internal_queue))

	# simulate continue on with other things
	while True:
		print("I am the main thread of BTU daemon: prepared to be whelmed!")
		await asyncio.sleep(1)
