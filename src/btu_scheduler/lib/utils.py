"""btu_scheduler/lib/utils.py"""

# NOTE: Functions here should not depend on other btu_scheduler modules or namespaces.

import time
from datetime import datetime as DateTimeType

# Third Party
import structlog

log = structlog.get_logger(__name__)



def is_port_in_use(port: int) -> bool:
	"""
	Returns a boolean True if a socket with a particular Port is currently being used.
	"""
	import socket

	port_as_integer = int(port)
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		return s.connect_ex(("localhost", port_as_integer)) == 0


class Stopwatch:
	"""
	My own take on a stopwatch program.
	"""

	def __init__(self, description=None, disable_log=False):
		"""
		When 'disable_log' is True, no rows are written to "tabPerformance Log" SQL table.
		"""
		self.start = time.perf_counter()
		self.last_checkpoint = self.start
		self.description = description or "Stopwatch"
		self.disable_log = disable_log

	def reset(self):
		self.start = time.perf_counter()
		self.last_checkpoint = self.start

	def get_elapsed_seconds_total(self):
		now = time.perf_counter()
		seconds_elapsed_start = round(now - self.start, 2)
		return seconds_elapsed_start

	def elapsed(self, prefix=None, no_print=False):
		"""
		Print the time elapsed, and write a row to Performance Log.
		"""
		now = time.perf_counter()
		seconds_elapsed_start = round(now - self.start, 2)
		seconds_elapsed_last_checkpoint = round(now - self.last_checkpoint, 2)

		# Print a message to stdout
		if not no_print:
			message = (
				f"{seconds_elapsed_last_checkpoint} seconds since last Checkpoint, {seconds_elapsed_start} since Start."
			)
			if prefix or self.description:
				message = f"---> {prefix or self.description} {message}"
			log.info(message)

		# This is now the 'last_checkpoint'
		self.last_checkpoint = now
		return seconds_elapsed_start


def get_datetime_string():
	"""
	Return the current datetime in a easily readable format.
	"""
	return DateTimeType.now().strftime("%Y-%m-%d %H:%M:%S")



def get_frappe_base_url() -> str:
	from btu_scheduler.lib.config import load_config

	config_data = load_config()

	if config_data.webserver_port == 443:
		return f"https://{config_data.webserver_ip}"
	return f"http://{config_data.webserver_ip}:{config_data.webserver_port}"
