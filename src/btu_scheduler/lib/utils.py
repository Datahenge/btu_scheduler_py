"""btu_scheduler/lib/utils.py"""

# NOTE: Functions here should not depend on other btu_scheduler modules or namespaces.

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from btu_scheduler.lib.config import SchedulerSettings


class Stopwatch:
	"""
	Measures seconds elapsed since it was created or last reset.
	"""

	def __init__(self):
		self.start = time.perf_counter()

	def reset(self):
		self.start = time.perf_counter()

	def get_elapsed_seconds_total(self):
		return round(time.perf_counter() - self.start, 2)


def build_frappe_headers(config: "SchedulerSettings", content_type: str = "application/json") -> dict[str, str]:
	"""Build the HTTP headers required for every Frappe REST API call."""
	headers: dict[str, str] = {
		"Authorization": config.webserver_token,
		"Content-Type": content_type,
	}
	if config.webserver_host_header:
		headers["Host"] = config.webserver_host_header
	return headers


def get_frappe_base_url() -> str:
	from btu_scheduler.lib.config import load_config

	config_data = load_config()

	if config_data.webserver_port == 443:
		return f"https://{config_data.webserver_ip}"
	return f"http://{config_data.webserver_ip}:{config_data.webserver_port}"
