"""btu_scheduler/lib/frappe_api.py

REST-based replacements for lib/sql.py's direct SQL reads, used when
BTU_SCHEDULER_CONNECTIVITY_MODE=webserver. Instead of querying tabBTU Task Schedule
directly, these call whitelisted methods on the BTU Frappe app.

See docs/technical/04-webserver-only-architecture.md for why this mode exists.
"""

import structlog
import requests

from btu_scheduler.lib.config import load_config
from btu_scheduler.lib.utils import build_frappe_headers, get_frappe_base_url

log = structlog.get_logger(__name__)


def get_enabled_task_schedules() -> list[dict]:
	"""Return [{'schedule_key': ..., 'task_key': ...}, ...] for all enabled Task Schedules."""
	config_data = load_config()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.get_enabled_task_schedules"
	headers = build_frappe_headers(config_data)

	response = requests.get(url=url, headers=headers, timeout=30)
	if response.status_code != 200:
		raise IOError(
			f"Unexpected response from Frappe while listing Task Schedules: {response.status_code} {response.text}"
		)
	return response.json()["message"]


def get_task_schedule_by_id(task_schedule_id: str) -> dict | None:
	"""Return a single Task Schedule's details, or None if it does not exist / is not found."""
	config_data = load_config()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.get_task_schedule_details"
	headers = build_frappe_headers(config_data)

	response = requests.get(url=url, headers=headers, params={"task_schedule_key": task_schedule_id}, timeout=30)
	if response.status_code == 404:
		return None
	if response.status_code != 200:
		raise IOError(
			f"Unexpected response from Frappe while reading Task Schedule '{task_schedule_id}': "
			f"{response.status_code} {response.text}"
		)
	return response.json()["message"]


def get_pending_scheduler_commands() -> list[dict]:
	"""
	Drain and return commands queued by the Frappe web server for this scheduler to run.

	Replaces the Redis RPC push model (daemon/coroutines.py's redis_command_listener)
	with polling, since 'webserver' mode does not give BTU direct Redis access.
	"""
	config_data = load_config()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.get_pending_scheduler_commands"
	headers = build_frappe_headers(config_data)

	response = requests.get(url=url, headers=headers, timeout=10)
	if response.status_code != 200:
		log.error(f"Unable to poll BTU Scheduler commands from Frappe: {response.status_code} {response.text}")
		return []
	return response.json()["message"]
