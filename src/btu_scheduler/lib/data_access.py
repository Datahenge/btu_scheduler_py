"""btu_scheduler/lib/data_access.py

Mode-aware facade over the two ways BTU Scheduler can read Task Schedule data:
direct SQL (lib/sql.py) or the Frappe REST API (lib/frappe_api.py). Callers use
these functions instead of importing either backend directly, so they don't need
to know which connectivity_mode is active.
"""

import asyncio

from btu_scheduler.lib import frappe_api
from btu_scheduler.lib import sql
from btu_scheduler.lib.config import load_config


async def get_enabled_task_schedules() -> list:
	if load_config().connectivity_mode == "webserver":
		return await asyncio.to_thread(frappe_api.get_enabled_task_schedules)
	return await sql.get_enabled_task_schedules()


async def get_task_schedule_by_id(task_schedule_id: str) -> dict | None:
	if load_config().connectivity_mode == "webserver":
		return await asyncio.to_thread(frappe_api.get_task_schedule_by_id, task_schedule_id)
	return await sql.get_task_schedule_by_id(task_schedule_id)
