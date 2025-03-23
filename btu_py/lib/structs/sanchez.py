""" btu_py/lib/structus/sanchez.py """

import json
from typing import Union

import requests

from btu_py import get_config_data
from btu_py.lib.utils import get_frappe_base_url

NoneType = type(None)


async def get_pickled_function_from_web(task_id: str, task_schedule_id: Union[str, NoneType]) -> bytes:
	"""
	Call ERPNext REST API and acquire pickled Python function as bytes.
	"""
	config_data = get_config_data()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.get_pickled_task"
	headers = {
		"Authorization": config_data.webserver_token,
		"Content-Type": "application/json",
	}
	# If Frappe is running via gunicorn, in DNS Multi-tenancy mode, then we have to pass a "Host" header.
	if config_data.webserver_host_header:
		headers["Host"] = config_data.webserver_host_header

	request_payload = {
		"task_id": task_id,
		"task_schedule_id": task_schedule_id
	}

	response = requests.get(
		url=url,
		headers=headers,
		params = request_payload,
		timeout=30
	)

	if response.status_code != 200:
		raise IOError(f"Unexpected response code from Frappe Framework web server: {response.status_code}")

	# The value of response.content are encoded bytes.
	# Once decoded you have a String like this:  {"message": [ 10, 42, 2, 84, 4, 24, 28 ] }
	# Convert that into JSON
	# Return just the value, not the key

	response_string = response.content.decode("utf-8")
	response_json = json.loads(response_string)
	return bytes(response_json["message"])  # bit of magic here: convert our list fo integers into bytes (a pickled Python function)
