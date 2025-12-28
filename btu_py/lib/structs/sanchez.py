""" btu_py/lib/structus/sanchez.py """

import zlib

import json
from typing import Union

import requests

from btu_py import get_config_data
from btu_py.lib.utils import get_frappe_base_url, whatis

NoneType = type(None)


async def get_pickled_function_from_web(task_id: str, task_schedule_id: Union[str, NoneType]) -> bytes:
	"""
	Call Frappe REST API and acquire pickled Python function as bytes.
	"""
	config_data = get_config_data()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.get_pickled_task"
	headers = {
		"Authorization": config_data.webserver_token,
		"Content-Type": "application/octet-stream",
	}
	# If Frappe is running via gunicorn, in DNS Multi-tenancy mode, then we have to pass a "Host" header.
	if config_data.webserver_host_header:
		headers["Host"] = config_data.webserver_host_header

	response = requests.get(
		url=url,
		headers=headers,
		params = {
			"task_id": task_id,
			"task_schedule_id": task_schedule_id
		},
		timeout=30
	)

	if response.status_code != 200:
		raise IOError(f"Unexpected response code from Frappe Framework web server: {response.status_code}")

	# The value of response.content are encoded bytes.
	# Once decoded you have a String like this:  {"message": [ 10, 42, 2, 84, 4, 24, 28 ] }

	response_decoded = response.content.decode('utf-8')
	response_json = json.loads(response_decoded)  # convert that into JSON
	response_integer_array = response_json["message"]  # Python List of integers

	response_bytes = bytes(response_integer_array)

	print("Response Bytes:")
	print(response_bytes)

	return response_bytes
