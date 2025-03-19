
import requests

from btu_py.lib.config import AppConfig
from btu_py.lib.utils import whatis

NoneType = type(None)


async def get_pickled_function_from_web(task_id: str, task_schedule_id: (str, NoneType)) -> list:
	"""
	Call ERPNext REST API and acquire pickled Python function as bytes.
	"""

	if AppConfig.get('webserver_port') == 443:
		url = f"https://{AppConfig.get('webserver_ip')}/api/method/btu.btu_api.endpoints.get_pickled_task"
	else:
		url = f"http://{AppConfig.get('webserver_ip')}:{AppConfig.get('webserver_port')}/api/method/btu.btu_api.endpoints.get_pickled_task"

	headers = {
		"Authorization": AppConfig.get('webserver_token'),
		"Content-Type": "application/json",
		"Host": AppConfig.get('webserver_host_header')
	}
	if not headers["Host"]:
		headers.pop("Host")

	response = requests.get(
		url=url,
		headers=headers,
		data = {
			"task_id": task_id,
			"task_schedule_id": task_schedule_id
		},
		timeout=30
	)

	whatis(response)

	# Check if 200
	# Check if "Content-Length"

	# Convert reponse to bytes?
