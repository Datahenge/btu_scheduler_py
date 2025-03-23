""" btu_py/lib/tests.py """

import psycopg

def test_redis():
	"""
	Test the connection to the Redis database.
	"""
	from btu_py.lib.btu_rq import create_connection
	conn = create_connection()
	return conn.ping()


async def test_sql():
	"""
	Test the connection to the Frappe database.
	"""
	from btu_py.lib.sql import create_connection
	query_string = "SELECT count(*) AS record_count FROM \"tabDocType\";"

	aconn = await create_connection()  # returns an object which can be used as a context.
	async with aconn.cursor() as acur:
		acursor: psycopg.AsyncCursor = await acur.execute(query_string)
		sql_row: dict = await acursor.fetchone()
		print(f"Number of records in DocType table = {sql_row['record_count']}")


def test_slack():

	import ssl
	from slack_sdk import WebClient
	from btu_py import get_config
	from btu_py.lib.utils import get_datetime_string, send_message_to_slack

	# Test One
	client = WebClient(ssl=ssl._create_unverified_context())  # pylint: disable=protected-access
	api_response = client.api_test()
	if api_response.get('ok', False):
		print("\u2713 First test successful.")
	else:
		print("\u2717 First failed.")

	# Test Two
	message = f"{get_datetime_string()} : This is a test initiated by the 'btu-py' CLI application.\nNothing to see here; move along!"
	if send_message_to_slack(get_config(), message):
		print("\u2713 Second test successful.  Please examine Slack to find a test message.")
	else:
		print("\u2717 Second test failed.")


def test_frappe_ping(debug_mode=False):
	"""
	Calls a built-in BTU endpoint 'test_ping'
	"""
	import requests
	import btu_py
	from btu_py.lib.utils import get_frappe_base_url

	btu_py.initialize_shared_config()
	config_data = btu_py.get_config_data()

	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.test_ping"
	if debug_mode:
		print(f"URL for ping = {url}")

	headers = {
		"Authorization": config_data.webserver_token,
		"Content-Type": "application/json",
	}
	# If Frappe is running via gunicorn, in DNS Multi-tenancy mode, then we have to pass a "Host" header.
	if config_data.webserver_host_header:
		headers["Host"] = config_data.webserver_host_header

	response = requests.get(
		url=url,
		headers=headers,
		timeout=30
	)
	print(f"Response Status Code: {response.status_code}")
	print(f"Response JSON: {response.json()}")


def test_pickler(debug_mode: bool=True):
	"""
	Function calls the Frappe web server, and asks for 'Hello World' in bytes.
	"""
	import requests
	import btu_py
	from btu_py.lib.utils import get_frappe_base_url

	config_data = btu_py.get_config_data()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.test_function_ping_now_bytes"
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
		timeout=30
	)

	print(f"\nResponse Status Code = {response.status_code}")

	response_bytes: bytes = response.content
	response_bytes_decoded = response_bytes.decode("utf-8")

	print(f"Response bytes as UTF-8 string:\n{response_bytes_decoded}")


def test_get_pickled_function():
	# from btu_py.lib.structs.sanchez import get_pickled_function_from_web
	# response = await get_pickled_function_from_web("TS-000008")
	pass
