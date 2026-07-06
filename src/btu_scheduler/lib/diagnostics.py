"""btu_scheduler/lib/diagnostics.py"""


def diagnose_redis():
	"""
	Test the connection to the Redis database.
	"""
	from btu_scheduler.lib.btu_rq import create_connection

	conn = create_connection()
	return conn.ping()


async def diagnose_sql(quiet=False):
	"""
	Test the connection to the Frappe database.
	"""
	from btu_scheduler.lib.config import load_config
	from btu_scheduler.lib.sql import _quote_identifier, get_database

	def quote(x):
		return _quote_identifier(x, load_config().sql_type)

	query_string = f"SELECT count(*) AS record_count FROM {quote('tabDocType')};"

	database = await get_database()
	sql_row = await database.fetch_one(query_string)
	if not quiet:
		print(f"Number of records in DocType table = {sql_row['record_count']}")


def diagnose_frappe_ping(debug_mode=False):
	"""
	Calls a built-in BTU endpoint 'test_ping'
	"""
	import requests

	from btu_scheduler.lib.config import load_config
	from btu_scheduler.lib.utils import get_frappe_base_url

	config_data = load_config()

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

	response = requests.get(url=url, headers=headers, timeout=30)
	print(f"Response Status Code: {response.status_code}")
	print(f"Response JSON: {response.json()}")


def diagnose_pickler(debug_mode: bool = True):
	"""
	Function calls the Frappe web server, and asks for 'Hello World' in bytes.
	"""
	import json

	import chardet
	import requests

	from btu_scheduler.lib.config import load_config
	from btu_scheduler.lib.utils import get_frappe_base_url

	config_data = load_config()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.test_function_ping_now_bytes"
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
		timeout=30,
	)
	print(f"\nResponse Status Code = {response.status_code}")
	print(f"Response Encoding = {response.encoding}")

	response_bytes: bytes = response.content

	response_bytes_decoded = response_bytes.decode("utf-8")

	response_bytes_dict = json.loads(response_bytes_decoded)

	list_of_byte_integers = response_bytes_dict["message"]
	print(f"Byte integers: {list_of_byte_integers}")

	result = chardet.detect(bytes(list_of_byte_integers))
	print(f"Found encoding = {result['encoding']}")

	result_string = bytes(list_of_byte_integers).decode(result["encoding"])
	print(f"String:\n{result_string}")


def ping_now():
	print("pong")


def diagnose_rq_hello_world():
	"""
	Demonstrate how Python RQ constructs a Hash key, and pickles a Python function.
	"""
	from rq import Queue

	from btu_scheduler.lib.btu_rq import create_connection

	# Create a new RQ Job.
	q = Queue(
		name="erpnext-mybench:short",
		connection=create_connection(decode_responses=True),
	)
	result = q.enqueue(ping_now)
	new_job_id = result.id
	print(f"\u2713 RQ created a new Job with identifier '{new_job_id}'")

	# Based on previous observations, this is the contents of the 'data" field
	expected_data_string = b"x\x9ck`\x9d\xaa\xc2\x00\x01\x1a=\x92I%\xa5\xf1\x05\x95z9\x99Iz%\xa9\xc5%\xc5z\x05\x99y\xe9\xf1y\xf9\xe5S\xfc4k\xa7\x94L\xd1\x03\x003\x1c\x0fF"
	print(f"Number of bytes in expected string = {len(expected_data_string)}")

	# Read the 'data' key from Redis database.  Do NOT decode the responses!
	actual_data_string = create_connection(decode_responses=False).hget(f"rq:job:{new_job_id}", "data")
	if not actual_data_string == expected_data_string:
		raise RuntimeError("These bytes should absolutely be identical.")

	print("\u2713 The 'data' key in Redis is a 100% match with expected value.")


