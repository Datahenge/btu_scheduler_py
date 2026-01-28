""" btu_py/lib/tests.py """

import sys

def test_redis():
	"""
	Test the connection to the Redis database.
	"""
	from btu_py.lib.btu_rq import create_connection
	conn = create_connection()
	return conn.ping()


async def test_sql(quiet=False):
	"""
	Test the connection to the Frappe database.
	"""
	from btu_py.lib.sql import get_database, _quote_identifier
	from btu_py import get_config
	
	config = get_config()
	db_type = config.get_sql_type()
	quote = lambda x: _quote_identifier(x, db_type)
	
	query_string = f"SELECT count(*) AS record_count FROM {quote('tabDocType')};"

	database = await get_database()
	sql_row = await database.fetch_one(query_string)
	if not quiet:
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
	try:
		send_message_to_slack(get_config(), message)
		print("\u2713 Second test successful.  Please examine Slack to find a test message.")
	except Exception as ex:
		print(f"\u2717 Second test failed: {ex}")


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
	import json
	import chardet
	import requests
	import btu_py
	from btu_py.lib.utils import get_frappe_base_url

	config_data = btu_py.get_config_data()
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
	# print(f"Response as bytes:\n{response_bytes}")	

	response_bytes_decoded = response_bytes.decode("utf-8")
	# print(f"Response bytes as UTF-8 string:\n{response_bytes_decoded}")

	response_bytes_dict = json.loads(response_bytes_decoded)
	# print(f"Response bytes to string, to dictionary:\n{response_bytes_dict}")

	list_of_byte_integers = response_bytes_dict['message']
	print(f"Byte integers: {list_of_byte_integers}")

	result = chardet.detect(bytes(list_of_byte_integers))     
	print(f"Found encoding = {result['encoding']}")  

	result_string = bytes(list_of_byte_integers).decode(result['encoding'])
	print(f"String:\n{result_string}")


def ping_now():
	print("pong")


def decode_redis(src):
    if isinstance(src, list):
        rv = list()
        for key in src:
            rv.append(decode_redis(key))
        return rv
    elif isinstance(src, dict):
        rv = dict()
        for key in src:
            rv[key.decode()] = decode_redis(src[key])
        return rv
    elif isinstance(src, bytes):
        return src.decode()
    else:
        raise Exception("type not handled: " +type(src))


def test_rq_hello_world():
	"""
	Demonstrate how Python RQ constructs a Hash key, and pickles a Python function.
	"""
	import rq
	from rq import Queue
	from btu_py.lib.utils import whatis
	from btu_py.lib.btu_rq import create_connection

	# Create a new RQ Job.
	q = Queue(name="erpnext-mybench:short", connection=create_connection(decode_responses=True))
	result = q.enqueue(
		ping_now
	)
	new_job_id = result.id
	print(f"\u2713 RQ created a new Job with identifier '{new_job_id}'")

	# Based on previous observations, this is the contents of the 'data" field
	expected_data_string = b"x\x9ck`\x9d\xaa\xc2\x00\x01\x1a=\x92I%\xa5\xf1\x05\x95z9\x99Iz%\xa9\xc5%\xc5z\x05\x99y\xe9\xf1y\xf9\xe5S\xfc4k\xa7\x94L\xd1\x03\x003\x1c\x0fF"
	print(f"Number of bytes in expected string = {len(expected_data_string)}")

	# FYI, if you want to see the hexademical bytes, here's how:
	#
	# print(expected_data_string.hex(' ', 1))
	#
	# A byte consists of 8 bits, and a single hex character can represent 4 bits.  So 2 hexadecimal characters represent 1 byte.

	# Read the 'data' key from Redis database.  Do NOT decode the responses!
	actual_data_string = create_connection(decode_responses=False).hget(f"rq:job:{new_job_id}", "data")
	if not actual_data_string == expected_data_string:
		raise RuntimeError("These bytes should absolutely be identical.")

	print("\u2713 The 'data' key in Redis is a 100% match with expected value.")


async def test_unix_socket_async():
	"""Send a message to the Unix socket listener and print the response."""
	import asyncio
	import pathlib
	import btu_py
	from btu_py.lib.config import AppConfig
	btu_py.shared_config.set(AppConfig())
	
	socket_path = pathlib.Path(btu_py.get_config_data().socket_path)
	if not socket_path.exists():
		print(f"Error: Unix socket file does not exist at '{socket_path}'")
		print("Make sure the daemon is running with 'btu run-daemon'")
		return
	
	try:
		reader, writer = await asyncio.open_unix_connection(str(socket_path))
		message = "Hello Mars\n"
		writer.write(message.encode())
		await writer.drain()
		
		response = await reader.readline()
		decoded_response = response.decode().strip()
		print(f"Sent: {message.strip()}")
		print(f"Received: {decoded_response}")
		
		writer.close()
		await writer.wait_closed()
	except Exception as ex:
		print(f"Error connecting to Unix socket: {ex}")


def test_tcp_socket_listener():
	"""Test the TCP socket listener."""
	import socket
	import btu_py
	from btu_py.lib.config import AppConfig
	btu_py.shared_config.set(AppConfig())
	
	host = btu_py.get_config_data().webserver_ip
	port = btu_py.get_config_data().tcp_socket_port
	message = "Hello, Mars!"
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((host, port))
	sock.sendall(message.encode())
	response = sock.recv(1024)
	decoded_response = response.decode().strip()
	print(f"Sent Message: {message}")
	print(f"Received Response: {decoded_response}")
	sock.close()
