"""btu_scheduler/lib/diagnostics.py"""


def diagnose_redis():
	"""
	Test the connection to the Redis database.
	"""
	from btu_scheduler.lib.btu_rq import create_connection

	conn = create_connection()
	return conn.ping()


def diagnose_redis_version():
	"""
	Verify Redis server is >= 6.2.0 (required for ZRANGE BYSCORE).
	"""
	from btu_scheduler.lib.btu_rq import create_connection

	conn = create_connection()
	version_string = conn.info("server")["redis_version"]
	major, minor, *_ = (int(x) for x in version_string.split("."))
	if (major, minor) < (6, 2):
		raise RuntimeError(
			f"Redis {version_string} is too old — BTU Scheduler requires 6.2+ for ZRANGE BYSCORE."
		)
	print(f"Redis version: {version_string} (>= 6.2 required).")


def diagnose_rq_workers():
	"""
	Verify at least one RQ worker is registered and report their queues and state.
	"""
	from rq import Worker

	from btu_scheduler.lib.btu_rq import create_connection

	conn = create_connection()
	workers = Worker.all(connection=conn)
	if not workers:
		raise RuntimeError("No RQ workers are currently registered. Jobs will queue but never execute.")
	for w in workers:
		queue_names = ", ".join(q.name for q in w.queues)
		print(f"  Worker {w.name[:8]}…  state={w.state}  queues=[{queue_names}]")
	print(f"Found {len(workers)} active worker(s).")


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
	from btu_scheduler.lib.utils import build_frappe_headers, get_frappe_base_url

	config_data = load_config()

	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.test_ping"
	if debug_mode:
		print(f"URL for ping = {url}")

	headers = build_frappe_headers(config_data)

	response = requests.get(url=url, headers=headers, timeout=30)
	print(f"Response Status Code: {response.status_code}")
	print(f"Response JSON: {response.json()}")


def diagnose_pickler(debug_mode: bool = True):
	"""
	Calls the Frappe web server, receives a pickled Python function as bytes,
	and verifies it deserializes to a callable with the expected name.
	"""
	import json

	import requests

	from btu_scheduler.lib.config import load_config
	from btu_scheduler.lib.utils import build_frappe_headers, get_frappe_base_url

	config_data = load_config()
	url = f"{get_frappe_base_url()}/api/method/btu.btu_api.endpoints.test_function_ping_now_bytes"
	headers = build_frappe_headers(config_data)

	response = requests.get(url=url, headers=headers, timeout=30)
	if response.status_code != 200:
		raise IOError(f"Unexpected status code from Frappe: {response.status_code}")

	payload = json.loads(response.content.decode("utf-8"))
	pickled_bytes = bytes(payload["message"])
	print(f"Received {len(pickled_bytes)} pickled bytes from Frappe.")

	# Validate pickle structure without fully deserializing — the pickled function
	# references the 'btu' Frappe app module, which is not importable in this venv.
	# Pickle data always begins with 0x80 followed by the protocol version byte.
	if not pickled_bytes or pickled_bytes[0] != 0x80:
		raise RuntimeError(f"Bytes do not start with a valid pickle header (got: {pickled_bytes[:4]!r}).")
	protocol = pickled_bytes[1]
	print(f"✓ Valid pickle data: protocol {protocol}, {len(pickled_bytes)} bytes.")


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
		name="default",
		connection=create_connection(decode_responses=True),
	)
	result = q.enqueue(ping_now)
	new_job_id = result.id
	print(f"\u2713 RQ created a new Job with identifier '{new_job_id}'")

	# Fetch the job back from Redis and verify it references the correct function.
	from rq.job import Job

	job = Job.fetch(new_job_id, connection=create_connection(decode_responses=False))
	expected_func = "btu_scheduler.lib.diagnostics.ping_now"
	if job.func_name != expected_func:
		raise RuntimeError(f"Job func_name mismatch: expected '{expected_func}', got '{job.func_name}'.")
	print(f"\u2713 Job deserializes correctly: func_name = '{job.func_name}'")

	job.delete()
	print(f"\u2713 Diagnostic job '{new_job_id}' removed from Redis.")


