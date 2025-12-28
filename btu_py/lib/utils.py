""" btu_py/lib/utils.py """

# NOTE: Functions here should not depend on other btu_py modules or namespaces.

from datetime import datetime as DateTimeType
import inspect
import ssl
import time

# Third Party
import psutil
from slack_sdk.webhook import WebhookClient


def validate_datatype(argument_name, argument_value, expected_type, mandatory=False):
	"""
	A helpful generic function for checking a variable's datatype, and throwing an error on mismatches.
	Absolutely necessary when dealing with extremely complex Python programs that talk to SQL, HTTP, Redis, etc.

	NOTE: expected_type can be a single Type, or a tuple of Types.
	"""
	# Throw error if missing mandatory argument.
	NoneType = type(None)
	if mandatory and isinstance(argument_value, NoneType):
		raise ValueError(f"Argument '{argument_name}' is mandatory.")

	if not argument_value:
		return argument_value  # datatype is going to be a NoneType, which is okay if not mandatory.

	# Check argument type
	if not isinstance(argument_value, expected_type):
		if isinstance(expected_type, tuple):
			expected_type_names = [ each.__name__ for each in expected_type ]
			msg = f"Argument '{argument_name}' should be one of these types: '{', '.join(expected_type_names)}'"
			msg += f"\nFound a {type(argument_value).__name__} with value '{argument_value}' instead."
		else:
			msg = f"Argument '{argument_name}' should be of type = '{expected_type.__name__}'"
			msg += f"<br>Found a {type(argument_value).__name__} with value '{argument_value}' instead."
		raise ValueError(msg)

	# Otherwise, return the argument to the caller.
	return argument_value


def whatis(message):
	"""
	This function can be called to assist in debugging, showing an object's value, type, and call stack.
	"""
	inspected_stack = inspect.stack()

	direct_caller = inspected_stack[1]
	direct_caller_linenum = direct_caller[2]

	parent_caller = inspected_stack[2]
	parent_caller_function = parent_caller[3]
	parent_caller_path = parent_caller[1]
	parent_caller_line = parent_caller[2]

	message_type = str(type(message)).replace('<', '').replace('>', '')
	msg = "---> DEBUG (dw_etl.generics.whatis)\n"
	msg += f"* Initiated on Line: {direct_caller_linenum}"
	msg += f"\n  * Value: {message}\n  * Type: {message_type}"
	msg += f"\n  * Caller: {parent_caller_function}"
	msg += f"\n  * Caller Path: {parent_caller_path}\n  * Caller Line: {parent_caller_line}\n"
	print(msg)


def send_message_to_slack(app_config, message_string: str) -> bool:
	"""
	Send a message string to Slack using Webhooks API.
	"""
	if 'slack_webhook_url' not in app_config.as_dictionary():
		raise RuntimeError("Cannot send message to Slack: Configuration file is missing an entry 'slack_webhook_url'")
	webhook_url = app_config.as_dictionary()['slack_webhook_url']
	webhook = WebhookClient(url=webhook_url, ssl=ssl._create_unverified_context())  # pylint: disable=protected-access
	response = webhook.send(text=message_string)
	return response.status_code == 200 and response.body == "ok"


def is_port_in_use(port: int) -> bool:
	"""
	Returns a boolean True if a socket with a particular Port is currently being used.
	"""
	import socket
	port_as_integer = int(port)
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		return s.connect_ex(('localhost', port_as_integer)) == 0


class Stopwatch:
	"""
	My own take on a stopwatch program.
	"""
	def __init__(self, description=None, disable_log=False):
		"""
		When 'disable_log' is True, no rows are written to "tabPerformance Log" SQL table.
		"""
		self.start = time.perf_counter()
		self.last_checkpoint = self.start
		self.description = description or "Stopwatch"
		self.disable_log = disable_log

	def reset(self):
		self.start = time.perf_counter()
		self.last_checkpoint = self.start

	def get_elapsed_seconds_total(self):
		now = time.perf_counter()
		seconds_elapsed_start = round(now - self.start,2)
		return seconds_elapsed_start

	def elapsed(self, prefix=None, no_print=False):
		"""
		Print the time elapsed, and write a row to Performance Log.
		"""
		now = time.perf_counter()
		seconds_elapsed_start = round(now - self.start,2)
		seconds_elapsed_last_checkpoint = round(now - self.last_checkpoint,2)

		# Print a message to stdout
		if not no_print:
			message = f"{seconds_elapsed_last_checkpoint} seconds since last Checkpoint, {seconds_elapsed_start} since Start."
			if prefix or self.description:
				message = f"---> {prefix or self.description} {message}"
			print(message)

		# This is now the 'last_checkpoint'
		self.last_checkpoint = now
		return seconds_elapsed_start


class DictToDot(dict):
	"""
	Makes a dictionary accessible via dot notation.
	"""
	def __init__(self, *args, **kwargs):
		super(DictToDot, self).__init__(*args, **kwargs)
		for arg in args:
			if isinstance(arg, dict):
				for k, v in arg.items():
					self[k] = v

		if kwargs:
			for k, v in kwargs.items():
				self[k] = v

	def __getattr__(self, attr):
		return self.get(attr)

	def __setattr__(self, key, value):
		self.__setitem__(key, value)

	def __setitem__(self, key, value):
		super(DictToDot, self).__setitem__(key, value)
		self.__dict__.update({key: value})

	def __delattr__(self, item):
		self.__delitem__(item)

	def __delitem__(self, key):
		super(DictToDot, self).__delitem__(key)
		del self.__dict__[key]


def get_datetime_string():
	"""
	Return the current datetime in a easily readable format.
	"""
	return DateTimeType.now().strftime("%Y-%m-%d %H:%M:%S")


def utc_to_rq_string(datetime_utc: DateTimeType) -> str:
	#  The format is VERY important.  If the UTC DateTime is not correctly formatted,
	#  it *will crash* the Python RQ Worker.

	# 2022-12-01T08:32:20.580242150Z
	# 2022-12-01T08:32:20Z

	result = datetime_utc.isoformat()
	print(f"utc_to_rq_string() >>> {result}")
	return result


def get_frappe_base_url() -> str:
	import btu_py
	config_data = btu_py.get_config_data()

	if config_data.webserver_port == 443:
		return f"https://{config_data.webserver_ip}"
	return f"http://{config_data.webserver_ip}:{config_data.webserver_port}"
