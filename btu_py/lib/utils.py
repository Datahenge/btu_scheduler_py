""" ftp_py_docker/utils.py """

# NOTE: Functions here should not depend on other ftp-docker Python modules.

from datetime import datetime
import inspect
import ssl

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


def get_datetime_string():
	"""
	Return the current datetime in ISO 8601 format.
	"""
	now = datetime.now()
	return now.strftime("%Y-%m-%d %H:%M:%S")


def send_message_to_slack(app_config, message_string: str) -> bool:
	"""
	Send a message string to Slack using Webhooks API.
	"""
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


def _kill_process_and_children(proc):
	children = proc.children(recursive=True)
	for child in children:
		kill_process(child)
	kill_process(proc)


def kill_process(proc):
	print(f"Attempting to kill process with PID {proc.pid}")
	proc.kill()


def kill_processes_by_port(port):
	killed_any = False

	attributes = ['pid', 'name', 'connections']
	attributes = None
	for proc in psutil.process_iter(attrs=attributes):
		for conn in proc.connections():
			if conn.laddr.port == port:
				try:
					print(f"Found process with PID {proc.pid} and name '{proc.name()}'")

					if proc.name().startswith("docker"):
						print("This process is running via Docker. You must stop the container manually.")
						continue

					_kill_process_and_children(proc)
					killed_any = True

				except (PermissionError, psutil.AccessDenied) as e:
					print(f"Unable to kill process {proc.pid}. The process might be running as another user or root. Try again with sudo")
					print(str(e))

				except Exception as e:
					print(f"Error killing process {proc.pid}: {str(e)}")

	return killed_any
