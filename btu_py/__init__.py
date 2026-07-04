"""btu-py.__init__.py"""

import contextvars
import importlib.metadata

__version__ = importlib.metadata.version("btu_py")  # read the version from pyproject.toml

shared_config = contextvars.ContextVar("config")


def get_config():
	# If the context variable has not been initialized, do so now.
	if isinstance(shared_config.get("config"), str):
		initialize_shared_config()
	return shared_config.get("config")


def get_config_data():
	# If the context variable has not been initialized, do so now.
	if isinstance(shared_config.get("config"), str):
		initialize_shared_config()
	return shared_config.get("config")


def get_logger():
	return get_config().get_logger()


def initialize_shared_config():
	"""
	A useful one-liner function for initalizing the global content variable.
	"""
	import sys

	from pydantic import ValidationError

	from btu_py.lib.config import get_env_file_path, load_config

	try:
		shared_config.set(load_config())
	except ValidationError as exc:
		missing = [err["loc"][0] for err in exc.errors() if err["type"] == "missing"]
		other = [err for err in exc.errors() if err["type"] != "missing"]
		print("Error: BTU Scheduler configuration is incomplete.")
		if missing:
			print("\nMissing required environment variables:")
			for field in missing:
				print(f"  BTU_SCHEDULER_{field.upper()}")
			print(f"\nSet these in your environment, or in the optional .env file at:\n  {get_env_file_path()}")
		for err in other:
			field = err["loc"][0] if err["loc"] else "?"
			print(f"  BTU_SCHEDULER_{str(field).upper()}: {err['msg']}")
		sys.exit(1)
