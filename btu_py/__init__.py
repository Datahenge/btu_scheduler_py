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
	return shared_config.get("config").data


def get_logger():
	return get_config().get_logger()


def initialize_shared_config():
	"""
	A useful one-liner function for initalizing the global content variable.
	"""
	from btu_py.lib.config import AppConfig

	shared_config.set(AppConfig())
