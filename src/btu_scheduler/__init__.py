"""btu_scheduler.__init__.py"""

import contextvars
import importlib.metadata
import sys

__version__ = importlib.metadata.version("btu-scheduler")  # read the version from pyproject.toml

from btu_scheduler._vendor.config_logging import ConfigurationError

shared_config = contextvars.ContextVar("config", default=None)


def get_config():
	# If the context variable has not been initialized, do so now.
	if shared_config.get() is None:
		initialize_shared_config()
	return shared_config.get()


def get_config_data():
	# If the context variable has not been initialized, do so now.
	if shared_config.get() is None:
		initialize_shared_config()
	return shared_config.get()


def get_logger():
	return get_config().get_logger()


def initialize_shared_config(*, handle_signals: bool = False):
	"""
	A useful one-liner function for initalizing the global content variable.
	"""
	from btu_scheduler.lib.config import load_config, load_config_with_signal_handlers

	try:
		if handle_signals:
			shared_config.set(load_config_with_signal_handlers())
		else:
			shared_config.set(load_config())
	except ConfigurationError as exc:
		print(exc, file=sys.stderr)
		sys.exit(1)
