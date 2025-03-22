""" btu-py.__init__.py """

import contextvars
import importlib.metadata

from semantic_version import Version as SemanticVersion

__version__ = importlib.metadata.version("btu_py")  # read the version from pyproject.toml

shared_config = contextvars.ContextVar('config')

def get_config():
	return shared_config.get('config')

def get_config_data():
	return shared_config.get('config').data

def get_logger():
	return get_config().get_logger()
