""" btu-py.__init__.py """


import importlib.metadata
from semantic_version import Version as SemanticVersion

__version__ = importlib.metadata.version("btu_py")  # read the version from pyproject.toml
