"""btu_py/lib/config/__init__.py"""

# Standard library
import copy
import pathlib

# Third Party
import pprint
import toml
import tomllib  # New in Python versions 3.11+.  Useless for writing TOML, but can read it.
import urllib.parse
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

from schema import And, Optional, Or, Schema

from btu_py.lib.app_logger import build_new_logger
from btu_py.lib.utils import DictToDot

BASE_DIRECTORY = pathlib.Path("/etc/btu_scheduler")


def get_config_schema():
	"""
	Return the schema rules for the configuration file.
	"""

	result = Schema(
		{
			"name": And(str, len),  # BTU Scheduler Daemon
			"environment_name": And(str, len),
			"full_refresh_internal_secs": int,
			"jobs_site_prefix": str,
			"scheduler_polling_interval": int,
			"time_zone_string": And(str, len),  # America/Los_Angeles
			"tracing_level": And(str, len),  # INFO
			"startup_without_database_connections": bool,
			Optional("disable_unix_socket"): Or(int, bool),
			Optional("disable_tcp_socket"): Or(int, bool),
			"sql_type": And(str, len, lambda x: x in ("mariadb", "postgres")),
			"sql_host": And(str, len),
			"sql_port": int,
			"sql_database": And(str, len),
			"sql_schema": And(str, len),  # public
			"sql_user": And(str, len),
			"sql_password": And(str, len),
			"rq_host": And(str, len),
			"rq_port": int,
			"tcp_socket_port": And(int),
			"socket_path": And(str, len),
			"socket_file_group_owner": And(str, len),
			"webserver_ip": And(str, len),
			"webserver_port": int,
			"webserver_token": And(str, len),
			Optional("webserver_host_header"): And(str, len),
			Optional("slack_webhook_url"): And(str, len),
		}
	)
	return result


def get_default_config_template():
	# WARNING: Do not use 'None' as a value or the entire key will be left out of TOML file.
	return {
		"description": "BTU-PY Configuration",
		"debug_mode": False,
	}


@dataclass
class AppConfig:
	"""
	Class to hold application configuration data.
	This approach avoids import side effects, and polluting any namespaces.
	"""

	__logger: object
	__sql_connection_string: str = None
	__data_dict: dict = field(default_factory=dict)

	# Dataclass attributes with Defaults:
	__config_directory: str = copy.copy(BASE_DIRECTORY)
	__config_file_path: pathlib.Path = BASE_DIRECTORY / "btu_scheduler.toml"
	data: object = None  # this will end up being a list of attributes, accessible via dot notation

	def __init__(self):
		self.init_config_from_files()

	def get(self, key):
		return self.as_dictionary()[key]

	def as_dictionary(self):
		"""
		Return the configuration as a Python dictionary.
		"""
		return self.__data_dict

	def get_config_file_path(self):
		"""
		Return a path to the main configuration file.
		"""
		return self.__config_file_path

	def get_config_directory_path(self):
		"""
		Return a path to the main configuration file.
		"""
		return self.__config_directory

	def debug_mode_enabled(self):
		"""
		Is the application running in "Debugging Mode"?
		"""
		return bool(self.as_dictionary().get("debug_mode", False))

	def print_config(self):
		"""
		Print the main configuration settings to stdout.
		"""
		print()  # empty line for aesthetics
		printer = pprint.PrettyPrinter(indent=4, compact=False)
		printer.pprint(self.as_dictionary())
		print()  # empty line for aesthetics

	def __read_configuration_from_disk(self):
		"""
		Load the main configuration file into memory.
		"""
		with self.get_config_file_path().open(mode="rb") as fstream:
			data_dictionary = tomllib.load(fstream)

		get_config_schema().validate(data_dictionary)
		self.__data_dict = data_dictionary

	def init_config_from_files(self):
		"""
		Load from data files if they exist.  Otherwise create new, default files.
		"""
		try:
			self.__read_configuration_from_disk()
			if not self.as_dictionary():  # If file is empty:
				self.revert_to_defaults()
			if not self.as_dictionary():
				raise IOError("Failed to initialize main configuration settings.")

			# Enable some dot notation, just to make code a bit cleaner
			self.data = DictToDot(self.as_dictionary())
		except FileNotFoundError:
			print(
				f"Warning: Could not read configuration file '{self.get_config_file_path()}'.  Reverting to default values."
			)
			self.revert_to_defaults()

	def __writeback_to_disk(self):
		"""
		Write the in-memory configuration data back to disk.
		"""
		# a_logger = make_logger(__name__)
		# a_logger.info("Writing new main configuration (from default template) to disk.")

		with open(self.get_config_file_path(), "w", encoding="utf-8") as fstream:
			toml.dump(self.as_dictionary(), fstream)

	def revert_to_defaults(self):
		"""
		Revert main configuration file to default setting.
		"""
		new_file_path = self.get_config_file_path()
		print(f"Warning: Creating a new, default configuration file: {new_file_path.absolute()}")

		# If necessary, create the parent directories for the configuration file.
		if not new_file_path.parent.exists():
			# print("Creating the parent directories ...")
			# new_file_path.parent.mkdir(parents=True, exist_ok=True)
			# if not new_file_path.parent.is_dir():
			raise FileNotFoundError(
				f"Error: Configuration file's parent directory '{new_file_path.parent}' does not exist."
			)

		# 1. Write the default configuration data to disk in TOML format.
		with open(new_file_path, mode="wb") as fstream:
			toml.dump(self.__default_config_template, fstream)

		# 2. Try to read it back.
		self.__read_configuration_from_disk()

	def get_sql_type(self):
		"""
		Get the database type from configuration.
		Returns 'postgres' or 'mariadb'.
		"""
		return self.as_dictionary()["sql_type"].lower()

	def get_sql_connection_string(self):
		"""
		Create a connection string for the configured database type.
		Supports PostgreSQL and MariaDB/MySQL based on sql_type configuration.
		"""
		if not hasattr(self, "__sql_connection_string") or (not self.__sql_connection_string):
			user = urllib.parse.quote(self.as_dictionary()["sql_user"])
			password = urllib.parse.quote(self.as_dictionary()["sql_password"])
			host = self.as_dictionary()["sql_host"]
			port = self.as_dictionary()["sql_port"]
			database_name = self.as_dictionary()["sql_database"]
			sql_type = self.get_sql_type()

			if sql_type == "postgres":
				# PostgreSQL connection string using asyncpg driver
				self.__sql_connection_string = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}"
			elif sql_type == "mariadb":
				# MariaDB/MySQL connection string using asyncmy driver
				self.__sql_connection_string = f"mysql+asyncmy://{user}:{password}@{host}:{port}/{database_name}"
			else:
				raise ValueError(f"Unsupported sql_type: {sql_type}. Supported types: 'postgres', 'mariadb'")
		return self.__sql_connection_string

	def get_logger(self):
		"""
		Returns the instance of logger associated with this configuration.
		"""
		if (not hasattr(self, "_AppConfig__logger")) or (not self.__logger):
			print("Constructing a new logger ...")
			self.__logger = build_new_logger("btu_py", "/etc/btu_scheduler/logs/logger.log", self.data.tracing_level)
		return self.__logger

	def timezone(self) -> ZoneInfo:
		"""
		Return a ZoneInfo object for the application's preferred Time Zone.
		"""
		return ZoneInfo(self.data.time_zone_string)
