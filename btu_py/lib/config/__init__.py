""" ftp_py_docker/config.py """

# Standard library
import pathlib
import urllib.parse

import pprint
import tomllib  # New in Python versions 3.11+.  Useless for writing TOML, but can read it.

# Third Party
from schema import Schema, And, Or, Optional  # pylint: disable=unused-import
import toml

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
			"scheduler_polling_interval": int,
			"time_zone_string": And(str, len),  # America/Los_Angeles
			"tracing_level": And(str, len),  # INFO
			"startup_without_database_connections": bool,

			"sql_type": And(str,len, lambda x: x in ('mariadb', 'postgres')),
			"sql_host": And(str,len),
			"sql_port": int,
			"sql_database": And(str, len),
			"sql_schema": And(str, len),  # public
			"sql_user": And(str, len),
			"sql_password": And(str, len),

			"rq_host": And(str, len),
			"rq_port": int,
			"socket_path": And(str, len),
			"socket_file_group_owner": And(str, len),
			"webserver_ip": And(str, len),
			"webserver_port": int,
			"webserver_token": And(str, len),

			Optional("slack_webhook_url"): And(str, len),

		})
	return result


class AppConfig:
	"""
	Class to hold application configuration data.
	This approach avoids import side effects, and polluting any namespaces.
	"""
	__DEFAULT_CONFIG_FILE = BASE_DIRECTORY / "btu_scheduler.toml"

	# WARNING: Do not use 'None' as a value or the entire key will be left out of TOML file.
	__default_config_template = {
		"description": "BTU-PY Configuration",
		"debug_mode": False,
	}

	# Private objects for this class:
	__config_dict = {}
	__config_file = __DEFAULT_CONFIG_FILE
	__config_directory = None

	@staticmethod
	def get(key):
		return AppConfig.as_dictionary()[key]

	@staticmethod
	def as_dictionary():
		"""
		Return the configuration as a Python dictionary.
		"""
		return AppConfig.__config_dict

	@staticmethod
	def get_config_file_path():
		"""
		Return a path to the main configuration file.
		"""
		return AppConfig.__config_file

	@staticmethod
	def get_config_directory_path():
		"""
		Return a path to the main configuration file.
		"""
		return AppConfig.__config_directory

	@staticmethod
	def debug_mode_enabled():
		"""
		Is the application running in "Debugging Mode"?
		"""
		return bool(AppConfig.as_dictionary().get('debug_mode', False))

	@staticmethod
	def print_config():
		"""
		Print the main configuration settings to stdout.
		"""
		print()  # empty line for aesthetics
		printer = pprint.PrettyPrinter(indent=4, compact=False)
		printer.pprint(AppConfig.as_dictionary())
		print()  # empty line for aesthetics

	@staticmethod
	def __read_configuration_from_disk():
		"""
		Load the main configuration file into memory.
		"""
		with AppConfig.get_config_file_path().open(mode="rb") as fstream:
			data_dictionary = tomllib.load(fstream)

		get_config_schema().validate(data_dictionary)
		AppConfig.__config_dict = data_dictionary
		return AppConfig.as_dictionary()

	@staticmethod
	def init_config_from_files(path_to_config_file=None):
		"""
		Load from data files if they exist.  Otherwise create new, default files.
		"""
		try:
			if path_to_config_file:
				AppConfig.__config_file = pathlib.Path(path_to_config_file)  # set path to the requested argument
			AppConfig.__read_configuration_from_disk()
			if not AppConfig.as_dictionary():  # If file is empty:
				AppConfig.revert_to_defaults()
			if not AppConfig.as_dictionary():
				raise IOError("Failed to initialize main configuration settings.")
		except FileNotFoundError:
			print("Warning: Could not read configuration file.")
			AppConfig.revert_to_defaults()

		# TODO: After the configuration data is loaded, assign more class variables, such as Logging.
		# if 'log_level' in AppConfig.settings().keys():
		# 	LoggerConfig.set_default_log_level(AppConfig.settings()['log_level'])

	@staticmethod
	def __writeback_to_disk():  # pylint: disable=unused-private-member
		"""
		Write the in-memory configuration data back to disk.
		"""
		#a_logger = make_logger(__name__)
		#a_logger.info("Writing new main configuration (from default template) to disk.")

		with open(AppConfig.get_config_file_path(), "w", encoding="utf-8") as fstream:
			toml.dump(AppConfig.as_dictionary(), fstream)

	@staticmethod
	def revert_to_defaults():
		"""
		Revert main configuration file to default setting.
		"""
		new_file_path = AppConfig.get_config_file_path()
		print(f"Warning: Creating a new, default configuration file: {new_file_path.absolute()}")

		# If necessary, create the parent directories for the configuration file.
		if not new_file_path.parent.exists():
			# print("Creating the parent directories ...")
			# new_file_path.parent.mkdir(parents=True, exist_ok=True)
			# if not new_file_path.parent.is_dir():
			raise FileNotFoundError(f"Error: Configuration file's parent directory '{new_file_path.parent}' does not exist.")

		# 1. Write the default configuration data to disk in TOML format.
		with open(new_file_path, mode="wb") as fstream:
			toml.dump(AppConfig.__default_config_template, fstream)

		# 2. Try to read it back.
		AppConfig.__read_configuration_from_disk()

	@staticmethod
	def get_sql_connection_string():
		"""
		Create a connection string to a Postgres database.
		"""
		if not hasattr(AppConfig, "__sql_connection_string"):
			user = urllib.parse.quote(AppConfig.as_dictionary()["sql_user"])
			password = urllib.parse.quote(AppConfig.as_dictionary()["sql_password"])
			host = AppConfig.as_dictionary()["sql_host"]
			port = AppConfig.as_dictionary()["sql_port"]
			database_name = AppConfig.as_dictionary()["sql_database"]
			AppConfig.__sql_connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
		return AppConfig.__sql_connection_string


#def set_global_loglevel(new_level):
#	logger_level_names = [d['name'] for d in LoggerConfig.list_available_levels()]
#	if new_level not in logger_level_names:
#		raise Exception(f'New logger level {new_level} is not a valid level name.')
#	AppConfig.settings()['log_level'] = new_level
#	AppConfig.writeback_to_file()
#	LoggerConfig.set_default_log_level(new_level)
