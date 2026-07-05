"""btu_py/lib/config.py"""

import os
import pathlib
import pprint
import urllib.parse
from functools import lru_cache
from typing import Literal
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from btu_py.lib.app_logger import build_new_logger

config_home = pathlib.Path(os.environ.get("XDG_CONFIG_HOME", "~/.config")).expanduser()
load_dotenv(config_home / "btu_scheduler" / ".env", override=False)

_SECRET_FIELDS = frozenset({"sql_password", "webserver_token", "slack_webhook_url"})


def get_env_file_path() -> pathlib.Path:
	"""Return the path to the optional .env file."""
	return config_home / "btu_scheduler" / ".env"


class SchedulerSettings(BaseSettings):
	model_config = SettingsConfigDict(
		env_prefix="BTU_SCHEDULER_",
		env_file=None,
		extra="ignore",
	)

	full_refresh_internal_secs: int
	scheduler_polling_interval: int
	time_zone_string: str
	tracing_level: str
	sql_type: Literal["postgres", "mariadb"]
	sql_host: str
	sql_port: int
	sql_database: str
	sql_user: str
	sql_password: str
	rq_host: str
	rq_port: int
	tcp_socket_port: int
	webserver_ip: str
	webserver_port: int
	webserver_token: str
	jobs_site_prefix: str
	disable_redis_rpc: bool = False
	disable_tcp_socket: bool = False
	webserver_host_header: str | None = None
	slack_webhook_url: str | None = None

	_sql_connection_string: str | None = None
	_logger: object | None = None

	@field_validator("sql_type", mode="before")
	@classmethod
	def normalize_sql_type(cls, value: str) -> str:
		return value.lower()

	def get_sql_type(self) -> str:
		return self.sql_type

	def get_sql_connection_string(self) -> str:
		if not self._sql_connection_string:
			user = urllib.parse.quote(self.sql_user)
			password = urllib.parse.quote(self.sql_password)
			if self.sql_type == "postgres":
				self._sql_connection_string = (
					f"postgresql+asyncpg://{user}:{password}@{self.sql_host}:{self.sql_port}/{self.sql_database}"
				)
			elif self.sql_type == "mariadb":
				self._sql_connection_string = (
					f"mysql+asyncmy://{user}:{password}@{self.sql_host}:{self.sql_port}/{self.sql_database}"
				)
			else:
				raise ValueError(f"Unsupported sql_type: {self.sql_type}. Supported types: 'postgres', 'mariadb'")
		return self._sql_connection_string

	def get_logger(self):
		if not self._logger:
			self._logger = build_new_logger("btu_py", self.tracing_level)
		return self._logger

	def timezone(self) -> ZoneInfo:
		return ZoneInfo(self.time_zone_string)

	def as_dictionary(self, *, redact_secrets: bool = False) -> dict:
		data = self.model_dump(mode="json")
		if redact_secrets:
			for key in _SECRET_FIELDS:
				if data.get(key):
					data[key] = "***"
		return data

	def print_config(self):
		print()
		pprint.PrettyPrinter(indent=4, compact=False).pprint(self.as_dictionary(redact_secrets=True))
		print()


@lru_cache(maxsize=1)
def load_config() -> SchedulerSettings:
	return SchedulerSettings()


def reload_config() -> SchedulerSettings:
	load_config.cache_clear()
	return load_config()
