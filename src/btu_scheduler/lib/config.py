import pathlib
import urllib.parse
from typing import Literal

import structlog
from platformdirs import user_config_dir
from pydantic import Field, field_validator, model_validator

from btu_scheduler._vendor.config_logging import LogLevel, XdgSettings, bootstrap_app

APP_NAME = "btu-scheduler"
_settings: "SchedulerSettings | None" = None


def get_env_file_path() -> pathlib.Path:
	"""Return the path to the optional .env file."""
	return pathlib.Path(user_config_dir(APP_NAME)) / ".env"


class SchedulerSettings(XdgSettings):
	"""Validated BTU Scheduler settings loaded by the vendored bootstrap layer."""

	full_refresh_internal_secs: int
	scheduler_polling_interval: int
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
	tracing_level: LogLevel | None = Field(
		default=None,
		description="Compatibility alias for log_level; prefer BTU_SCHEDULER_LOG_LEVEL.",
	)

	_sql_connection_string: str | None = None

	@field_validator("sql_type", mode="before")
	@classmethod
	def normalize_sql_type(cls, value: str) -> str:
		return value.lower()

	@field_validator("tracing_level", mode="before")
	@classmethod
	def normalize_tracing_level(cls, value: object) -> object:
		if isinstance(value, str):
			return value.upper()
		return value

	@model_validator(mode="after")
	def apply_legacy_tracing_level(self):
		if self.tracing_level is not None and self.log_level is LogLevel.INFO:
			self.log_level = self.tracing_level
		return self

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

def bootstrap_scheduler(*, handle_signals: bool = False) -> tuple[SchedulerSettings, structlog.stdlib.BoundLogger]:
	global _settings  # noqa: PLW0603
	settings, log = bootstrap_app(SchedulerSettings, app_name=APP_NAME, handle_signals=handle_signals)
	_settings = settings
	return settings, log


def load_config() -> SchedulerSettings:
	if _settings is None:
		settings, _log = bootstrap_scheduler(handle_signals=False)
		return settings
	return _settings


def reload_config() -> SchedulerSettings:
	global _settings  # noqa: PLW0603
	_settings = None
	settings, _log = bootstrap_scheduler(handle_signals=False)
	return settings
