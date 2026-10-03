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
	connectivity_mode: Literal["direct", "webserver"] = "direct"
	sql_type: Literal["postgres", "mariadb"] | None = None
	sql_host: str | None = None
	sql_port: int | None = None
	sql_database: str | None = None
	sql_user: str | None = None
	sql_password: str | None = None
	rq_host: str | None = None
	rq_port: int | None = None
	rq_password: str | None = None
	webserver_ip: str
	webserver_port: int
	webserver_token: str
	webserver_host_header: str | None = None
	tracing_level: LogLevel | None = Field(
		default=None,
		description="Compatibility alias for log_level; prefer BTU_SCHEDULER_LOG_LEVEL.",
	)

	_sql_connection_string: str | None = None

	@field_validator("sql_type", mode="before")
	@classmethod
	def normalize_sql_type(cls, value: str | None) -> str | None:
		if value is None or value == "":
			return None
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

	@model_validator(mode="after")
	def require_fields_for_connectivity_mode(self):
		"""
		In 'direct' mode (the default), BTU talks to SQL and Redis directly, so those
		credentials are required. In 'webserver' mode, BTU only talks to the Frappe web
		server, so SQL/Redis settings are not needed and may be omitted entirely.
		"""
		if self.connectivity_mode == "direct":
			missing = [
				name
				for name in (
					"sql_type",
					"sql_host",
					"sql_port",
					"sql_database",
					"sql_user",
					"sql_password",
					"rq_host",
					"rq_port",
				)
				if getattr(self, name) is None
			]
			if missing:
				env_names = ", ".join(f"BTU_SCHEDULER_{name.upper()}" for name in missing)
				raise ValueError(f"connectivity_mode='direct' requires these settings to be set: {env_names}")
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
