# VENDORED — part of brian_appkit (github.com/brian-pond/brian_appkit_config_py)
# Placed here by ventwig (github.com/brian-pond/ventwig). Edit at source, not in consumer projects.

import logging
import sys
from typing import Protocol

import structlog

from ._types import LogFormat, LogLevel


class _LoggingConfig(Protocol):
    log_format: LogFormat | None
    log_level: LogLevel


def configure_logging(settings: _LoggingConfig) -> None:
    """Call once at startup. Configures structlog to write to stdout."""
    # Clear any cached loggers from a prior configure() call so that
    # reconfiguration (e.g. between tests) takes full effect everywhere.
    structlog.reset_defaults()

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    # Exception rendering is split by format: JSON mode produces a structured
    # dict (type, value, frames) that log aggregators can index and query;
    # text mode produces a human-readable traceback string for the console.
    # None means "auto" — bootstrap_app resolves it before calling us, but
    # callers invoking configure_logging directly get TEXT as a safe fallback.
    log_format = settings.log_format if settings.log_format is not None else LogFormat.TEXT

    if log_format is LogFormat.JSON:
        exc_processor: structlog.types.Processor = structlog.processors.ExceptionRenderer()
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        exc_processor = structlog.processors.format_exc_info
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            exc_processor,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    # Remove any previously installed brian_appkit handler, leave all others
    # (e.g. pytest's LogCaptureHandler, APM SDK handlers) intact.
    root.handlers = [
        h for h in root.handlers
        if not isinstance(getattr(h, "formatter", None), structlog.stdlib.ProcessorFormatter)
    ]
    root.addHandler(handler)
    root.setLevel(settings.log_level.value)
