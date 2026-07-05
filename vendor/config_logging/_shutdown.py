# VENDORED — part of brian_appkit (github.com/brian-pond/brian_appkit_config_py)
# Placed here by ventwig (github.com/brian-pond/ventwig). Edit at source, not in consumer projects.

import signal
import sys
import threading
from types import TracebackType

import structlog


def _make_excepthook(
    log: structlog.stdlib.BoundLogger,
):
    def excepthook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType | None,
    ) -> None:
        # Let KeyboardInterrupt through unchanged — Ctrl-C is normal user
        # termination, not a programming error worth alerting on.
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        log.critical("unhandled exception", exc_info=(exc_type, exc_value, exc_tb))

    return excepthook


def install_excepthook(log: structlog.stdlib.BoundLogger) -> None:
    """Route unhandled exceptions through structlog instead of stderr."""
    sys.excepthook = _make_excepthook(log)


def install_signal_handlers(
    event: threading.Event, log: structlog.stdlib.BoundLogger
) -> None:
    """Set event and log on SIGTERM or SIGINT so the app can shut down cleanly."""

    def _handler(signum: int, _frame: object) -> None:
        sig_name = signal.Signals(signum).name
        log.info("shutdown signal received", signal=sig_name)
        event.set()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
