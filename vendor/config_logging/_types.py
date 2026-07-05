# VENDORED — part of brian_appkit (github.com/brian-pond/brian_appkit_config_py)
# Placed here by ventwig (github.com/brian-pond/ventwig). Edit at source, not in consumer projects.

from enum import StrEnum


class ConfigurationError(Exception):
    """Raised by bootstrap_app() when settings fail pydantic validation.

    The message is pre-formatted for human consumption: field names,
    plain-English descriptions, and the corresponding environment variable
    name are all included so the user knows exactly what to set and where.

    Why ConfigurationError instead of sys.exit()?
    bootstrap_app() is library (vendored) code. Calling sys.exit() from a
    library removes the caller's ability to handle the error — tests would
    die, threads would abort, and larger applications could not recover or
    report cleanly. Raising lets the application boundary decide what to do.
    The example template (examples/foo_main.py) shows the idiomatic pattern:
    catch here, print to stderr, return a non-zero exit code.
    """


class AppEnv(StrEnum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class LogFormat(StrEnum):
    TEXT = "text"
    JSON = "json"


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
