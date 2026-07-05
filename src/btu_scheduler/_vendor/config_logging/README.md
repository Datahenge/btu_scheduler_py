# brian_appkit

Config loading and structured logging bootstrap for Python CLI tools and daemons.

Source: https://github.com/brian-pond/brian_appkit_config_py

---

## Quick start

```python
import sys
from brian_appkit import AppEnv, ConfigurationError, XdgSettings, bootstrap_app
from pydantic import Field

class MySettings(XdgSettings):
    database_url: str = Field(description="Postgres DSN")   # required — no default
    worker_count: int = Field(default=4, ge=1, le=64)
    api_key: str | None = Field(default=None)

def main() -> int:
    try:
        settings, log = bootstrap_app(MySettings, app_name="my-service")
    except ConfigurationError as exc:
        print(exc, file=sys.stderr)
        return 1

    log.info("started", workers=settings.worker_count, env=settings.app_env)
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

`settings` is a fully validated `MySettings` instance. `log` is a structlog
`BoundLogger` already configured and writing to stdout.

---

## Built-in fields

Every app gets these fields for free — no declaration needed in the subclass.

The env var prefix is derived from `app_name`: hyphens become underscores,
uppercased. For `app_name="my-service"` the prefix is `MY_SERVICE_`.

| Field | Type | Default | Env var |
|-------|------|---------|---------|
| `app_env` | `AppEnv` | `"production"` | `MY_SERVICE_APP_ENV` |
| `log_format` | `LogFormat` | auto from `app_env` | `MY_SERVICE_LOG_FORMAT` |
| `log_level` | `LogLevel` | `"INFO"` | `MY_SERVICE_LOG_LEVEL` |

`app_env` values: `development`, `staging`, `production` (default).

When `log_format` is not explicitly set, `bootstrap_app` selects it automatically:
`development` → `text` (colorized); `staging` / `production` → `json`.

---

## Config precedence

Highest wins.

| Priority | Source |
|----------|--------|
| 1 | `bootstrap_app()` kwargs: `bootstrap_app(MySettings, app_name="x", worker_count=8)` |
| 2 | Environment variables: `MY_SERVICE_WORKER_COUNT=8` |
| 3 | `.env` in the current working directory |
| 4 | `~/.config/my-service/.env` (XDG fallback; respects `$XDG_CONFIG_HOME`) |
| 5 | Field defaults on the settings class |

`.env` file format:
```
MY_SERVICE_APP_ENV=development
MY_SERVICE_DATABASE_URL=postgresql://user:pass@localhost/mydb
MY_SERVICE_WORKER_COUNT=8
```

Nested model fields use `__` as the delimiter: `MY_SERVICE_DATABASE__HOST=db.internal`.

---

## XDG directories

Every `XdgSettings` instance exposes four read-only path properties. Directories
are not created automatically — call `.mkdir(parents=True, exist_ok=True)` as needed.

| Property | Typical path | Purpose |
|----------|-------------|---------|
| `settings.config_dir` | `~/.config/<app>/` | User config, `.env` fallback |
| `settings.data_dir` | `~/.local/share/<app>/` | Persistent application state |
| `settings.cache_dir` | `~/.cache/<app>/` | Ephemeral data — safe to delete |
| `settings.runtime_dir` | `/run/user/<uid>/<app>/` | PID files, Unix sockets |

---

## Graceful shutdown

`bootstrap_app()` installs `SIGTERM`/`SIGINT` handlers that log the signal and
set `settings.shutdown_event` (a `threading.Event`).

```python
settings, log = bootstrap_app(MySettings, app_name="my-service")

log.info("started")
while not settings.shutdown_event.is_set():
    do_work()
    settings.shutdown_event.wait(timeout=1.0)

log.info("shutting down")
```

Pass `handle_signals=False` to manage signals yourself.

---

## Logging

- Always stdout. Text or JSON controlled by `log_format`.
- `text` — colorized when attached to a TTY, plain when piped.
- `json` — one object per line; suitable for Loki, ELK, Datadog, CloudWatch, etc.
- Timestamps are UTC ISO 8601.
- Unhandled exceptions are routed through structlog at `CRITICAL` level via a
  custom `sys.excepthook`. `KeyboardInterrupt` uses Python's default handler.

Use `structlog.get_logger(__name__)` in modules to include the component name in
log lines.

---

## API reference

### `bootstrap_app(settings_cls, app_name, dump_config=False, handle_signals=True, **overrides)`

Returns `(settings, log)`. Raises `ConfigurationError` on validation failure.
Does not call `sys.exit()`. Safe to call multiple times (does not mutate `settings_cls`).

| Parameter | Description |
|-----------|-------------|
| `settings_cls` | Your `XdgSettings` subclass (the class, not an instance) |
| `app_name` | Canonical app name — drives env var prefix and XDG paths |
| `dump_config` | Log all effective settings at `DEBUG` level; sensitive field names are redacted |
| `handle_signals` | Install SIGTERM/SIGINT handlers and unhandled-exception hook |
| `**overrides` | Highest-precedence field values |

### `XdgSettings`

Base class for app settings. Subclass this — do not subclass `BaseSettings` directly.

### `ConfigurationError`

Raised by `bootstrap_app()` on missing required fields or failed validation.
The message names each failing field and its corresponding env var.

### `AppEnv` / `LogFormat` / `LogLevel`

```python
class AppEnv(StrEnum):
    DEVELOPMENT = "development"
    STAGING     = "staging"
    PRODUCTION  = "production"

class LogFormat(StrEnum):
    TEXT = "text"
    JSON = "json"

class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO  = "INFO"
    WARNING  = "WARNING"
    ERROR    = "ERROR"
    CRITICAL = "CRITICAL"
```

### `configure_logging(settings)`

Used internally by `bootstrap_app`. Available directly if you need to call it
independently. Accepts any object with `log_format: LogFormat` and
`log_level: LogLevel` attributes.
