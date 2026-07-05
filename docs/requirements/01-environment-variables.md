# Loading environment variables

How BTU Scheduler loads environment variables from `.env` files and how that fits Docker/Kubernetes, development, and PyPI installs.

## Sources

BTU Scheduler loads configuration through the vendored bootstrap layer in
`btu_scheduler._vendor.config_logging`.

The normal user config file is:

- **Path:** `$XDG_CONFIG_HOME/btu-scheduler/.env`, or if `XDG_CONFIG_HOME` is unset, `~/.config/btu-scheduler/.env`.

The bootstrap layer also reads a `.env` file in the current working directory.
Both files are optional. If neither exists, BTU Scheduler uses only the process
environment (e.g. variables passed by the shell, systemd, or the container
runtime).

The canonical app name is `btu-scheduler`. XDG paths use that exact name.
Environment variables use the normalized `BTU_SCHEDULER_` prefix because the
bootstrap uppercases the app name and converts hyphens to underscores.

## Why this approach

- **Docker/Kubernetes:** The config files typically do not exist in the image. The container uses only the environment variables passed at startup (e.g. `env` in the pod spec or `docker run -e`). No `.env` file is required.
- **Development:** Create `~/.config/btu-scheduler/.env` (or set `XDG_CONFIG_HOME` and put `.env` in `$XDG_CONFIG_HOME/btu-scheduler/`) and add your `BTU_SCHEDULER*` variables. A project-local `.env` also works for development.
- **Install-type agnostic:** Does not depend on a package subdirectory or current working directory, so behavior is identical for editable installs, PyPI installs, and system packages.
- **Bootstrap-standard:** CLI, daemon, config validation, and logging all use the same settings loader.

## Loading

The settings are read when `load_config()` runs. BTU Scheduler caches that
validated settings object for normal runtime access. Tests and explicit reload
paths can call `reload_config()` to clear the cache and read again.

## Precedence

Highest wins:

1. Explicit settings overrides passed to the bootstrap layer
2. Process environment variables
3. `.env` in the current working directory
4. `$XDG_CONFIG_HOME/btu-scheduler/.env`, or `~/.config/btu-scheduler/.env`
5. Field defaults

Use `BTU_SCHEDULER_LOG_LEVEL` for logging. `BTU_SCHEDULER_TRACING_LEVEL` remains
supported as a compatibility alias.

The daemon configures structured logging through `structlog`: text output in
development, JSON output in staging and production unless `BTU_SCHEDULER_LOG_FORMAT`
is set explicitly.
