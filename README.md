# BTU Scheduler
This is the Python-based alternative to the original 2021 scheduler: https://github.com/Datahenge/btu_scheduler_daemon

### Prerequisites

| Dependency | Minimum version | Notes |
|---|---|---|
| Python | 3.11 | |
| Redis server | **6.2** | See note below |
| MariaDB or PostgreSQL | Frappe-supported version | Frappe's own database |
| Frappe Framework | version-15 | Provides the BTU app and REST API |

> **Redis 6.2 minimum — not 6.0.**
> Frappe v15's official requirement is "Redis 6", but BTU Scheduler uses the
> `ZRANGE ... BYSCORE` Redis command, which was added in Redis **6.2.0** (released
> February 2021). Redis 6.0.x and 6.1.x do not support this command and will
> return an error at runtime. Verify your Redis version with `redis-cli INFO server | grep redis_version`.

### Installing
- Create a new Python virtual environment, and activate it.
- Download the btu-scheduler app:  `git clone https://github.com/Datahenge/btu_scheduler_py.git`
- Install with pip:
  ```bash
  pip install -e .
  # or with development dependencies like linters
  pip install -e ".[development]"
  ```
- Create a configuration file at `~/.config/btu-scheduler/.env` (or set `XDG_CONFIG_HOME` and use `$XDG_CONFIG_HOME/btu-scheduler/.env`):
  ```bash
  mkdir -p ~/.config/btu-scheduler
  cp .env.example ~/.config/btu-scheduler/.env
  # edit values for your environment
  nano ~/.config/btu-scheduler/.env
  ```

Alternatively, pass all settings as process environment variables (e.g. in Docker, Kubernetes, or systemd). See [docs/requirements/01-environment-variables.md](docs/requirements/01-environment-variables.md) for loading precedence and [docs/requirements/02-system-prerequisites.md](docs/requirements/02-system-prerequisites.md) for service version requirements.

### Configuration variables

All settings use the `BTU_SCHEDULER_` prefix. Required variables:

The canonical app name is `btu-scheduler`. The vendored bootstrap derives XDG
paths from that name and normalizes hyphens to underscores for environment
variables.

| Variable | Description |
|----------|-------------|
| `BTU_SCHEDULER_FULL_REFRESH_INTERNAL_SECS` | Seconds between full queue refills |
| `BTU_SCHEDULER_SCHEDULER_POLLING_INTERVAL` | Seconds between RQ eligibility checks |
| `BTU_SCHEDULER_SQL_TYPE` | `postgres` or `mariadb` |
| `BTU_SCHEDULER_SQL_HOST` | Database host |
| `BTU_SCHEDULER_SQL_PORT` | Database port |
| `BTU_SCHEDULER_SQL_DATABASE` | Database name |
| `BTU_SCHEDULER_SQL_USER` | Database user |
| `BTU_SCHEDULER_SQL_PASSWORD` | Database password |
| `BTU_SCHEDULER_RQ_HOST` | Redis host |
| `BTU_SCHEDULER_RQ_PORT` | Redis port |
| `BTU_SCHEDULER_WEBSERVER_IP` | Frappe web server IP |
| `BTU_SCHEDULER_WEBSERVER_PORT` | Frappe web server port |
| `BTU_SCHEDULER_WEBSERVER_TOKEN` | Frappe API token |

Optional variables (with defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `BTU_SCHEDULER_APP_ENV` | `production` | Runtime environment: `development`, `staging`, or `production` |
| `BTU_SCHEDULER_LOG_FORMAT` | auto | `text` in development, `json` otherwise |
| `BTU_SCHEDULER_LOG_LEVEL` | `INFO` | Log level |
| `BTU_SCHEDULER_TRACING_LEVEL` | (unset) | Legacy alias for `BTU_SCHEDULER_LOG_LEVEL` |
| `BTU_SCHEDULER_RQ_PASSWORD` | (unset) | Redis AUTH password — required for non-localhost Redis |
| `BTU_SCHEDULER_DISABLE_REDIS_RPC` | `false` | Disable Redis RPC command listener |
| `BTU_SCHEDULER_WEBSERVER_HOST_HEADER` | (unset) | Host header for multi-tenant Frappe |

### Running the CLI
```bash
btu-scheduler
btu-scheduler config show    # print current settings (secrets redacted)
btu-scheduler config path    # print path to .env file
btu-scheduler config edit    # open .env in $EDITOR
```

### Running the Daemon
```bash
btu-scheduler run-daemon
```

### Development

See [docs/technical/01-ventwig.md](docs/technical/01-ventwig.md) for vendoring notes, [docs/technical/02-dev-commands.md](docs/technical/02-dev-commands.md) for common `uv`, `ruff`, and `ventwig` commands, and [docs/technical/03-architecture-frappe-dependency.md](docs/technical/03-architecture-frappe-dependency.md) for the architectural history of the Frappe web server dependency and the planned fallback path.

### Regarding Croniter
https://pypi.org/project/croniter/

No longer maintained by the original authors.  It's now part of Pallets (https://palletsprojects.com)
* https://github.com/pallets-eco/croniter
* https://github.com/pallets-eco/croniter/issues/144
