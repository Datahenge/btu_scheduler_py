# BTU Scheduler
This is the Python-based alternative to the original 2021 scheduler: https://github.com/Datahenge/btu_scheduler_daemon

### Installing
- Create a new Python virtual environment, and activate it.
- Download the btu-py app:  `git clone https://github.com/Datahenge/btu_scheduler_py.git`
- Install with pip:
  ```bash
  pip install -e .
  # or with development dependencies like linters
  pip install -e ".[development]"
  ```
- Create a configuration file at `~/.config/btu_scheduler/.env` (or set `XDG_CONFIG_HOME` and use `$XDG_CONFIG_HOME/btu_scheduler/.env`):
  ```bash
  mkdir -p ~/.config/btu_scheduler
  cp .env.example ~/.config/btu_scheduler/.env
  # edit values for your environment
  nano ~/.config/btu_scheduler/.env
  ```

Alternatively, pass all settings as process environment variables (e.g. in Docker, Kubernetes, or systemd). See [docs/requirements/01-environment-variables.md](docs/requirements/01-environment-variables.md) for loading precedence.

### Configuration variables

All settings use the `BTU_SCHEDULER_` prefix. Required variables:

| Variable | Description |
|----------|-------------|
| `BTU_SCHEDULER_FULL_REFRESH_INTERNAL_SECS` | Seconds between full queue refills |
| `BTU_SCHEDULER_SCHEDULER_POLLING_INTERVAL` | Seconds between RQ eligibility checks |
| `BTU_SCHEDULER_TIME_ZONE_STRING` | IANA timezone (e.g. `America/New_York`) |
| `BTU_SCHEDULER_TRACING_LEVEL` | Log level (e.g. `INFO`) |
| `BTU_SCHEDULER_SQL_TYPE` | `postgres` or `mariadb` |
| `BTU_SCHEDULER_SQL_HOST` | Database host |
| `BTU_SCHEDULER_SQL_PORT` | Database port |
| `BTU_SCHEDULER_SQL_DATABASE` | Database name |
| `BTU_SCHEDULER_SQL_USER` | Database user |
| `BTU_SCHEDULER_SQL_PASSWORD` | Database password |
| `BTU_SCHEDULER_RQ_HOST` | Redis host |
| `BTU_SCHEDULER_RQ_PORT` | Redis port |
| `BTU_SCHEDULER_TCP_SOCKET_PORT` | TCP listener port |
| `BTU_SCHEDULER_WEBSERVER_IP` | Frappe web server IP |
| `BTU_SCHEDULER_WEBSERVER_PORT` | Frappe web server port |
| `BTU_SCHEDULER_WEBSERVER_TOKEN` | Frappe API token |
| `BTU_SCHEDULER_JOBS_SITE_PREFIX` | Prefix for RQ job identifiers |

Optional variables (with defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `BTU_SCHEDULER_DISABLE_REDIS_RPC` | `false` | Disable Redis RPC listener |
| `BTU_SCHEDULER_DISABLE_TCP_SOCKET` | `false` | Disable TCP socket listener |
| `BTU_SCHEDULER_WEBSERVER_HOST_HEADER` | (unset) | Host header for multi-tenant Frappe |
| `BTU_SCHEDULER_SLACK_WEBHOOK_URL` | (unset) | Slack webhook for notifications |
| `BTU_SCHEDULER_LOGGER_PATH` | `$XDG_STATE_HOME/btu_scheduler/logger.log` | Log file path |

### Running the CLI
```bash
btu-py
btu-py config show    # print current settings (secrets redacted)
btu-py config path    # print path to .env file
btu-py config edit    # open .env in $EDITOR
```

### Running the Daemon
```bash
btu-py run-daemon
```

### Regarding Croniter
https://pypi.org/project/croniter/

No longer maintained by the original authors.  It's now part of Pallets (https://palletsprojects.com)
* https://github.com/pallets-eco/croniter
* https://github.com/pallets-eco/croniter/issues/144
