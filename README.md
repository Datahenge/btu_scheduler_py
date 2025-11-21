Python-based alternative to the original 2021 https://github.com/Datahenge/btu_scheduler_daemon



### Regarding Croniter
https://pypi.org/project/croniter/

No longer mainteained by the original authors.  It's now part of Pallets (https://palletsprojects.com)
* https://github.com/pallets-eco/croniter
* https://github.com/pallets-eco/croniter/issues/144


### Installing
* Create a new Python virtual environment, and activate it.
* Download the btu-py app:  `git clone https://github.com/Datahenge/btu_scheduler_py.git`
* Install with pip:
    ```
  pip install -e .
  ```
* Create a new directory in Linux to hold the BTU scheduler files and folders.
    * sudo mkdir  /etc/btu_scheduler/
* sudo chown youruser: /etc/btu_scheduler
* Create a default configuration file:
    * sudo micro /etc/btu_scheduler/btu_scheduler.toml
    * ```
name = "BTU Scheduler Daemon"
environment_name = "PROD"
full_refresh_internal_secs = 30
scheduler_polling_interval=30
time_zone_string="America/New_York"
tracing_level="INFO"
startup_without_database_connections = true
disable_unix_socket = false

# This prefix is added to each RQ Job identifier.
jobs_site_prefix="DNU_does_not_matter"

# SQL Database with the Tasks and Schedules
sql_type = "postgres"
sql_database = "your_frappe_database_name"
sql_schema = "public"
sql_user = "your_postgres_user"
sql_password = "your_postgres_pw"
sql_host = "127.0.0.1"
sql_port = 5432

# Redis Queue
rq_host = "127.0.0.1"
rq_port = 11000

# Other
socket_path = "/run/btu_daemon/btu_scheduler.sock"
socket_file_group_owner = "erp_group"
webserver_ip = "127.0.0.1"
webserver_port = 8000
webserver_host_header = "erp.yourcorp.com"
webserver_token = "token 12345:67890"
```
