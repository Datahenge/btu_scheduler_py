# BTU Scheduler: standalone Docker Compose example

A working pattern for running the BTU Scheduler daemon beside an existing containerized ERPNext
stack, as its own Compose project:

- **Webserver connectivity mode.** The daemon reaches ERPNext only through the Frappe web server's
  whitelisted BTU endpoints. It needs no SQL or Redis credentials.
- **Private network only.** It joins the ERPNext stack's Docker network as an *external* network and
  calls the web server by service name over plain HTTP (for example `http://backend:8000`), sending the
  site's `Host` header. Nothing goes through a reverse proxy or the public internet.
- **Separate lifecycle.** It can be rebuilt or restarted without touching the ERPNext stack, and the
  ERPNext stack's own tooling never needs to know about it.

## Files

| File | Purpose |
| --- | --- |
| `compose.yml` | The service, the external network, the restart policy and a log cap. |
| `scheduler.settings.env.example` | Non-secret settings: mode, web server name and port, `Host` header, intervals. |
| `scheduler.secret.env.example` | The API token, kept in its own file so it can be `chmod 600`. |

## Set up

1. **The Frappe site.**
   - Install the BTU app and migrate.
   - Create a User for the daemon (for example `btu-scheduler@example.com`) with only the **BTU
     Scheduler** role, and generate its API key and secret.
   - In **BTU Configuration**, set **Run Scheduled Tasks As** to the User scheduled Tasks should run
     as (default Administrator). Scheduled Tasks never run as the daemon's own User.
   - Switch the site to webserver mode: `bench --site <site> set-config btu_scheduler_connectivity_mode webserver`.
2. **A project directory** on the Docker host, for example `~/btu-scheduler`:
   - Copy `compose.yml` here, plus the two `.env.example` files, dropping `.example`.
   - Put the source in `src/`, ideally pinned: `git -C /path/to/btu_scheduler_py archive --format=tar <commit> | tar -x -C src`, and record the commit in `src/COMMIT`.
   - Set `networks.erpnext.name` from `docker network ls`, and the web server service name, port and site in `scheduler.settings.env`.
   - Put `BTU_SCHEDULER_WEBSERVER_TOKEN=token <api_key>:<api_secret>` in `scheduler.secret.env` and run `chmod 600 scheduler.secret.env`.
3. **Start it:** `docker compose up -d --build`, then `docker compose ps` (expect `healthy` after about a minute) and `docker compose logs -f`.

## Operate

- **Health.** The image's `HEALTHCHECK` reports `healthy` while the daemon's scheduling loop keeps
  touching an empty heartbeat file. The loops retry after a failed call (for example while the web
  server restarts), and a fatal error makes the process exit, so `restart: unless-stopped` brings it
  back. Plain Docker Compose shows `unhealthy` but does not restart on it.
- **Liveness in ERPNext.** In webserver mode the daemon cannot answer a ping. **BTU Configuration**
  disables the ping button and shows when the daemon last polled for commands instead.
- **Logs.** The daemon logs to stdout only, as JSON lines. Docker's `json-file` log is capped at
  3 × 10 MB by `compose.yml`. Per-schedule next-run lines are DEBUG; set
  `BTU_SCHEDULER_LOG_LEVEL=DEBUG` to see them.
- **Upgrade.** Replace `src/` with a newer commit (keep the old one for rollback), update `src/COMMIT`,
  and run `docker compose up -d --build`.
