# syntax=docker/dockerfile:1
#
# BTU Scheduler daemon image.
#
# Configuration is entirely environment-variable driven (see
# docs/requirements/01-environment-variables.md) — no .env file is baked in or
# required. Set BTU_SCHEDULER_* vars at `docker run` / Compose / Kubernetes level.
#
# BTU_SCHEDULER_CONNECTIVITY_MODE selects how this container reaches ERPNext:
#   direct     (default) - needs BTU_SCHEDULER_SQL_*  and BTU_SCHEDULER_RQ_* reachable
#                           (e.g. Compose service names if co-located with ERPNext).
#   webserver  - only needs BTU_SCHEDULER_WEBSERVER_* reachable; no SQL/Redis required.
# See docs/technical/04-webserver-only-architecture.md.

# ---- Build stage: resolve and install dependencies with uv ----
FROM python:3.12-slim AS builder

# Official uv binary distribution — see https://docs.astral.sh/uv/guides/integration/docker/
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN uv sync --frozen --no-cache

# ---- Runtime stage ----
FROM python:3.12-slim AS runtime

RUN groupadd --gid 1000 btu \
    && useradd --uid 1000 --gid btu --create-home --shell /usr/sbin/nologin btu

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

USER btu

ENTRYPOINT ["btu"]
CMD ["run-daemon"]
