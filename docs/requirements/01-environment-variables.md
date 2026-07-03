# Loading environment variables

How BTU Scheduler loads environment variables from `.env` files and how that fits Docker/Kubernetes, development, and PyPI installs.

## Single source: XDG config directory

BTU Scheduler loads a `.env` file from one location only:

- **Path:** `$XDG_CONFIG_HOME/btu_scheduler/.env`, or if `XDG_CONFIG_HOME` is unset, `~/.config/btu_scheduler/.env`.
- The file is optional. If it does not exist, BTU Scheduler uses only the process environment (e.g. variables passed by the shell, systemd, or the container runtime).

Implementation (see `btu_py/lib/config.py`):

```python
from dotenv import load_dotenv
from pathlib import Path
import os

config_home = Path(os.environ.get("XDG_CONFIG_HOME", "~/.config")).expanduser()
load_dotenv(config_home / "btu_scheduler" / ".env", override=False)
```

## Why this approach

- **Docker/Kubernetes:** The directory and file typically do not exist in the image. The container uses only the environment variables passed at startup (e.g. `env` in the pod spec or `docker run -e`). No `.env` file is required.
- **Development:** Create `~/.config/btu_scheduler/.env` (or set `XDG_CONFIG_HOME` and put `.env` in `$XDG_CONFIG_HOME/btu_scheduler/`) and add your `BTU_SCHEDULER*` variables. Works the same for editable installs and PyPI installs.
- **Install-type agnostic:** Does not depend on a package subdirectory or current working directory, so behavior is identical for editable installs, PyPI installs, and system packages.
- **Easy to document:** One path, one file; no ambiguity about which `.env` is used.

## Read exactly once

The `.env` file is read exactly once when `load_config()` runs (at first import of `btu_py.lib.config`). Pydantic-settings is configured with `env_file=None` so it does not load a file; it only reads from `os.environ`, which we have already populated.

## Precedence: environment always wins over the .env file

`load_dotenv` is always called with `override=False` (the library default). This means:

- If a variable is **already set** in `os.environ` (shell, container env, test code), the `.env` file value is ignored.
- If a variable is **not set**, the `.env` file provides the value.

```
os.environ  ← shell / container / test code        (highest priority)
.env file   ← developer convenience defaults        (fills gaps only)
```

**Never call `load_dotenv(override=True)`** unless you specifically intend to let a file beat the process environment. Doing so silently breaks any caller that sets env vars before calling the config loader — including test fixtures that set schema or connection overrides before `reload_config()`.
