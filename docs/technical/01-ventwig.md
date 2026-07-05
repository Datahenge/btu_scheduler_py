# Ventwig Vendoring

This project uses `ventwig` to vendor upstream Python code that is required at runtime.

## Purpose

Vendored code is code copied from an upstream project into this repository so BTU Scheduler can run without depending on that upstream package being installed separately.

The vendored code is treated as a runtime dependency of BTU Scheduler.

## Layout

BTU Scheduler uses a `src` layout:

```text
src/
  btu_py/
    _vendor/
      config_logging/
```

Vendored Python package code belongs under:

```text
src/btu_py/_vendor/
```

The current `ventwig` source is configured in `pyproject.toml`:

```toml
[[tool.ventwig.sources]]
name          = "brian_appkit"
local_path    = "src/btu_py/_vendor/config_logging"
upstream      = "https://github.com/brian-pond/brian_appkit_config_py.git"
upstream_path = "src/brian_appkit"
ref           = "main"
```

## Package Discovery

This project uses normal setuptools package discovery:

```toml
[tool.setuptools]
package-dir = { "" = "src" }

[tool.setuptools.packages.find]
where = [ "src" ]
include = [ "btu_py*" ]
```

Normal package discovery expects explicit `__init__.py` files in package directories.

For vendored code, this means parent package marker files should exist:

```text
src/btu_py/_vendor/__init__.py
src/btu_py/_vendor/config_logging/__init__.py
```

If `src/btu_py/_vendor/__init__.py` is missing, setuptools normal package discovery may not include the vendored package in the built distribution.

## Runtime Dependencies

Vendored code may still depend on third-party packages.

For example, if vendored code imports `structlog`, then `structlog` must be declared in BTU Scheduler's runtime dependencies unless it is also vendored.

Runtime dependencies belong in:

```toml
[project]
dependencies = [
    ...
]
```

Development-only dependencies belong in:

```toml
[project.optional-dependencies]
development = [
    ...
]
```

## Sync Command

Sync vendored code with:

```bash
uv run --extra development ventwig sync
```

If the development extra is not installed yet:

```bash
uv sync --extra development
```

See [02-dev-commands.md](02-dev-commands.md) for the broader development command list.
