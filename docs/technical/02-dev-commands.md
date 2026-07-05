# Development Commands

This project uses `uv` for Python environments and dependency workflows, `ruff` for linting, and `ventwig` for vendored code.

## Environment

Create or sync the project environment:

```bash
uv sync
```

Include development tools such as `pytest`, `ruff`, `twine`, and `ventwig`:

```bash
uv sync --extra development
```

In restricted environments where `uv` cannot write to its default cache under `~/.cache/uv`, use a workspace-local cache:

```bash
UV_CACHE_DIR=.uv-cache uv sync
```

## Run The CLI

Run the installed console command:

```bash
uv run btu-scheduler
uv run btu
```

Run common CLI commands:

```bash
uv run btu-scheduler config show
uv run btu-scheduler config path
uv run btu-scheduler config edit
```

Run the daemon:

```bash
uv run btu-scheduler run-daemon
```

## Lint

Run `ruff` against the source tree:

```bash
uv run --extra development ruff check src
```

Use a workspace-local cache when needed:

```bash
UV_CACHE_DIR=.uv-cache uv run --extra development ruff check src
```

## Vendored Code

Sync vendored code with `ventwig`:

```bash
uv run --extra development ventwig sync
```

If `ventwig` is not installed in the active environment, make sure the `development` extra is enabled:

```bash
uv sync --extra development
```

Upgrade the locked `ventwig` version:

```bash
uv lock --upgrade-package ventwig
uv sync --extra development
```

## Packaging Checks

Verify that imports resolve from the `src` layout:

```bash
uv run python -c "import btu_scheduler; print(btu_scheduler.__file__)"
```

The expected path should start with:

```text
src/btu_scheduler/
```

Check setuptools package discovery:

```bash
uv run python -c "from setuptools import find_packages; print(find_packages(where='src'))"
```

## Notes

- Runtime package code lives under `src/btu_scheduler/`.
- Vendored runtime code belongs under `src/btu_scheduler/_vendor/`.
- This project uses normal package discovery, so package directories should include explicit `__init__.py` files.
- The `development` optional dependency group is enabled with `uv` using `--extra development`.
