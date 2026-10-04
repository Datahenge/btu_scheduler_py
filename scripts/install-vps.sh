#!/usr/bin/env bash
# install-vps.sh — Bootstrap BTU Scheduler onto a fresh VPS.
#
# This script does exactly the one thing a Python CLI can't do for itself: get Python
# tooling and the btu-scheduler package onto the machine. Everything after that —
# service user, environment file, systemd unit, enabling — is `btu install-systemd`'s
# job; this script hands off to it directly once the package is installed.
#
# Usage:
#   sudo ./install-vps.sh [bootstrap options] [-- <args for btu install-systemd>]
#
# Bootstrap options (how to get the btu-scheduler package onto this machine):
#   --source pypi                  pip install btu-scheduler        (not yet published — see docs)
#   --source git --git-ref <ref>   pip install from a GitHub ref    (default ref: main)
#   --source local <path>          pip install from a local checkout
#   --install-dir <dir>            where to create the venv (default: /opt/btu-scheduler)
#
# Anything else — recognized or not — is forwarded as-is to `btu install-systemd`,
# e.g.:
#   sudo ./install-vps.sh --source local . -- --mode webserver --non-interactive
#   sudo ./install-vps.sh --dry-run          # still needs root (this script writes to /opt);
#                                             # --dry-run itself is just forwarded to the CLI
#
# See `btu install-systemd --help` for everything that command accepts.

set -euo pipefail

INSTALL_DIR="/opt/btu-scheduler"
SOURCE="pypi"
GIT_URL="https://github.com/Datahenge/btu_scheduler_py.git"
GIT_REF="main"
LOCAL_PATH=""
FORWARD_ARGS=()

usage() {
	sed -n '2,20p' "$0"
	exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--source)
			SOURCE="$2"
			shift 2
			if [[ "$SOURCE" == "local" ]]; then
				if [[ $# -eq 0 ]]; then
					echo "--source local requires a path, e.g. --source local /path/to/btu_scheduler_py" >&2
					exit 1
				fi
				LOCAL_PATH="$1"
				shift
			fi
			;;
		--git-ref)
			GIT_REF="$2"
			shift 2
			;;
		--install-dir)
			INSTALL_DIR="$2"
			shift 2
			;;
		-h|--help)
			usage 0
			;;
		--)
			shift
			FORWARD_ARGS+=("$@")
			break
			;;
		*)
			# Not one of this script's own flags — pass it straight through to
			# `btu install-systemd` (e.g. --mode, --non-interactive, --dry-run).
			FORWARD_ARGS+=("$1")
			shift
			;;
	esac
done

if [[ "$EUID" -ne 0 ]]; then
	# Root is required unconditionally — this script itself writes to /opt (venv creation,
	# pip install) before it ever gets to forwarding args like --dry-run to 'btu install-systemd'.
	echo "This script must be run as root (it writes to /opt, and hands off to 'btu install-systemd' which needs root too)." >&2
	echo "Re-run with: sudo $0 $*" >&2
	exit 1
fi

# ---- 1. Install uv if missing ----
if ! command -v uv >/dev/null 2>&1; then
	echo "==> Installing uv..."
	curl -LsSf https://astral.sh/uv/install.sh | sh
	export PATH="$HOME/.local/bin:$PATH"
fi
UV_BIN="$(command -v uv)"

# ---- 2. Install btu-scheduler into its own venv ----
echo "==> Installing BTU Scheduler into $INSTALL_DIR (source: $SOURCE)..."
mkdir -p "$INSTALL_DIR"
"$UV_BIN" venv "$INSTALL_DIR/.venv" --python 3.12

case "$SOURCE" in
	pypi)
		"$UV_BIN" pip install --python "$INSTALL_DIR/.venv/bin/python" btu-scheduler
		;;
	git)
		"$UV_BIN" pip install --python "$INSTALL_DIR/.venv/bin/python" "git+${GIT_URL}@${GIT_REF}"
		;;
	local)
		"$UV_BIN" pip install --python "$INSTALL_DIR/.venv/bin/python" "$LOCAL_PATH"
		;;
	*)
		echo "Unknown --source '$SOURCE' (expected pypi, git, or local)." >&2
		exit 1
		;;
esac

# ---- 3. Hand off to the CLI for everything else ----
echo "==> Handing off to 'btu install-systemd'..."
exec "$INSTALL_DIR/.venv/bin/btu" install-systemd "${FORWARD_ARGS[@]}"
