#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

ZIG_BIN="${ZIG:-}"
if [ -z "$ZIG_BIN" ]; then
  ZIG_BIN="$(command -v zig 2>/dev/null || true)"
fi
if [ -z "$ZIG_BIN" ] || [ ! -x "$ZIG_BIN" ]; then
  for candidate in /opt/zig-0.13.0/zig "${HOME:-/tmp}/.local/zig-0.13.0/zig"; do
    if [ -x "$candidate" ]; then ZIG_BIN="$candidate"; break; fi
  done
fi
[ -n "$ZIG_BIN" ] && [ -x "$ZIG_BIN" ] || { echo "zig 0.13.0 is not installed (run install.sh)" >&2; exit 1; }

# libpq-dev puts libpq-fe.h under /usr/include/postgresql on Debian/Ubuntu.
INCLUDE_ARGS=()
[ -d /usr/include/postgresql ] && INCLUDE_ARGS+=("-I/usr/include/postgresql")
"$ZIG_BIN" build-exe src/main.zig -O ReleaseFast -fstrip "${INCLUDE_ARGS[@]}" -lc -lpq -lcrypto -femit-bin=feed-zig
