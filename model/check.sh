#!/usr/bin/env bash
# Translates the PlusCal algorithm and model-checks one configuration.
# Usage: ./check.sh [Config.cfg]   (default: TokenOutbox.cfg)
# Env:   JAVA (default: java, needs Java 11+), TLA2TOOLS (path to tla2tools.jar)
set -euo pipefail
cd "$(dirname "$0")"
JAVA="${JAVA:-java}"
TLA2TOOLS="${TLA2TOOLS:-tla2tools.jar}"
CFG="${1:-TokenOutbox.cfg}"
"$JAVA" -cp "$TLA2TOOLS" pcal.trans -nocfg TokenOutbox.tla > /dev/null
rm -f TokenOutbox.old
"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -deadlock -workers auto \
    -metadir "${TMPDIR:-/tmp}/tlc-states" -config "$CFG" TokenOutbox
