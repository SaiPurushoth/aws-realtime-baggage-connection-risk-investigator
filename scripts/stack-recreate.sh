#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/stack-destroy.sh" "$@"
"$SCRIPT_DIR/stack-create.sh"
