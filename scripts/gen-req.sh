#!/bin/bash

# === 1. Find the project root ===.
get_project_root() {
    local dir="$1"
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/pyproject.toml" ] || [ -f "$dir/requirements.txt" ] ; then
            echo "$dir"
            return 0
        fi
        dir="$(dirname "$dir")"
    done
    echo "ERROR: Could not find project root (no pyproject.toml, requirements.txt, or .git found)" >&2
    exit 1
}

# Determine script location and find project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(get_project_root "$SCRIPT_DIR")"

echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT" || exit 1

uv export --no-dev --no-hashes -o requirements.txt
uv export --only-group streamlit --no-hashes -o requirements-streamlit.txt