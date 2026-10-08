#!/usr/bin/env bash
set -euo pipefail

echo "Fixing legacy iptables FORWARD policy (Codespaces)..."
sudo iptables-legacy -P FORWARD ACCEPT
sudo iptables-legacy -I FORWARD 1 -i br-+ -j ACCEPT
sudo iptables-legacy -I FORWARD 1 -o br-+ -j ACCEPT
sudo iptables-legacy -L FORWARD -n | head -1


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

bash ./scripts/gen-req.sh

# permission issues that can cause the dag processor to fail silently
echo "Ensuring logs/ and plugins/ are writable by the container (uid ${AIRFLOW_UID:-50000}:0)..."
mkdir -p logs plugins dags
sudo chown -R "$(id -u):0" logs plugins
sudo chmod -R 775 logs plugins

echo "Running docker compose"
> compose.log
docker compose up --no-color --timestamps 2>&1 | tee compose.log