
# DAG Processor Unhealthy — Permission Fix

## Symptom

- Airflow standalone shows the DAG processor as **unhealthy**.
- No `/opt/airflow/logs/dag_processor/` directory is ever created.
- `docker logs` shows nothing after the startup banner.
- `AIRFLOW__DAG_PROCESSOR__HEALTH_CHECK_THRESHOLD` has no effect.

## Problem

The DAG processor crashes on startup with a permission error.

The container runs as `airflow` (uid 50000, gid 0), but `/opt/airflow/logs` is owned by `root:root` with mode `755`. The DAG processor cannot create its log directory or file, so logging setup raises before the process starts. The job row is marked `failed` within ~100 ms.

Root cause: **uid mismatch on the bind-mounted `logs/` directory.**

## Solution

**1. `compose.yaml`**

```yaml
services:
  airflow-standalone:
    user: "${AIRFLOW_UID}:0"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
```

**2. Fix host ownership**

```bash
sudo chown -R "$(id -u):0" logs plugins
sudo chmod -R 775 logs plugins
touch logs/.gitkeep plugins/.gitkeep
```

**3. Make it self-healing in `scripts/run-compose.sh`** (after `cd "$PROJECT_ROOT"`)

```bash
mkdir -p logs plugins dags
sudo chown -R "$(id -u):0" logs plugins
sudo chmod -R 775 logs plugins
```

**4. Recreate**

```bash
docker compose down
docker compose up -d
```

**Verify**

```bash
docker exec -i airflow_local_standalone airflow jobs check --job-type DagProcessorJob
# state=running
```