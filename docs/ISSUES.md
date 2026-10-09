
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

# Tooltip Not Rendering on NYC Taxi Zone Map

## Problem

The pydeck `GeoJsonLayer` on the Streamlit dashboard does not display tooltips when hovering over zones, despite the layer being configured with `pickable=True` and a valid tooltip template.

**Environment / setup:**
- `dashboard/streamlit_app.py`
- Uses `pdk.Layer("GeoJsonLayer", zones, pickable=True, ...)`
- Tooltip configured in `pdk.Deck(...)` as:
  ```python
  tooltip={
      "html": "<b>{properties.zone_name}</b><br/>Avg duration: {properties.avg_duration} mins",
      "style": {"backgroundColor": "steelblue", "color": "white"},
  }
  ```
- Each feature's `properties` dict is enriched before rendering with `zone_name`, `avg_duration`, and `color`.

## Symptoms

- Map renders correctly and **zones are colored** by average duration — so the data loop, `ramp()` function, and `LocationID` key matching all work.
- Hovering produces **no tooltip at all** (not even an empty/grey box).
- No errors in the Streamlit logs when hovering.

## What Has Been Verified Working

- `values` (pickup_locationid → avg_duration_mins) is fully populated for all zones.
- `names` (pickup_locationid → zone name) is fully populated for all zones.
- `values.get(lid)` and `names.get(lid)` are **not** returning `None`.
- Colors on the map confirm the loop is iterating every feature and assigning colors correctly.

## Attempts to Solve (all unsuccessful so far)

1. **Added debug output** — `st.info(values)` and `st.info(names)` confirmed both dicts are populated, ruling out `None` values in the tooltip template.

2. **Disabling autorefresh** — Commented out `st_autorefresh(interval=5000, key="data_refresh")` and hard-reloaded. Tooltip still does not appear.

3. **Replacing `use_container_width=True`** — Changed to `width="stretch"` to silence the deprecation warning and rule out a rendering quirk. Tooltip still does not appear.

4. **Attempted to consult pydeck documentation** — Blocked. The docs site returns **HTTP 503** from the current network. Also tried via Tor; same 503 response.

## Open Questions

- Is the tooltip syntax for `GeoJsonLayer` in the current pydeck version actually `{properties.zone_name}`, or has the interpolation syntax changed?
- Could a version mismatch between `pydeck` (0.9.3), `streamlit` (1.65.0), and the underlying deck.gl release be dropping the tooltip config silently?
- Is there a required `get_tooltip` callback on the layer itself (rather than only the `Deck`-level `tooltip` dict) for `GeoJsonLayer`?
- Does the `auto_highlight` flag interact with tooltip rendering in this version?

## Impact

The dashboard's primary purpose — letting a user identify which pickup zone a color corresponds to and see its average trip duration — is not achievable without the tooltip. The zone data is visible only as color intensity on the choropleth.

