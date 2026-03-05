# app.py
import os
import json
import time
import requests
from uuid import uuid4
from pathlib import Path
from datetime import datetime
import hashlib

from dash import Dash, html, dcc, dash_table, Input, Output, State, no_update, ctx

# =========================================
# .env loader (no external dependencies)
# =========================================
def load_dotenv(path: str = ".env"):
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                val = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        pass

ENV_FILE = os.getenv("ENV_FILE", ".env")
load_dotenv(ENV_FILE)

# =========================================
# Configuration (Environment Variables)
# =========================================
HOST  = os.getenv("DATABRICKS_INSTANCE")
TOKEN = os.getenv("DATABRICKS_TOKEN")
JOBID = os.getenv("DATABRICKS_JOB_ID")

# Optional/used by orchestrator
COMPONENTS_TASK_KEY = os.getenv("COMPONENTS_TASK_KEY", "components_task")

# CRUD Jobs (no SQL Warehouse required)
CRUD_READ_JOB_ID  = os.getenv("CRUD_READ_JOB_ID")    # required for CRUD tab
CRUD_WRITE_JOB_ID = os.getenv("CRUD_WRITE_JOB_ID")   # required for CRUD tab
CRUD_JOB_ID = os.getenv("CRUD_JOB_ID")                   # legacy single-job approach (optional if above two are set)
UC_CATALOG = os.getenv("UC_CATALOG", "price_catalog")
UC_SCHEMA  = os.getenv("UC_SCHEMA", "ercot_metadata")
CRUD_READ_TASK_KEY  = os.getenv("CRUD_READ_TASK_KEY", "ercot_read")
CRUD_WRITE_TASK_KEY = os.getenv("CRUD_WRITE_TASK_KEY", "ercot_write")
AUDIT_TABLE_FQN = os.getenv("AUDIT_TABLE_FQN", f"{UC_CATALOG}.{UC_SCHEMA}.crud_audit_log")

# Tables exposed in CRUD tab (short names or FQN)
CRUD_TABLES = [s.strip() for s in os.getenv(
    "CRUD_TABLES",
    "meter_master,cost_component_mapping,product_definitions"
).split(",") if s.strip()]

# Per-table PK & editable columns
TABLE_CONFIG = {
    "meter_master": {
        "pk": "esi_id",
        "editable_cols": "ALL",   # all except PK
    },
    "cost_component_mapping": {
        "pk": "id",               # ensure this exists; otherwise UI becomes read-only
        "editable_cols": [
            "cost_level", "cost_type", "load_zone", "resource_node",
            "price_formula", "formula_type", "description"
        ],
    },
    "product_definitions": {
        "pk": "id",
        "editable_cols": "ALL",
    },
}

# --------------------------
# Simple file-based cache
# --------------------------
CACHE_DIR = Path(os.getenv("CACHE_DIR", ".cache"))
RUNS_DIR = CACHE_DIR / "runs"
INDEX_FILE = CACHE_DIR / "index.json"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

def as_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, str):  return v.lower() == "true"
    return False

def _now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _param_key(esi_text, asof, fdate, tdate, compute_delta_flag):
    raw = f"{(esi_text or '').strip()}|{(asof or '').strip()}|{(fdate or '').strip()}|{(tdate or '').strip()}|{compute_delta_flag}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def _read_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}

def _write_json(path, payload):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

def cache_save_run(run_id, batch, components, submitted_params):
    blob = {
        "run_id": run_id,
        "ts": _now_iso(),
        "batch": batch or {},
        "components": components or {},
        "submitted_params": submitted_params or {},
    }
    _write_json(RUNS_DIR / f"{run_id}.json", blob)

    idx = _read_json(INDEX_FILE, default={"runs": [], "by_param": {}})
    label = (submitted_params or {}).get("esi_ids", "")[:60]
    pk = _param_key(
        (submitted_params or {}).get("esi_ids"),
        (submitted_params or {}).get("as_of_date"),
        (submitted_params or {}).get("from_date"),
        (submitted_params or {}).get("to_date"),
        (submitted_params or {}).get("compute_delta"),
    )
    status = (batch or {}).get("status")

    idx["runs"] = [r for r in idx.get("runs", []) if r.get("run_id") != run_id]
    idx["runs"].insert(0, {
        "run_id": run_id,
        "ts": blob["ts"],
        "label": label,
        "param_key": pk,
        "status": status,
    })
    if pk:
        idx.setdefault("by_param", {})[pk] = run_id
    _write_json(INDEX_FILE, idx)

def cache_list_runs(limit=50):
    idx = _read_json(INDEX_FILE, default={"runs": []})
    return idx.get("runs", [])[:limit]

def cache_get_by_run_id(run_id):
    return _read_json(RUNS_DIR / f"{run_id}.json", default=None)

def cache_get_by_params(esi_text, asof, fdate, tdate, compute_delta_flag):
    pk = _param_key(esi_text, asof, fdate, tdate, compute_delta_flag)
    idx = _read_json(INDEX_FILE, default={"by_param": {}})
    rid = idx.get("by_param", {}).get(pk)
    if not rid:
        return None
    return cache_get_by_run_id(rid)

# =========================================
# Databricks REST Helpers (Jobs API v2.1)
# =========================================
def _require_env(keys):
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

def run_orchestrator(esi_ids, as_of_date, from_date, to_date, compute_delta_flag):
    _require_env(["DATABRICKS_INSTANCE", "DATABRICKS_TOKEN", "DATABRICKS_JOB_ID"])
    url = f"{HOST}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    payload = {
        "job_id": int(JOBID),
        "notebook_params": {
            "esi_ids": esi_ids,
            "as_of_date": as_of_date,
            "from_date": from_date,
            "to_date": to_date,
            "compute_delta": compute_delta_flag,  # legacy param to your current orchestrator
        },
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["run_id"]

def get_run_state(run_id):
    _require_env(["DATABRICKS_INSTANCE", "DATABRICKS_TOKEN"])
    url = f"{HOST}/api/2.1/jobs/runs/get"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers=headers, params={"run_id": run_id}, timeout=30)
    r.raise_for_status()
    return r.json()

def get_run_result(run_id, task_key=None):
    _require_env(["DATABRICKS_INSTANCE", "DATABRICKS_TOKEN"])
    headers = {"Authorization": f"Bearer {TOKEN}"}

    def _get_output(rid: int):
        url = f"{HOST}/api/2.1/jobs/runs/get-output"
        r = requests.get(url, headers=headers, params={"run_id": rid}, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(
                f"runs/get-output failed for run_id={rid}: {r.status_code} {r.reason} — {r.text}"
            )
        payload = r.json()
        raw = payload.get("notebook_output", {}).get("result")
        if not raw:
            raise RuntimeError(f"No notebook_output.result for run_id={rid}")
        return json.loads(raw)

    try:
        if task_key is None:
            return _get_output(run_id)
    except RuntimeError as e:
        if "runs/get-output failed" not in str(e):
            raise

    get_url = f"{HOST}/api/2.1/jobs/runs/get"
    g = requests.get(get_url, headers=headers, params={"run_id": run_id}, timeout=60)
    g.raise_for_status()
    run_info = g.json()
    tasks = run_info.get("tasks") or []
    if not tasks:
        raise RuntimeError("No tasks found for this run; is this a multi-task job?")

    selected = None
    if task_key:
        for t in tasks:
            if t.get("task_key") == task_key:
                selected = t
                break
    if not selected:
        for t in tasks:
            if t.get("run_id"):
                selected = t
                break
    if not selected or not selected.get("run_id"):
        raise RuntimeError("Could not resolve a child run_id to fetch output.")
    return _get_output(selected["run_id"])

def _ensure_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

# =========================================
# CRUD via Databricks SQL Connector (SQL Warehouse)
# =========================================
from databricks import sql as dbsql

# Required: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
SQL_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
SQL_PATH = os.getenv("DATABRICKS_HTTP_PATH")
SQL_TOKEN = os.getenv("DATABRICKS_TOKEN")

def _require_sql_env():
    missing = []
    if not SQL_HOST:  missing.append("DATABRICKS_SERVER_HOSTNAME")
    if not SQL_PATH:  missing.append("DATABRICKS_HTTP_PATH")
    if not SQL_TOKEN: missing.append("DATABRICKS_TOKEN")
    if missing:
        raise RuntimeError(f"Missing SQL Warehouse env vars: {', '.join(missing)}")

def get_sql_connection():
    """Open a short-lived SQL connection (context-managed)."""
    _require_sql_env()
    return dbsql.connect(
        server_hostname=SQL_HOST,
        http_path=SQL_PATH,
        access_token=SQL_TOKEN,
        # Optional defaults — uncomment if you want to bind catalog/schema here:
        # catalog=UC_CATALOG,
        # schema=UC_SCHEMA,
    )

def _execute(query: str, params: dict | list | tuple | None = None, fetch: bool = True):
    """
    Execute parameterized SQL with native parameters (supported by the connector v3+).
    Returns (columns, rows_as_dicts) if fetch=True else (None, None).
    """
    with get_sql_connection() as conn, conn.cursor() as cur:
        cur.execute(query, params or {})
        if not fetch:
            return None, None
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall() or []
        # rows are Row objects (name-access or index-access). Convert to dicts:
        out = []
        for r in rows:
            obj = {}
            for i, c in enumerate(cols):
                # Row supports index access and getattr by alias; index is safest
                obj[c] = r[i]
            out.append(obj)
        return cols, out

def _fqn(tbl: str) -> str:
    if "." in tbl:
        return tbl
    return f"{UC_CATALOG}.{UC_SCHEMA}.{tbl}"

def _get_table_columns(table_fqn: str) -> list[str]:
    """
    Infer column order from an empty SELECT; works across Unity Catalog.
    """
    q = f"SELECT * FROM {table_fqn} LIMIT 0"
    cols, _ = _execute(q, fetch=True)
    return cols

def crud_read_table(table_name: str, limit=500, columns=None, where=None):
    """
    Read rows from table using the SQL connector.
    Returns (columns, data_rows_as_list_of_dicts).
    """
    table_fqn = _fqn(table_name)
    cols = columns or _get_table_columns(table_fqn)
    if not cols:
        raise RuntimeError(f"Could not resolve columns for {table_fqn}")

    where = where or {}
    predicates = []
    params = {}
    for i, (k, v) in enumerate(where.items()):
        ph = f"p{i}"
        predicates.append(f"{k} = :{ph}")
        params[ph] = v
    where_sql = (" WHERE " + " AND ".join(predicates)) if predicates else ""
    q = f"SELECT {', '.join(cols)} FROM {table_fqn}{where_sql} LIMIT {int(limit)}"
    _, data = _execute(q, params, fetch=True)
    return cols, data

def crud_history_table(table_name: str):
    """
    Delta history via DESCRIBE HISTORY (most recent first).
    Returns dict with status + history list for your UI.
    """
    table_fqn = _fqn(table_name)
    q = f"DESCRIBE HISTORY {table_fqn}"
    cols, rows = _execute(q, fetch=True)
    # Keep the raw rows; your UI just previews the first N as JSON
    return {"status": "SUCCESS", "history": rows, "columns": cols}

def crud_row_history(table_name: str, pk_col: str, pk_val: str | int, max_versions=20):
    """
    Build a simple per-row timeline across recent table versions:
    1) Get last N versions from DESCRIBE HISTORY
    2) For each version, try to select the row snapshot via VERSION AS OF <v>
    """
    table_fqn = _fqn(table_name)
    # 1) fetch version metadata
    hist_q = f"DESCRIBE HISTORY {table_fqn}"
    _, hist = _execute(hist_q, fetch=True)
    # Most recent first — clip to requested window
    versions = [h.get("version") for h in hist if "version" in h]
    versions = [v for v in versions if isinstance(v, int)]
    versions = versions[: int(max_versions)]

    timeline = []
    # Build a tiny map version->operation if available
    vers2op = {h.get("version"): h.get("operation") for h in hist if "version" in h}

    for v in versions:
        snap_q = f"SELECT * FROM {table_fqn} VERSION AS OF {v} WHERE {pk_col} = :pk LIMIT 1"
        _, rows = _execute(snap_q, params={"pk": pk_val}, fetch=True)
        if rows:
            timeline.append({
                "version": v,
                "operation": vers2op.get(v),
                "row": rows[0],
            })
        else:
            # If row does not exist in that version, still show a tombstone entry
            timeline.append({
                "version": v,
                "operation": vers2op.get(v),
                "row": None
            })

    return {"status": "SUCCESS", "timeline": timeline}

def _insert_rows(table_fqn: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    count = 0
    with get_sql_connection() as conn, conn.cursor() as cur:
        for r in rows:
            cols = list(r.keys())
            vals = [r[c] for c in cols]
            placeholders = ", ".join([f":v{i}" for i in range(len(vals))])
            params = {f"v{i}": v for i, v in enumerate(vals)}
            q = f"INSERT INTO {table_fqn} ({', '.join(cols)}) VALUES ({placeholders})"
            cur.execute(q, params)
            count += 1
    return count

def _update_rows(table_fqn: str, pk: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    count = 0
    with get_sql_connection() as conn, conn.cursor() as cur:
        for r in rows:
            if pk not in r:
                continue
            set_cols = [c for c in r.keys() if c != pk]
            if not set_cols:
                continue
            set_sql = ", ".join([f"{c} = :{c}" for c in set_cols])
            params = {c: r.get(c) for c in set_cols}
            params["pk"] = r.get(pk)
            q = f"UPDATE {table_fqn} SET {set_sql} WHERE {pk} = :pk"
            cur.execute(q, params)
            count += 1
    return count

def _delete_rows(table_fqn: str, pk: str, pk_values: list) -> int:
    if not pk_values:
        return 0
    # Chunk deletes to keep parameter lists reasonable
    CHUNK = 500
    total = 0
    with get_sql_connection() as conn, conn.cursor() as cur:
        for i in range(0, len(pk_values), CHUNK):
            chunk = pk_values[i:i+CHUNK]
            placeholders = ", ".join([f":p{j}" for j in range(len(chunk))])
            params = {f"p{j}": v for j, v in enumerate(chunk)}
            q = f"DELETE FROM {table_fqn} WHERE {pk} IN ({placeholders})"
            cur.execute(q, params)
            total += len(chunk)
    return total

def _log_audit(action: str, table_fqn: str, details: dict):
    """
    Optional lightweight audit log into AUDIT_TABLE_FQN if configured.
    Expect a schema like (event_time TIMESTAMP, action STRING, table STRING, details STRING).
    """
    if not AUDIT_TABLE_FQN:
        return
    try:
        q = f"INSERT INTO {AUDIT_TABLE_FQN} (event_time, action, details) VALUES (current_timestamp(), :act, :det)"
        _execute(q, {"act": f"{action}:{table_fqn}", "det": json.dumps(details, ensure_ascii=False)}, fetch=False)
    except Exception:
        # Don't block the main flow if audit insert fails
        pass

def _current_table_version(table_fqn: str) -> int | None:
    try:
        _, hist = _execute(f"DESCRIBE HISTORY {table_fqn}", fetch=True)
        if hist and isinstance(hist[0].get("version"), int):
            return hist[0]["version"]
    except Exception:
        return None
    return None

def crud_apply_changes(table_name: str, pk: str, inserts, updates, deletes, versioning_mode="delta_audit"):
    """
    Apply inserts/updates/deletes via SQL connector (native parameters).
    versioning_mode:
      - "delta_audit": normal DML + optional audit table write.
      - "scd2": treated the same as delta_audit here unless your table has explicit SCD2 columns.
    """
    table_fqn = _fqn(table_name)
    inserts = list(inserts or [])
    updates = list(updates or [])
    deletes = list(deletes or [])

    ins_count = _insert_rows(table_fqn, inserts)
    upd_count = _update_rows(table_fqn, pk, updates)
    del_count = _delete_rows(table_fqn, pk, deletes)

    _log_audit("APPLY_CHANGES", table_fqn, {
        "pk": pk, "inserted": ins_count, "updated": upd_count, "deleted": del_count
    })

    ver = _current_table_version(table_fqn)
    return {"status": "SUCCESS", "inserted": ins_count, "updated": upd_count, "deleted": del_count, "version": ver}

def crud_revert(table_name: str, pk_col: str, pk_vals: list, version: int):
    """
    Revert selected PKs to the state in a given Delta version:
      - For each PK, read the snapshot row at VERSION AS OF <version>
      - If found, DELETE current row for that PK and INSERT the snapshot row
    """
    table_fqn = _fqn(table_name)
    cols = _get_table_columns(table_fqn)
    if not cols:
        raise RuntimeError(f"Could not resolve columns for {table_fqn}")

    reverted = 0
    with get_sql_connection() as conn, conn.cursor() as cur:
        for val in pk_vals or []:
            # Snapshot row
            snap_q = f"SELECT * FROM {table_fqn} VERSION AS OF {int(version)} WHERE {pk_col} = :pk LIMIT 1"
            cur.execute(snap_q, {"pk": val})
            snap = cur.fetchall()
            if not snap:
                continue
            row = snap[0]
            # DELETE current
            cur.execute(f"DELETE FROM {table_fqn} WHERE {pk_col} = :pk", {"pk": val})
            # INSERT snapshot (preserve column order)
            placeholders = ", ".join([f":c{i}" for i in range(len(cols))])
            params = {f"c{i}": row[i] for i in range(len(cols))}
            ins_q = f"INSERT INTO {table_fqn} ({', '.join(cols)}) VALUES ({placeholders})"
            cur.execute(ins_q, params)
            reverted += 1

    _log_audit("REVERT", table_fqn, {"pk": pk_col, "count": reverted, "to_version": version})
    ver = _current_table_version(table_fqn)
    return {"status": "SUCCESS", "reverted": reverted, "version": ver}


# =========================================
# Dash App
# =========================================
app = Dash(__name__, title="Ercot Price C&I Breakdown")

# ---------- Pricing Tab content (existing UI) ----------
pricing_surface = html.Div(
    className="surface",
    children=[
        html.Div(
            className="header",
            children=[
                html.H4("Ercot C&I Pricing"),
                html.Div("Submit parameters and retrieve final price breakdown", className="subtle"),
                # FORM
                html.Div(
                    className="form-grid",
                    children=[
                        html.Div(
                            className="form-item col-12",
                            children=[
                                html.Label("ESI IDs (comma-separated OR JSON array)"),
                                dcc.Input(id="orch-esi", type="text", debounce=True, style={"width": "100%"}),
                            ],
                        ),
                        html.Div(
                            className="form-item col-12",
                            children=[
                                html.Div(
                                    style={"display": "flex", "gap": "28px", "marginTop": "12px", "flexWrap": "wrap"},
                                    children=[
                                        html.Div(
                                            style={"flex": "1", "minWidth": "220px"},
                                            children=[
                                                html.Label("As-of Date (YYYY-MM-DD)"),
                                                dcc.Input(id="orch-asof", type="text", debounce=True, style={"width": "100%"}),
                                            ],
                                        ),
                                        html.Div(
                                            style={"flex": "1", "minWidth": "220px"},
                                            children=[
                                                html.Label("From Delivery Date (YYYY-MM-DD)"),
                                                dcc.Input(id="orch-from", type="text", debounce=True, style={"width": "100%"}),
                                            ],
                                        ),
                                        html.Div(
                                            style={"flex": "1", "minWidth": "220px"},
                                            children=[
                                                html.Label("To Delivery Date (YYYY-MM-DD)"),
                                                dcc.Input(id="orch-to", type="text", debounce=True, style={"width": "100%"}),
                                            ],
                                        ),
                                        html.Div(
                                            className="form-item col-12 mt-12",
                                            children=[
                                                dcc.Checklist(
                                                    id="orch-compute-delta",
                                                    options=[{"label": "Compute Delta Exposure", "value": "true"}],
                                                    value=[],
                                                    inputStyle={"margin-right": "8px"},
                                                )
                                            ],
                                        ),
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
                # Submit
                html.Div(
                    className="mt-12",
                    children=[
                        html.Button("Submit", id="orch-submit", className="btn"),
                        html.Div(id="orch-status", className="status mt-12"),
                    ],
                ),
                # ESI Dropdown + Previous
                html.Div(
                    className="form-item col-12 mt-12",
                    children=[
                        html.Label("Select ESI ID to view details"),
                        dcc.Dropdown(id="esi-select", options=[], value=None, placeholder="Select an ESI ID", clearable=False),
                    ],
                ),
                html.Div(
                    className="form-item col-12 mt-12",
                    children=[
                        html.Label("Previous Runs"),
                        dcc.Dropdown(id="previous-runs", options=[], value=None, placeholder="Select a previous run", clearable=True),
                    ],
                ),
                # Results
                dcc.Loading(
                    id="orch-loading",
                    type="default",
                    children=[
                        html.Div(id="orch-final-card", className="mt-16"),
                        html.Div(id="orch-meta", className="mt-12"),
                        dash_table.DataTable(
                            id="orch-components",
                            columns=[], data=[], page_size=15, sort_action="native", filter_action="native",
                            style_table={"maxHeight": "60vh", "overflowY": "auto"},
                            style_cell={"textAlign": "center", "fontFamily": "Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', Arial, sans-serif", "fontSize": "13px", "padding": "8px 10px"},
                        ),
                    ],
                ),
                # Stores & interval
                dcc.Store(id="orch-runid"),
                dcc.Store(id="orch-batch"),
                dcc.Store(id="components-store"),
                dcc.Interval(id="orch-poller", interval=1500, disabled=True),
            ],
        )
    ],
)

# ---------- CRUD Tab content ----------
crud_surface = html.Div(
    className="surface",
    children=[
        html.Div("Admin — Tables (CRUD)", className="subtle"),
        html.Div(
            style={"display":"flex","gap":"12px","alignItems":"end","flexWrap":"wrap"},
            children=[
                html.Div([
                    html.Label("Table"),
                    dcc.Dropdown(
                        id="crud-table",
                        options=[{"label": t, "value": t} for t in CRUD_TABLES],
                        placeholder="Select table",
                        clearable=False,
                        style={"minWidth":"280px"},
                    ),
                ]),
                html.Div([
                    html.Label("Versioning Mode"),
                    dcc.Dropdown(
                        id="crud-versioning",
                        options=[
                            {"label":"Delta Audit (default)","value":"delta_audit"},
                            {"label":"SCD Type 2","value":"scd2"},
                        ],
                        value="delta_audit",
                        clearable=False,
                        style={"minWidth":"220px"},
                    ),
                ]),
                html.Div([
                    html.Label("Revert Version"),
                    dcc.Input(id="crud-revert-ver", type="number", placeholder="e.g. 17", style={"minWidth":"120px"}),
                ]),
                html.Button("Refresh", id="crud-refresh", className="btn"),
                html.Button("Add Row", id="crud-add", className="btn"),
                html.Button("Delete Selected", id="crud-del", className="btn"),
                html.Button("Save Changes", id="crud-save", className="btn btn-primary"),
                html.Button("History (table)", id="crud-history-table", className="btn"),
                html.Button("History (row)", id="crud-history-row", className="btn"),
                html.Button("Revert Selected to Version", id="crud-revert", className="btn"),
                html.Div(id="crud-toast", className="status", style={"marginLeft":"12px"}),
            ],
        ),
        dash_table.DataTable(
            id="crud-grid",
            columns=[],
            data=[],
            page_size=15,
            editable=True,
            row_selectable="multi",
            cell_selectable=True,
            style_table={"maxHeight":"65vh","overflowY":"auto","marginTop":"10px"},
            style_cell={"textAlign":"left","fontSize":"13px","padding":"6px 8px"},
        ),
        html.Pre(id="crud-history-pre", style={"whiteSpace":"pre-wrap","maxHeight":"30vh","overflowY":"auto","marginTop":"12px"}),
        # Stores
        dcc.Store(id="crud-original"),
        dcc.Store(id="crud-schema"),
        dcc.Store(id="crud-config"),
        dcc.Store(id="crud-deletes"),
        dcc.Store(id="crud-history-data"),
        dcc.Store(id="crud-row-history"),
    ]
)

# ---------- Overall layout with Tabs ----------
app.layout = html.Div(
    className="app-root",
    children=[
        dcc.Tabs(
            id="main-tabs",
            value="tab-pricing",
            children=[
                dcc.Tab(label="Pricing", value="tab-pricing", children=[pricing_surface]),
                dcc.Tab(label="View and Edit Tables", value="tab-crud", children=[crud_surface]),
            ],
        )
    ],
)
# =========================================
# Controller: submit & poll (Pricing tab)
# =========================================
@app.callback(
    Output("orch-runid", "data"),
    Output("orch-status", "children"),
    Output("orch-poller", "disabled"),
    Output("orch-batch", "data"),
    Output("components-store", "data"),
    Output("orch-submit", "disabled"),
    Input("orch-submit", "n_clicks"),
    Input("orch-poller", "n_intervals"),
    Input("previous-runs", "value"),
    State("orch-runid", "data"),
    State("orch-esi", "value"),
    State("orch-asof", "value"),
    State("orch-from", "value"),
    State("orch-to", "value"),
    State("orch-compute-delta", "value"),
    prevent_initial_call=True,
)
def orchestrator_controller(
    n_clicks,
    n_intervals,
    selected_previous_run,
    current_run_id,
    esi_text,
    asof,
    fdate,
    tdate,
    compute_delta_value,
):
    trigger = ctx.triggered_id
    # ---- previous run
    if trigger == "previous-runs":
        if not selected_previous_run:
            return no_update, no_update, True, no_update, no_update, False
        cached = cache_get_by_run_id(selected_previous_run)
        if not cached:
            return no_update, "Cached run not found.", True, no_update, no_update, False
        return (
            cached.get("run_id"),
            f"Loaded cached run {selected_previous_run}",
            True,
            cached.get("batch"),
            cached.get("components"),
            False,
        )
    # ---- submit
    if trigger == "orch-submit":
        if not all([esi_text, asof, fdate, tdate]):
            return None, "All four parameters are required.", True, no_update, no_update, False
        esi_text = esi_text.strip()
        asof = asof.strip()
        fdate = fdate.strip()
        tdate = tdate.strip()
        compute_delta_flag = True if "true" in (compute_delta_value or []) else False

        cached = cache_get_by_params(esi_text, asof, fdate, tdate, compute_delta_flag)
        if cached:
            return (
                cached.get("run_id"),
                f"Loaded from cache (Run {cached.get('run_id')})",
                True,
                cached.get("batch"),
                cached.get("components"),
                False,
            )
        try:
            run_id = run_orchestrator(esi_text, asof, fdate, tdate, compute_delta_flag)
            return run_id, f"Submitted. Run ID: {run_id}", False, None, None, True
        except Exception as e:
            return None, f"Submission failed: {e}", True, no_update, no_update, False
    # ---- poller
    if trigger == "orch-poller":
        if not current_run_id:
            return no_update, no_update, True, no_update, no_update, False
        compute_delta_flag = True if "true" in (compute_delta_value or []) else False
        try:
            st = get_run_state(current_run_id)
            life = st["state"]["life_cycle_state"]
            result_state = st["state"].get("result_state")
            if life in ("PENDING", "QUEUED", "RUNNING", "BLOCKED", "TERMINATING"):
                return (current_run_id, f"Run {current_run_id} is {life}...", False, no_update, no_update, True)
            if life == "TERMINATED":
                if result_state == "SUCCESS":
                    batch = get_run_result(current_run_id, task_key=os.getenv("ORCHESTRATOR_TASK_KEY"))
                    components_map = None
                    try:
                        comp_out = get_run_result(current_run_id, task_key=COMPONENTS_TASK_KEY)
                        if isinstance(comp_out, dict) and comp_out.get("data_by_esi"):
                            raw = comp_out.get("data_by_esi")
                            components_map = {str(k): v for k, v in raw.items()}
                    except Exception:
                        components_map = None

                    submitted_params = {
                        "esi_ids": esi_text,
                        "as_of_date": asof,
                        "from_date": fdate,
                        "to_date": tdate,
                        "compute_delta": compute_delta_flag,
                    }
                    cache_save_run(current_run_id, batch, components_map, submitted_params)
                    return (current_run_id, f"Run {current_run_id} completed successfully.", True, batch, components_map, False)

                error_message = (
                    st.get("state", {}).get("state_message")
                    or st.get("state", {}).get("result_state")
                    or "Unknown failure"
                )
                return (current_run_id, f"Run {current_run_id} FAILED: {error_message}", True, no_update, no_update, False)

            return (current_run_id, f"Run {current_run_id} state: {life}", False, no_update, no_update, True)
        except Exception as e:
            return current_run_id, f"Polling error: {e}", True, no_update, no_update, False

    return no_update, no_update, True, no_update, no_update, False

# =========================================
# Helpers (Pricing tab)
# =========================================
def _extract_results_list(batch: dict):
    if isinstance(batch, dict) and isinstance(batch.get("results"), list):
        return batch["results"]
    return []

def _pick_result(batch: dict, esi_value: str):
    for r in _extract_results_list(batch):
        if (r.get("esi_id") or r.get("esiid")) == esi_value:
            return r
    return None

# ESI selector populate
@app.callback(
    Output("esi-select", "options"),
    Output("esi-select", "value"),
    Input("orch-batch", "data"),
    Input("components-store", "data"),
    prevent_initial_call=True,
)
def populate_selector(batch, components_map):
    batch = _ensure_dict(batch)
    components_map = _ensure_dict(components_map)
    options = []
    if isinstance(components_map, dict) and components_map:
        for esi in sorted(components_map.keys(), key=lambda x: str(x)):
            esi_str = str(esi)
            options.append({"label": esi_str, "value": esi_str})
        return options, (options[0]["value"] if options else None)
    if batch and isinstance(batch.get("results"), list):
        for r in batch["results"]:
            eid = r.get("esi_id") or r.get("esiid")
            if eid is None:
                continue
            eid_str = str(eid)
            label = f"{eid_str} — {r.get('zone_id','')} / {r.get('node_name','')}".strip(" — /")
            options.append({"label": label, "value": eid_str})
        return options, (options[0]["value"] if options else None)
    return [], None

# Previous runs dropdown
@app.callback(
    Output("previous-runs", "options"),
    Input("orch-status", "children"),
)
def populate_previous_runs(_):
    runs = cache_list_runs(limit=25)
    options = []
    for r in runs:
        label = f"{r.get('ts')} — Run {r.get('run_id')} — {r.get('label','')}"
        options.append({"label": label, "value": r.get("run_id")})
    return options

# Render selection
@app.callback(
    Output("orch-final-card", "children"),
    Output("orch-meta", "children"),
    Output("orch-components", "columns"),
    Output("orch-components", "data"),
    Input("esi-select", "value"),
    Input("orch-batch", "data"),
    Input("components-store", "data"),
    prevent_initial_call=True,
)
def render_selected(selected_esi, batch, components_map):
    batch = _ensure_dict(batch)
    components_map = _ensure_dict(components_map)
    is_delta = as_bool(batch.get("compute_delta"))
    if not batch or batch.get("status") not in ("SUCCESS", "ENGINE_BAD_RESPONSE", "ENGINE_FAILURE", "PARTIAL"):
        return no_update, no_update, [], []
    if not selected_esi:
        return no_update, no_update, [], []

    def _fmt(v, default="—"):
        return default if v is None or (isinstance(v, str) and v.strip() == "") else v
    def _safe_len(x):
        try:
            return len(x) if x is not None else None
        except Exception:
            return None
    def _infer_zone_from_components(esi_id: str, comps: dict):
        try:
            rows = (comps or {}).get(str(esi_id)) or (comps or {}).get(esi_id) or []
            if not rows:
                return None
            cn = rows[0].get("curve_name") or ""
            if "__" in cn:
                maybe = cn.split("__")[-1].strip()
                if maybe:
                    return maybe
        except Exception:
            return None
        return None

    results_list = batch.get("results") or []
    results_map = {}
    for r in results_list:
        rid = r.get("esi_id") or r.get("esiid")
        if rid is None:
            continue
        results_map[str(rid)] = r

    sel = str(selected_esi).strip()
    res = results_map.get(sel) or {"esi_id": sel, "status": "SUCCESS"}

    fp = res.get("final_price_per_mwh")
    if not is_delta and not isinstance(fp, (int, float)) and isinstance(components_map, dict):
        comp_rows = components_map.get(sel) or []
        try:
            fp = round(sum((row.get("avg_price_per_mwh") or 0) for row in comp_rows), 4)
        except Exception:
            fp = None

    price_display = f"{fp:,.4f} USD/MWh" if isinstance(fp, (int, float)) else "N/A"
    final_card = html.Div([html.Div("Final Price", className="subtle"), html.H2(price_display)], className="surface")

    batch_size = batch.get("batch_size")
    if not isinstance(batch_size, int) or batch_size <= 0:
        bs = _safe_len(batch.get("esi_ids"))
        if isinstance(bs, int) and bs > 0:
            batch_size = bs
        else:
            rs = _safe_len(results_list)
            batch_size = rs if isinstance(rs, int) and rs > 0 else (batch_size or 0)

    esi_id_val = res.get("esi_id") or res.get("esiid") or sel
    zone_val = (res.get("zone_id") or res.get("zone") or _infer_zone_from_components(esi_id_val, components_map))
    node_val = (res.get("node_name") or res.get("node") or res.get("node_id"))

    def _row(label, value):
        return [html.Dt(label), html.Dd(_fmt(value))]

    meta = []
    meta += _row("Batch Run ID", batch.get("run_id"))
    meta += _row("Batch Size", batch_size)
    meta += _row("Batch Status", batch.get("status"))
    meta += _row("Per-ESI Status", res.get("status"))
    meta += _row("ESI ID", esi_id_val)
    meta += _row("Zone", zone_val)
    meta += _row("Node", node_val)
    meta += _row("Compute Delta", "Yes" if is_delta else "No")
    meta_block = html.Div([html.Div("Run Metadata", className="subtle"), html.Dl(meta, className="meta-list")], className="surface mt-12")

    rows = []
    if isinstance(components_map, dict):
        rows = components_map.get(sel) or []

    if not rows:
        meta_block.children.append(
            html.Div(
                f"Cost components not available from components task. Ensure the Job includes a dependent task with task_key='{COMPONENTS_TASK_KEY}' that returns data_by_esi.",
                className="subtle mt-8",
            )
        )
        return final_card, meta_block, [], []

    if is_delta:
        columns = [
            {"name": "Curve", "id": "curve_name"},
            {"name": "Bin", "id": "bin_label"},
            {"name": "Base Price", "id": "base_price"},
            {"name": "Bumped Price", "id": "bumped_price"},
            {"name": "Delta", "id": "delta"},
            {"name": "Bump Size", "id": "bump_size"},
            {"name": "Bump Type", "id": "bump_type"},
            {"name": "Weight", "id": "bin_weight"},
            {"name": "Has Data", "id": "has_data"},
        ]
        data = []
        for r in rows:
            data.append({
                "curve_name": r.get("curve_name"),
                "bin_label": r.get("bin_label"),
                "base_price": r.get("base_price"),
                "bumped_price": r.get("bumped_price"),
                "delta": r.get("delta"),
                "bump_size": r.get("bump_size"),
                "bump_type": r.get("bump_type"),
                "bin_weight": r.get("bin_weight"),
                "has_data": r.get("has_data"),
            })
        total = None  # not computed in delta table
    else:
        cols_order = ["curve_name", "component_label", "avg_price_per_mwh", "pct_of_total"]
        columns = [{"name": n.replace("_", " ").title(), "id": n} for n in cols_order]
        data = [{k: r.get(k) for k in cols_order} for r in rows]
        try:
            total = round(sum((r.get("avg_price_per_mwh") or 0) for r in data), 4)
        except Exception:
            total = None
        if total is not None:
            data.append({"curve_name": "", "component_label": "TOTAL", "avg_price_per_mwh": total, "pct_of_total": 100.0 if (fp and fp != 0) else None})

    if not is_delta and isinstance(fp, (int, float)) and abs((total or 0) - fp) > 1e-3:
        meta_block.children.append(
            html.Div(f"Note: component total ({total:,.4f}) differs from final price ({fp:,.4f}).", className="subtle mt-8")
        )

    return final_card, meta_block, columns, data

# =========================================
# CRUD Tab Callbacks
# =========================================
@app.callback(
    Output("crud-grid", "columns"),
    Output("crud-grid", "data"),
    Output("crud-original", "data"),
    Output("crud-schema", "data"),
    Output("crud-config", "data"),
    Output("crud-toast", "children", allow_duplicate=True),
    Input("crud-table", "value"),
    Input("crud-refresh", "n_clicks"),
    prevent_initial_call=True,
)
def crud_load_table(table_name, _n_refresh):
    if not table_name:
        return [], [], None, None, None, "Select a table."
    short = table_name.split(".")[-1]
    cfg = TABLE_CONFIG.get(short, {"pk": None, "editable_cols": []})
    fqn = _fqn(table_name)

    try:
        cols, data = crud_read_table(fqn, limit=500)
        # Resolve editable cols
        if cfg.get("editable_cols") == "ALL":
            editable_cols = [c for c in cols if c != cfg.get("pk")]
            cfg_resolved = {"pk": cfg.get("pk"), "editable_cols": editable_cols}
        else:
            cfg_resolved = {"pk": cfg.get("pk"), "editable_cols": cfg.get("editable_cols", [])}
        if not cfg_resolved["pk"] or cfg_resolved["pk"] not in cols:
            cfg_resolved["editable_cols"] = []
            pk_msg = f" (read-only: PK '{cfg.get('pk')}' not found)" if cfg.get("pk") else " (read-only: no PK configured)"
        else:
            pk_msg = f" (pk={cfg_resolved['pk']})"

        col_specs = [{"name": c, "id": c, "editable": c in cfg_resolved["editable_cols"]} for c in cols]
        return col_specs, data, data, cols, cfg_resolved, f"Loaded {fqn}{pk_msg}. Rows: {len(data)}"
    except Exception as e:
        return [], [], None, None, None, f"Load error: {e}"

@app.callback(
    Output("crud-grid", "data", allow_duplicate=True),
    Output("crud-toast", "children", allow_duplicate=True),
    Input("crud-add", "n_clicks"),
    State("crud-grid", "data"),
    State("crud-schema", "data"),
    State("crud-config", "data"),
    prevent_initial_call=True,
)
def crud_add_row(_n, data, cols, cfg):
    data = list(data or [])
    cols = cols or []
    cfg = cfg or {"pk": None, "editable_cols": []}
    new_row = {c: None for c in cols}
    if cfg.get("pk") == "id":  # convenience
        new_row["id"] = uuid4().hex
    data.append(new_row)
    return data, "Row added (not saved yet)."

@app.callback(
    Output("crud-grid", "data", allow_duplicate=True),
    Output("crud-deletes", "data"),
    Output("crud-toast", "children", allow_duplicate=True),
    Input("crud-del", "n_clicks"),
    State("crud-grid", "data"),
    State("crud-grid", "selected_rows"),
    State("crud-config", "data"),
    State("crud-deletes", "data"),
    prevent_initial_call=True,
)
def crud_delete_rows(_n, data, selected_rows, cfg, pending_deletes):
    if not data or not selected_rows:
        return no_update, no_update, "Select row(s) to delete."
    cfg = cfg or {"pk": None}
    pk = cfg.get("pk")
    if not pk:
        return no_update, no_update, "This table is read-only (no PK)."
    pending_deletes = set(pending_deletes or [])
    keep = []
    for i, row in enumerate(data):
        if i in selected_rows:
            if pk in row and row[pk] is not None:
                pending_deletes.add(row[pk])
        else:
            keep.append(row)
    return keep, list(pending_deletes), f"Marked {len(selected_rows)} row(s) for deletion (not saved yet)."

@app.callback(
    Output("crud-toast", "children", allow_duplicate=True),
    Output("crud-original", "data", allow_duplicate=True),
    Input("crud-save", "n_clicks"),
    State("crud-table", "value"),
    State("crud-grid", "data"),
    State("crud-original", "data"),
    State("crud-config", "data"),
    State("crud-schema", "data"),
    State("crud-deletes", "data"),
    State("crud-versioning", "value"),
    prevent_initial_call=True,
)
def crud_save_changes(_n, table_name, current, original, cfg, cols, pending_deletes, versioning_mode):
    if not table_name:
        return "Select a table first.", no_update
    fqn = _fqn(table_name)
    cfg = cfg or {"pk": None, "editable_cols": []}
    pk = cfg.get("pk")
    editable_cols = cfg.get("editable_cols") or []
    if not pk or not editable_cols:
        return "This table is read-only (missing PK or no editable columns).", no_update

    current = current or []
    original = original or []
    cols = cols or []

    def _by_pk(rows):
        out = {}
        for r in rows:
            if pk in r and r[pk] is not None:
                out[str(r[pk])] = r
        return out

    cur_by_pk = _by_pk(current)
    org_by_pk = _by_pk(original)
    org_keys   = set(org_by_pk.keys())
    cur_keys   = set(cur_by_pk.keys())

    inserts, updates = [], []
    deletes = set(pending_deletes or [])

    # Inserts: new PK or PK missing
    for r in current:
        pk_val = r.get(pk)
        if pk_val is None or str(pk_val) not in org_keys:
            ins = {c: r.get(c) for c in cols if c in r}
            inserts.append(ins)

    # Updates: changed editable cols
    for k in (cur_keys & org_keys):
        before = org_by_pk[k]; after = cur_by_pk[k]
        if any(before.get(c) != after.get(c) for c in editable_cols):
            updates.append({c: after.get(c) for c in set([pk] + editable_cols)})

    # Deletes: removed vs original
    removed_keys = org_keys - cur_keys
    deletes.update(removed_keys)

    try:
        res = crud_apply_changes(fqn, pk, inserts, updates, list(deletes), versioning_mode=versioning_mode or "delta_audit")
        msg = f"Saved — inserts: {res.get('inserted',0)}, updates: {res.get('updated',0)}, deletes: {res.get('deleted',0)}, version: {res.get('version')}"
    except Exception as e:
        return f"Save error: {e}", no_update

    # Reload canonical data after save
    try:
        _, fresh = crud_read_table(fqn, limit=500)
    except Exception:
        fresh = current

    return msg, fresh

@app.callback(
    Output("crud-history-data", "data"),
    Output("crud-history-pre", "children"),
    Output("crud-toast", "children", allow_duplicate=True),
    Input("crud-history-table", "n_clicks"),
    State("crud-table", "value"),
    prevent_initial_call=True,
)
def on_history_table(_n, table_name):
    if not table_name:
        return None, no_update, "Select a table."
    try:
        out = crud_history_table(_fqn(table_name))
        if out.get("status") != "SUCCESS":
            return None, "", f"History error: {out.get('error')}"
        hist = out.get("history", [])
        preview = json.dumps(hist[:10], indent=2, default=str)  # show top entries
        return out, preview, f"History loaded — {len(hist)} entries."
    except Exception as e:
        return None, "", f"History error: {e}"

@app.callback(
    Output("crud-row-history", "data"),
    Output("crud-history-pre", "children", allow_duplicate=True),
    Output("crud-toast", "children", allow_duplicate=True),
    Input("crud-history-row", "n_clicks"),
    State("crud-grid", "selected_rows"),
    State("crud-grid", "data"),
    State("crud-config", "data"),
    State("crud-table", "value"),
    prevent_initial_call=True,
)
def on_history_row(_n, sel_rows, data, cfg, table_name):
    if not table_name:
        return None, no_update, "Select a table."
    if not sel_rows:
        return None, no_update, "Select a row to view history."
    idx = sel_rows[0]
    row = (data or [])[idx]
    pk_col = (cfg or {}).get("pk")
    if not pk_col or pk_col not in row:
        return None, no_update, "This table has no PK configured."
    try:
        out = crud_row_history(_fqn(table_name), pk_col, row[pk_col], max_versions=20)
        if out.get("status") != "SUCCESS":
            return None, "", f"Row history error: {out.get('error')}"
        preview = json.dumps(out.get("timeline", []), indent=2, default=str)
        return out, preview, f"Row history loaded for {pk_col}={row[pk_col]}"
    except Exception as e:
        return None, "", f"Row history error: {e}"

@app.callback(
    Output("crud-toast", "children", allow_duplicate=True),
    Input("crud-revert", "n_clicks"),
    State("crud-revert-ver", "value"),
    State("crud-grid", "selected_rows"),
    State("crud-grid", "data"),
    State("crud-config", "data"),
    State("crud-table", "value"),
    prevent_initial_call=True,
)
def on_revert(_n, revert_ver, sel_rows, data, cfg, table_name):
    if not table_name:
        return "Select a table."
    if revert_ver is None:
        return "Enter a target version to revert to."
    if not sel_rows:
        return "Select at least one row to revert."
    pk = (cfg or {}).get("pk")
    if not pk:
        return "This table has no PK configured."
    rows = data or []
    pk_vals = []
    for i in sel_rows:
        if i < len(rows) and pk in rows[i]:
            pk_vals.append(rows[i][pk])
    try:
        res = crud_revert(_fqn(table_name), pk, pk_vals, int(revert_ver))
        return f"Reverted {res.get('reverted',0)} row(s) to version {res.get('version')}"
    except Exception as e:
        return f"Revert error: {e}"

# =========================================
# Main
# =========================================
if __name__ == "__main__":
    app.run(debug=True)
