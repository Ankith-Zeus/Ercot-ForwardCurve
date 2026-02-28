
import os
import json
import requests
from dash import Dash, html, dcc, dash_table, Input, Output, State, no_update, ctx
import hashlib
from pathlib import Path
from datetime import datetime
# =========================================
# .env loader (no external dependencies)
# =========================================

def load_dotenv(path: str = ".env"):
    """Minimal .env loader that populates os.environ.
    Supports lines like KEY=VALUE, ignores blanks and comments (#...).
    Does not override already-set environment variables.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
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

# Task key of the components notebook in your multi-task Job
COMPONENTS_TASK_KEY = os.getenv("COMPONENTS_TASK_KEY", "components_task")


import hashlib
from pathlib import Path
from datetime import datetime

# --------------------------
# Simple file-based cache
# --------------------------
CACHE_DIR = Path(os.getenv("CACHE_DIR", ".cache"))
RUNS_DIR = CACHE_DIR / "runs"
INDEX_FILE = CACHE_DIR / "index.json"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

def as_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() == "true"
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
    """Persist one run and update the index."""
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

    # De-dupe then insert newest-first
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
    """Trigger the multi-task Job run. The Job should:
       - Task A: engine notebook
       - Task B: components notebook (task_key = COMPONENTS_TASK_KEY) depends on A
    """
    _require_env(["DATABRICKS_INSTANCE", "DATABRICKS_TOKEN", "DATABRICKS_JOB_ID"])
    url = f"{HOST}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    payload = {
        "job_id": int(JOBID),
        "notebook_params": {
            # Passed to the orchestrator/engine entry task
            "esi_ids": esi_ids,
            "as_of_date": as_of_date,
            "from_date": from_date,
            "to_date": to_date,
            "compute_delta": compute_delta_flag,
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
    """Fetch notebook output JSON for a run.
    If task_key is None, try the run_id directly; if 400, resolve child task run_id.
    If task_key is provided, prefer that child task's run_id.
    """
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

    # Attempt direct
    try:
        if task_key is None:
            return _get_output(run_id)
    except RuntimeError as e:
        if "runs/get-output failed" not in str(e):
            raise

    # Resolve child tasks
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
        # fallback: first task with a run_id
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
# Dash App
# =========================================
app = Dash(__name__, title="Ercot Price C&I Breakdown")

app.layout = html.Div(
    className="app-root",
    children=[
        html.Div(
            className="surface",
            children=[
                html.Div(
                    className="header",
                    children=[

                        # Title Section
                        html.H4("Ercot C&I Pricing"),
                        html.Div(
                            "Submit parameters and retrieve final price breakdown",
                            className="subtle"
                        ),

                        # =========================
                        # FORM SECTION
                        # =========================
                        html.Div(
                            className="form-grid",
                            children=[

                                # ESI Input
                                html.Div(
                                    className="form-item col-12",
                                    children=[
                                        html.Label("ESI IDs (comma-separated OR JSON array)"),
                                        dcc.Input(
                                            id="orch-esi",
                                            type="text",
                                            debounce=True,
                                            style={"width": "100%"}
                                        ),
                                    ],
                                ),

                                # Date Row (All 3 Inline)
                                html.Div(
                                    className="form-item col-12",
                                    children=[
                                        html.Div(
                                            style={
                                                "display": "flex",
                                                "gap": "28px",
                                                "marginTop": "12px",
                                                "flexWrap": "wrap",
                                            },
                                            children=[

                                                html.Div(
                                                    style={"flex": "1", "minWidth": "220px"},
                                                    children=[
                                                        html.Label("As-of Date (YYYY-MM-DD)"),
                                                        dcc.Input(
                                                            id="orch-asof",
                                                            type="text",
                                                            debounce=True,
                                                            style={"width": "100%"}
                                                        ),
                                                    ],
                                                ),

                                                html.Div(
                                                    style={"flex": "1", "minWidth": "220px"},
                                                    children=[
                                                        html.Label("From Delivery Date (YYYY-MM-DD)"),
                                                        dcc.Input(
                                                            id="orch-from",
                                                            type="text",
                                                            debounce=True,
                                                            style={"width": "100%"}
                                                        ),
                                                    ],
                                                ),

                                                html.Div(
                                                    style={"flex": "1", "minWidth": "220px"},
                                                    children=[
                                                        html.Label("To Delivery Date (YYYY-MM-DD)"),
                                                        dcc.Input(
                                                            id="orch-to",
                                                            type="text",
                                                            debounce=True,
                                                            style={"width": "100%"}
                                                        ),
                                                    ],
                                                ),
                                                html.Div(
                                                    className="form-item col-12 mt-12",
                                                    children=[
                                                        dcc.Checklist(
                                                            id="orch-compute-delta",
                                                            options=[{"label": "Compute Delta Exposure", "value": "true"}],
                                                            value=[],   # empty = false
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

                        # Submit Section
                        html.Div(
                            className="mt-12",
                            children=[
                                html.Button("Submit", id="orch-submit", className="btn"),
                                html.Div(id="orch-status", className="status mt-12"),
                            ],
                        ),

                        # =========================
                        # ESI DROPDOWN
                        # =========================
                        html.Div(
                            className="form-item col-12 mt-12",
                            children=[
                                html.Label("Select ESI ID to view details"),
                                dcc.Dropdown(
                                    id="esi-select",
                                    options=[],
                                    value=None,
                                    placeholder="Select an ESI ID",
                                    clearable=False,
                                ),
                            ],
                        ),
                        html.Div(
                                className="form-item col-12 mt-12",
                                children=[
                                    html.Label("Previous Runs"),
                                    dcc.Dropdown(
                                        id="previous-runs",
                                        options=[],
                                        value=None,
                                        placeholder="Select a previous run",
                                        clearable=True,
                                    ),
                                ],
                            ),
                        # =========================
                        # RESULTS SECTION
                        # =========================
                        dcc.Loading(
                            id="orch-loading",
                            type="default",
                            children=[
                                html.Div(id="orch-final-card", className="mt-16"),
                                html.Div(id="orch-meta", className="mt-12"),
                                dash_table.DataTable(
                                    id="orch-components",
                                    columns=[],
                                    data=[],
                                    page_size=15,
                                    sort_action="native",
                                    filter_action="native",
                                    style_table={
                                        "maxHeight": "60vh",
                                        "overflowY": "auto"
                                    },
                                    style_cell={
                                        "textAlign": "center",
                                        "fontFamily": "Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', Arial, sans-serif",
                                        "fontSize": "13px",
                                        "padding": "8px 10px",
                                    },
                                ),
                            ],
                        ),

                        # =========================
                        # STORES & INTERVAL
                        # =========================
                        dcc.Store(id="orch-runid"),
                        dcc.Store(id="orch-batch"),
                        dcc.Store(id="components-store"),
                        dcc.Interval(
                            id="orch-poller",
                            interval=1500,
                            disabled=True
                        ),
                    ],
                )
            ],
        )
    ],
)

# =========================================
# Controller: submit & poll
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

    # ==========================================================
    # 0️⃣ LOAD PREVIOUS RUN
    # ==========================================================
    if trigger == "previous-runs":
        if not selected_previous_run:
            return no_update, no_update, True, no_update, no_update, False

        cached = cache_get_by_run_id(selected_previous_run)

        if not cached:
            return no_update, "Cached run not found.", True, no_update, no_update, False

        return (
            cached.get("run_id"),
            f"Loaded cached run {selected_previous_run}",
            True,                         # disable poller
            cached.get("batch"),
            cached.get("components"),
            False,                        # enable submit
        )

    # ==========================================================
    # 1️⃣ SUBMIT
    # ==========================================================
    if trigger == "orch-submit":
        if not all([esi_text, asof, fdate, tdate]):
            return None, "All four parameters are required.", True, no_update, no_update, False

        esi_text = esi_text.strip()
        asof = asof.strip()
        fdate = fdate.strip()
        tdate = tdate.strip()
        compute_delta_flag = True if "true" in (compute_delta_value or []) else False
        # 🔎 Check parameter cache first
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

        # 🚀 Otherwise trigger new job
        try:
            run_id = run_orchestrator(esi_text, asof, fdate, tdate, compute_delta_flag)
            return run_id, f"Submitted. Run ID: {run_id}", False, None, None, True
        except Exception as e:
            return None, f"Submission failed: {e}", True, no_update, no_update, False

    # ==========================================================
    # 2️⃣ POLLER
    # ==========================================================
    if trigger == "orch-poller":
        if not current_run_id:
            return no_update, no_update, True, no_update, no_update, False

        compute_delta_flag = True if "true" in (compute_delta_value or []) else False
        try:
            st = get_run_state(current_run_id)
            life = st["state"]["life_cycle_state"]
            result_state = st["state"].get("result_state")

            if life in ("PENDING", "QUEUED", "RUNNING", "BLOCKED", "TERMINATING"):
                return (
                    current_run_id,
                    f"Run {current_run_id} is {life}...",
                    False,
                    no_update,
                    no_update,
                    True,
                )

            if life == "TERMINATED":
                if result_state == "SUCCESS":

                    batch = get_run_result(
                        current_run_id,
                        task_key=os.getenv("ORCHESTRATOR_TASK_KEY"),
                    )

                    components_map = None
                    try:
                        comp_out = get_run_result(
                            current_run_id,
                            task_key=COMPONENTS_TASK_KEY,
                        )
                        if isinstance(comp_out, dict) and comp_out.get("data_by_esi"):
                            raw = comp_out.get("data_by_esi")
                            components_map = {str(k): v for k, v in raw.items()}  
                    except Exception:
                        components_map = None

                    # 💾 Save to cache
                    submitted_params = {
                        "esi_ids": esi_text,
                        "as_of_date": asof,
                        "from_date": fdate,
                        "to_date": tdate,
                        "compute_delta": compute_delta_flag,
                    }

                    cache_save_run(
                        run_id=current_run_id,
                        batch=batch,
                        components=components_map,
                        submitted_params=submitted_params,
                    )

                    return (
                        current_run_id,
                        f"Run {current_run_id} completed successfully.",
                        True,
                        batch,
                        components_map,
                        False,
                    )

                # FAILED
                error_message = (
                    st.get("state", {}).get("state_message")
                    or st.get("state", {}).get("result_state")
                    or "Unknown failure"
                )

                return (
                    current_run_id,
                    f"Run {current_run_id} FAILED: {error_message}",
                    True,
                    no_update,
                    no_update,
                    False,
                )

            return (
                current_run_id,
                f"Run {current_run_id} state: {life}",
                False,
                no_update,
                no_update,
                True,
            )

        except Exception as e:
            return current_run_id, f"Polling error: {e}", True, no_update, no_update, False

    # ==========================================================
    # FALLBACK
    # ==========================================================
    return no_update, no_update, True, no_update, no_update, False
# =========================================
# Helpers
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

# =========================================
# Populate ESI selector after batch ready
# =========================================
@app.callback(
    Output("esi-select", "options"),
    Output("esi-select", "value"),
    Input("orch-batch", "data"),
    Input("components-store", "data"),
    prevent_initial_call=True,
)
def populate_selector(batch, components_map):
    # Normalize inputs
    batch = _ensure_dict(batch)
    components_map = _ensure_dict(components_map)

    options = []

    # ---- Preferred: components task output ----
    if isinstance(components_map, dict) and components_map:
        for esi in sorted(components_map.keys(), key=lambda x: str(x)):
            esi_str = str(esi)
            options.append({"label": esi_str, "value": esi_str})
        # Always return a 2‑tuple (list, value)
        return options, (options[0]["value"] if options else None)

    # ---- Fallback: orchestrator results ----
    if batch and isinstance(batch.get("results"), list):
        for r in batch["results"]:
            eid = r.get("esi_id") or r.get("esiid")
            if eid is None:
                continue
            eid_str = str(eid)
            label = f"{eid_str} — {r.get('zone_id','')} / {r.get('node_name','')}".strip(" — /")
            options.append({"label": label, "value": eid_str})
        return options, (options[0]["value"] if options else None)

    # ---- Final fallback: nothing to select yet ----
    return [], None

#Populate previous runs dropdown
@app.callback(
    Output("previous-runs", "options"),
    Input("orch-status", "children"),
)
def populate_previous_runs(_):
    runs = cache_list_runs(limit=25)

    options = []
    for r in runs:
        label = f"{r.get('ts')} — Run {r.get('run_id')} — {r.get('label','')}"
        options.append({
            "label": label,
            "value": r.get("run_id")
        })

    return options
# @app.callback(
#     Output("orch-runid", "data"),
#     Output("orch-batch", "data"),
#     Output("components-store", "data"),
#     Output("orch-status", "children"),
#     Output("orch-poller", "disabled"),
#     Output("orch-submit", "disabled"),
#     Input("previous-runs", "value"),
#     prevent_initial_call=True,
# )
# def load_previous_run(selected_run_id):
#     if not selected_run_id:
#         return no_update, no_update, no_update, no_update, True, False

#     cached = cache_get_by_run_id(selected_run_id)
#     if not cached:
#         return no_update, no_update, no_update, "Cached run not found.", True, False

#     return (
#         cached.get("run_id"),
#         cached.get("batch"),
#         cached.get("components"),
#         f"Loaded cached run {selected_run_id}",
#         True,     # disable poller
#         False     # enable submit
#     )
# =========================================
# Render selection
# =========================================
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

    # ---------- helpers ----------
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

    # ---------- build results map keyed by string ESI ----------
    results_list = batch.get("results") or []
    results_map = {}
    for r in results_list:
        rid = r.get("esi_id") or r.get("esiid")
        if rid is None:
            continue
        results_map[str(rid)] = r  # normalize to string keys

    sel = str(selected_esi).strip()

    # ---------- pick per-ESI result ----------
    res = results_map.get(sel)
    if res is None:
        res = {"esi_id": sel, "status": "SUCCESS"}  # shell for safe rendering

    # ---------- final price (fallback to components sum) ----------
    fp = res.get("final_price_per_mwh")
    if not is_delta and not isinstance(fp, (int, float)) and isinstance(components_map, dict):
        comp_rows = components_map.get(sel) or []
        try:
            fp = round(sum((row.get("avg_price_per_mwh") or 0) for row in comp_rows), 4)
        except Exception:
            fp = None

    price_display = f"{fp:,.4f} USD/MWh" if isinstance(fp, (int, float)) else "N/A"
    final_card = html.Div(
        [html.Div("Final Price", className="subtle"), html.H2(price_display)],
        className="surface",
    )

    # ---------- robust batch size ----------
    batch_size = batch.get("batch_size")
    if not isinstance(batch_size, int) or batch_size <= 0:
        bs = _safe_len(batch.get("esi_ids"))
        if isinstance(bs, int) and bs > 0:
            batch_size = bs
        else:
            rs = _safe_len(results_list)
            batch_size = rs if isinstance(rs, int) and rs > 0 else (batch_size or 0)

    # ---------- Zone/Node from orchestrator result; zone fallback from components ----------
    esi_id_val = res.get("esi_id") or res.get("esiid") or sel
    zone_val = (
    res.get("zone_id")
    or res.get("zone")
    or _infer_zone_from_components(esi_id_val, components_map)
)

    node_val = (
        res.get("node_name")
        or res.get("node")
        or res.get("node_id")
    )

    # ---------- metadata ----------
    
    def _row(label, value):
        return [
        html.Dt(label),
        html.Dd(_fmt(value)),
    ]

    meta = []
    meta += _row("Batch Run ID", batch.get("run_id"))
    meta += _row("Batch Size", batch_size)
    meta += _row("Batch Status", batch.get("status"))
    meta += _row("Per-ESI Status", res.get("status"))
    meta += _row("ESI ID", esi_id_val)
    meta += _row("Zone", zone_val)
    meta += _row("Node", node_val)
    meta += _row("Compute Delta", "Yes" if is_delta else "No")
    meta_block = html.Div(
        [
            html.Div("Run Metadata", className="subtle"),
            html.Dl(meta, className="meta-list"),
        ],
        className="surface mt-12",
    )


    # ---------- components table ----------
    rows = []
    if isinstance(components_map, dict):
        rows = components_map.get(sel) or []

    if not rows:
        meta_block.children.append(
            html.Div(
                "Cost components not available from components task. Ensure the Job includes a dependent task with task_key='{}' that returns components_by_esi.".format(
                    COMPONENTS_TASK_KEY
                ),
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

    else:

        cols_order = ["curve_name", "component_label", "avg_price_per_mwh", "pct_of_total"]
        columns = [{"name": n.replace("_", " ").title(), "id": n} for n in cols_order]

        data = []
        for r in rows:
            d = {k: r.get(k) for k in cols_order}
            data.append(d)

    if not is_delta:

        try:
            total = round(sum((r.get("avg_price_per_mwh") or 0) for r in data), 4)
        except Exception:
            total = None

        if total is not None:
            data.append(
                {
                    "curve_name": "",
                    "component_label": "TOTAL",
                    "avg_price_per_mwh": total,
                    "pct_of_total": 100.0 if (fp and fp != 0) else None,
                }
            )

    if isinstance(fp, (int, float)) and abs((total or 0) - fp) > 1e-3:
        meta_block.children.append(
            html.Div(
                f"Note: component total ({total:,.4f}) differs from final price ({fp:,.4f}).",
                className="subtle mt-8",
            )
        )

    return final_card, meta_block, columns, data

# =========================================
# Main
# =========================================
if __name__ == "__main__":
    app.run(debug=True)
