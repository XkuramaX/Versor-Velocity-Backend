"""
Headless Workflow Runner
────────────────────────
Executes a saved workflow server-side without the UI.
- For upload_csv nodes: picks files from the watched folder
- For read_from_db nodes: executes the query directly
- Logs results per node
"""

import json
import os
import polars as pl
from typing import Optional
from controllers.DataframeController import DataframeController
from controllers.WorkflowManager import WorkflowManager

WATCHED_FOLDER_BASE = os.environ.get("WATCHED_FOLDER", "/app/watched_files")


def get_workflow_folder(workflow_id: str) -> str:
    folder = os.path.join(WATCHED_FOLDER_BASE, workflow_id)
    os.makedirs(folder, exist_ok=True)
    return folder


def _find_file(folder: str, label: str) -> Optional[str]:
    if not os.path.exists(folder):
        return None
    files = os.listdir(folder)
    if not files:
        return None
    for f in files:
        if f.lower() == (label or "").lower():
            return os.path.join(folder, f)
    csv_files = [f for f in files if f.endswith(".csv")]
    if csv_files:
        return os.path.join(folder, csv_files[0])
    excel_files = [f for f in files if f.endswith(".xlsx") or f.endswith(".xls")]
    if excel_files:
        return os.path.join(folder, excel_files[0])
    return None


def _resolve_method(node_type: str, config: dict) -> str:
    mode_map = {"upper": "str_to_upper", "lower": "str_to_lower", "title": "str_to_title"}
    mapping = {
        "safe_filter": "safe_filter_data", "select": "select_columns",
        "drop": "drop_columns", "sort": "sort_data", "rename": "rename_columns",
        "reorder": "reorder_columns",
        "string_case": mode_map.get(config.get("mode", "upper"), "str_to_upper"),
        "string_slice": "str_left" if config.get("mode") == "left" else "str_right",
        "string_mid": "str_mid", "string_concat": "concat_columns",
        "string_prefix_suffix": "concat_with_literal", "string_clean": "clean_string_column",
        "math_horizontal": "horizontal_sum" if config.get("op") == "sum" else "horizontal_average",
        "math_custom": "apply_custom_expression", "math_multiply_bulk": "multi_column_multiply",
        "vector_dot_product": "vector_dot_product", "vector_linear_multiply": "vector_linear_multiply",
        "vector_cross_product": "vector_cross_product",
        "drop_na": "drop_na", "drop_nulls": "drop_nulls", "drop_duplicates": "drop_duplicates",
        "fill_missing": "fill_missing", "cast": "cast_column", "extract_date_parts": "extract_date_parts",
        "outliers": "filter_outliers_iqr", "groupby": "group_by_agg",
        "stats": "col_stats_advanced", "pivot": "pivot_table", "moving_average": "moving_average",
        "conditional": "conditional_column", "transpose": "matrix_transpose",
        "linear_regression": "linear_regression_node", "logistic_prediction": "logistic_regression_prediction",
        "correlation": "correlation_matrix_1d", "ols_regression": "ols_regression",
        "t_test": "t_test", "f_test": "f_test", "chi_square_test": "chi_square_test",
        "dw_test": "dw_test", "anova_test": "anova_test", "chart": "chart_node",
        "add_literal_column": "add_literal_column", "range_bucket": "range_bucket",
        "date_offset": "date_offset", "crosstab": "crosstab", "cumulative_product": "cumulative_product",
    }
    return mapping.get(node_type)


def run_workflow_headless(workflow_id: str, workflow_data: str) -> dict:
    engine = DataframeController()
    wf_manager = WorkflowManager(engine)

    try:
        wf = json.loads(workflow_data)
    except json.JSONDecodeError:
        return {"status": "error", "error": "Invalid workflow JSON", "node_results": []}

    nodes = wf.get("nodes", [])
    edges = wf.get("edges", [])
    if not nodes:
        return {"status": "error", "error": "No nodes", "node_results": []}

    # Topological sort
    graph = {n["id"]: [] for n in nodes}
    in_deg = {n["id"]: 0 for n in nodes}
    for e in edges:
        if e["source"] in graph:
            graph[e["source"]].append(e["target"])
        if e["target"] in in_deg:
            in_deg[e["target"]] += 1
    queue = [nid for nid, d in in_deg.items() if d == 0]
    exec_order = []
    while queue:
        nid = queue.pop(0)
        exec_order.append(nid)
        for nb in graph.get(nid, []):
            in_deg[nb] -= 1
            if in_deg[nb] == 0:
                queue.append(nb)

    backend_ids = {}
    node_results = []
    folder = get_workflow_folder(workflow_id)

    for fid in exec_order:
        node = next((n for n in nodes if n["id"] == fid), None)
        if not node:
            continue
        data = node.get("data", {})
        nt = data.get("nodeType", "")
        config = data.get("config", {})
        label = data.get("label", "")

        try:
            if nt == "upload_csv":
                fp = _find_file(folder, label)
                if not fp:
                    raise FileNotFoundError(f"No file for '{label}' in {folder}")
                lf = pl.scan_csv(fp)
                bid = engine.save_node_result(lf, f"src_{fid[:8]}")
                wf_manager.nodes[bid] = {"type": "upload_csv", "params": {}, "parent": None, "children": []}
                backend_ids[fid] = bid

            elif nt == "upload_excel":
                fp = _find_file(folder, label)
                if not fp:
                    raise FileNotFoundError(f"No file for '{label}' in {folder}")
                lf = pl.read_excel(fp, sheet_name=config.get("sheet_name", "Sheet1")).lazy()
                bid = engine.save_node_result(lf, f"src_{fid[:8]}")
                backend_ids[fid] = bid

            elif nt == "read_from_db":
                bid = engine.read_from_db(config.get("connection_string", ""), config.get("query", ""))
                backend_ids[fid] = bid

            elif nt == "join":
                pes = [e for e in edges if e["target"] == fid]
                left = backend_ids.get(pes[0]["source"]) if len(pes) > 0 else None
                right = backend_ids.get(pes[1]["source"]) if len(pes) > 1 else None
                bid = wf_manager.create_node("join_nodes", {"right_id": right, **config}, left)
                backend_ids[fid] = bid

            elif nt == "union":
                pes = [e for e in edges if e["target"] == fid]
                ids = [backend_ids.get(e["source"]) for e in pes if backend_ids.get(e["source"])]
                bid = wf_manager.create_node("union_nodes", {"node_ids": ids}, None)
                backend_ids[fid] = bid

            else:
                method = _resolve_method(nt, config)
                if not method:
                    raise ValueError(f"Unknown node type: {nt}")
                pe = next((e for e in edges if e["target"] == fid), None)
                pid = backend_ids.get(pe["source"]) if pe else None
                if not pid:
                    raise ValueError(f"No parent for {nt}")
                bid = wf_manager.create_node(method, config, pid)
                backend_ids[fid] = bid

            node_results.append({"node_id": fid, "backend_id": backend_ids.get(fid), "type": nt, "label": label, "status": "success"})

        except Exception as e:
            node_results.append({"node_id": fid, "type": nt, "label": label, "status": "error", "error": str(e)})
            return {"status": "error", "error": f"{label} ({nt}): {e}", "node_results": node_results}

    return {"status": "success", "error": None, "node_results": node_results}
