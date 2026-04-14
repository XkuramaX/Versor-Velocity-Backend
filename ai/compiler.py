"""
The Compiler — Deterministic React Flow Generator
──────────────────────────────────────────────────
Input:  Validated step list from the Validator
Output: React Flow JSON {nodes: [...], edges: [...]}

Pure Python. No LLM. No hallucination. Guaranteed valid structure.
"""

X_START = 100
X_STEP = 260
Y_MAIN = 200
Y_BRANCH = 180


def compile_to_react_flow(steps: list[dict]) -> dict:
    """
    Convert validated steps into React Flow nodes and edges.
    Each step becomes a node. Edges are derived from the input references.
    """
    nodes = []
    edges = []
    alias_to_id = {}  # alias -> react flow node id
    counter = 0

    # First pass: identify upload nodes for branch layout
    upload_count = sum(1 for s in steps if s.get("action") in ("upload_csv", "upload_excel"))

    for i, step in enumerate(steps):
        action = step.get("action", "unknown")
        alias = step.get("alias", action)
        config = step.get("config", {}) or {}
        label = step.get("label", "") or _make_label(step)

        # Generate unique ID
        counter += 1
        node_id = f"{action}_{counter}"
        alias_to_id[alias] = node_id
        # Also map by action name for simple references
        alias_to_id[action] = node_id

        # Calculate position
        if action in ("upload_csv", "upload_excel") and upload_count > 1:
            upload_idx = sum(1 for s in steps[:i] if s.get("action") in ("upload_csv", "upload_excel"))
            x = X_START
            y = Y_MAIN + (upload_idx - (upload_count - 1) / 2) * Y_BRANCH
        else:
            x = X_START + i * X_STEP
            y = Y_MAIN

        # Build React Flow node
        node = {
            "id": node_id,
            "type": "custom",
            "position": {"x": round(x), "y": round(y)},
            "data": {
                "label": label,
                "nodeType": action,
                "config": config,
                "status": "idle",
                "backendNodeId": None,
            },
        }
        nodes.append(node)

        # Build edges from input references
        input_ref = step.get("input")
        if input_ref:
            if isinstance(input_ref, list):
                # Multiple inputs (join/union)
                for ref in input_ref:
                    src_id = alias_to_id.get(ref)
                    if src_id:
                        edges.append(_make_edge(src_id, node_id))
            else:
                src_id = alias_to_id.get(input_ref)
                if src_id:
                    edges.append(_make_edge(src_id, node_id))
        elif i > 0 and action not in ("upload_csv", "upload_excel", "read_from_db"):
            # Default: connect to previous node
            prev_id = nodes[i - 1]["id"]
            edges.append(_make_edge(prev_id, node_id))

    return {"nodes": nodes, "edges": edges}


def _make_edge(source: str, target: str) -> dict:
    return {
        "id": f"e{source}-{target}",
        "source": source,
        "target": target,
        "type": "smoothstep",
        "animated": True,
    }


def _make_label(step: dict) -> str:
    """Generate a human-readable label from a step."""
    action = step.get("action", "")
    config = step.get("config", {}) or {}

    if action == "upload_csv":
        return step.get("source_file", "CSV File")
    elif action == "safe_filter":
        filters = config.get("filters", [])
        if filters and isinstance(filters[0], dict):
            f = filters[0]
            return f"Filter {f.get('column','')} {f.get('operation','')} {f.get('value','')}"
        return "Filter"
    elif action == "math_horizontal":
        return f"{config.get('new_col', 'result')} = {config.get('op', 'sum')}({', '.join(config.get('columns', []))})"
    elif action == "groupby":
        return f"Group by {', '.join(config.get('group_cols', []))}"
    elif action == "sort":
        return f"Sort by {', '.join(config.get('by', []))}"
    elif action == "rename":
        m = config.get("mapping", {})
        return f"Rename {', '.join(f'{k}→{v}' for k,v in m.items())}" if m else "Rename"
    elif action == "select":
        return f"Select {', '.join(config.get('columns', []))}"
    elif action == "join":
        return f"Join on {config.get('on', '?')}"
    elif action == "conditional":
        return f"IF {config.get('column','')} {config.get('op','')} {config.get('threshold','')} → {config.get('new_col','')}"
    elif action == "string_clean":
        return f"Clean {config.get('column', '')}"
    elif action == "string_case":
        return f"{config.get('mode', 'upper')}({config.get('column', '')})"
    else:
        return action.replace("_", " ").title()
