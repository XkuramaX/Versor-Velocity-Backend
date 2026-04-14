"""
WorkflowCompiler — Stage 2 of the Planner-Compiler architecture.

Takes a flat Logic Recipe (list of steps from the LLM) and deterministically
compiles it into a valid React Flow workflow with:
  - Unique node IDs
  - Calculated X/Y positions on a grid
  - Proper edge connections (parent_id linking)
  - Schema validation (columns referenced must exist in the data)
  - Correct data structure (label, nodeType, config, status, backendNodeId)

The LLM never touches this code. It's pure Python — no hallucination possible.
"""

import json
from ai.node_knowledge import get_all_node_ids, get_node_by_id

VALID_NODE_TYPES = set(get_all_node_ids())

X_START = 100
X_STEP = 260
Y_MAIN = 200
Y_BRANCH_OFFSET = 180


class CompilationError:
    def __init__(self, step_index: int, message: str):
        self.step_index = step_index
        self.message = message

    def __repr__(self):
        return f"Step {self.step_index}: {self.message}"


class WorkflowCompiler:
    def __init__(self, file_schemas: list[dict]):
        """
        file_schemas: [{"name": "employees.csv", "schema": {"salary": "integer", ...}}]
        """
        self.file_schemas = {s["name"]: s.get("schema", s.get("columns", {})) for s in file_schemas}
        self.nodes = []
        self.edges = []
        self.errors = []
        self.warnings = []
        self._id_counter = 0
        self._columns_at_step = {}  # step_index -> set of available columns

    def compile(self, recipe: list[dict]) -> dict:
        """
        Compile a Logic Recipe into a React Flow workflow.
        Returns: {"nodes": [...], "edges": [...], "errors": [...], "warnings": [...]}
        """
        self.nodes = []
        self.edges = []
        self.errors = []
        self.warnings = []
        self._id_counter = 0

        if not recipe:
            self.errors.append(CompilationError(0, "Empty recipe"))
            return self._result()

        # Pass 1: Identify upload nodes and join points
        upload_indices = [i for i, s in enumerate(recipe) if s.get("node_type") == "upload_csv"]
        join_indices = [i for i, s in enumerate(recipe) if s.get("node_type") == "join"]

        # Pass 2: Build nodes with positions
        prev_node_id = None
        for i, step in enumerate(recipe):
            node_type = step.get("node_type", "")
            config = step.get("config", {})
            label = step.get("label", node_type)

            # Validate node_type
            if node_type not in VALID_NODE_TYPES:
                self.errors.append(CompilationError(i, f"Invalid node_type: '{node_type}'"))
                continue

            # Generate unique ID
            node_id = self._make_id(node_type)

            # Calculate position
            if node_type == "upload_csv" and len(upload_indices) > 1:
                # Multiple uploads: stack vertically
                upload_rank = upload_indices.index(i)
                x = X_START
                y = Y_MAIN + (upload_rank - len(upload_indices) / 2) * Y_BRANCH_OFFSET
            else:
                x = X_START + i * X_STEP
                y = Y_MAIN

            # Track available columns for validation
            if node_type in ("upload_csv", "upload_excel"):
                source_file = step.get("source_file", label)
                file_schema = self.file_schemas.get(source_file, {})
                if isinstance(file_schema, dict):
                    self._columns_at_step[i] = set(file_schema.keys())
                elif isinstance(file_schema, list):
                    self._columns_at_step[i] = set(file_schema)
                else:
                    self._columns_at_step[i] = set()
            elif prev_node_id is not None:
                # Inherit columns from previous step (simplified — doesn't track transforms)
                prev_idx = i - 1
                self._columns_at_step[i] = self._columns_at_step.get(prev_idx, set()).copy()

            # Validate column references in config
            self._validate_columns(i, node_type, config)

            # Build the React Flow node
            node = {
                "id": node_id,
                "type": "custom",
                "position": {"x": round(x), "y": round(y)},
                "data": {
                    "label": label,
                    "nodeType": node_type,
                    "config": config,
                    "status": "idle",
                    "backendNodeId": None,
                },
            }
            self.nodes.append(node)

            # Build edges
            if node_type == "join" and "join_sources" in step:
                # Join has two parents
                sources = step["join_sources"]
                for src_idx in sources:
                    if 0 <= src_idx < len(self.nodes):
                        src_id = self.nodes[src_idx]["id"]
                        self._add_edge(src_id, node_id)
            elif node_type not in ("upload_csv", "upload_excel", "read_from_db"):
                # Linear chain: connect to previous node
                if prev_node_id:
                    self._add_edge(prev_node_id, node_id)

            prev_node_id = node_id

        return self._result()

    def _make_id(self, node_type: str) -> str:
        self._id_counter += 1
        return f"{node_type}_{self._id_counter}"

    def _add_edge(self, source: str, target: str):
        edge_id = f"e{source}-{target}"
        self.edges.append({
            "id": edge_id,
            "source": source,
            "target": target,
            "type": "smoothstep",
            "animated": True,
        })

    def _validate_columns(self, step_index: int, node_type: str, config: dict):
        """Check that columns referenced in config exist in the schema."""
        available = self._columns_at_step.get(step_index, set())
        if not available:
            return  # Can't validate without schema

        # Extract column references from config
        cols_to_check = []
        if "column" in config:
            cols_to_check.append(config["column"])
        if "columns" in config and isinstance(config["columns"], list):
            cols_to_check.extend(config["columns"])
        if "by" in config and isinstance(config["by"], list):
            cols_to_check.extend(config["by"])
        if "group_cols" in config and isinstance(config["group_cols"], list):
            cols_to_check.extend(config["group_cols"])
        if "on" in config and isinstance(config["on"], str):
            cols_to_check.append(config["on"])
        if "target" in config:
            cols_to_check.append(config["target"])
        if "features" in config and isinstance(config["features"], list):
            cols_to_check.extend(config["features"])
        if "left_cols" in config and isinstance(config["left_cols"], list):
            cols_to_check.extend(config["left_cols"])
        if "vec_a" in config and isinstance(config["vec_a"], list):
            cols_to_check.extend(config["vec_a"])
        if "vec_b" in config and isinstance(config["vec_b"], list):
            cols_to_check.extend(config["vec_b"])
        if "filters" in config and isinstance(config["filters"], list):
            for f in config["filters"]:
                if isinstance(f, dict) and "column" in f:
                    cols_to_check.append(f["column"])
        if "aggs" in config and isinstance(config["aggs"], dict):
            cols_to_check.extend(config["aggs"].keys())

        for col in cols_to_check:
            if col and col not in available:
                self.warnings.append(
                    CompilationError(step_index, f"Column '{col}' not found in schema. Available: {sorted(available)}")
                )

    def _result(self) -> dict:
        return {
            "nodes": self.nodes,
            "edges": self.edges,
            "errors": [str(e) for e in self.errors],
            "warnings": [str(w) for w in self.warnings],
        }
