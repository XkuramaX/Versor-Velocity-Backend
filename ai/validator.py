"""
Stage B — The Validator (The Engineer)
──────────────────────────────────────
Input:  The step sequence from Stage A + file schemas
Task:   Validate columns exist, check type compatibility, auto-fix errors
Output: A validated step list + list of errors/warnings

The validator tracks the "live schema" as it flows through the pipeline:
  - upload_csv → schema comes from the file
  - rename → old column removed, new column added
  - math_horizontal → new column added
  - select → schema narrowed to selected columns
  - groupby → schema changes to group_cols + agg output columns
  - etc.
"""

from ai.node_knowledge import get_all_node_ids, get_node_by_id

VALID_ACTIONS = set(get_all_node_ids())

NUMERIC_OPS = {"gt", "gte", "lt", "lte", "between"}

# Common LLM mistakes: wrong key → correct key, per node type
CONFIG_KEY_ALIASES = {
    "math_horizontal": {
        "operation": "op", "new_column_name": "new_col", "new_column": "new_col",
        "result_column": "new_col", "output": "new_col", "name": "new_col",
    },
    "math_custom": {
        "columns": "left_cols", "operation": "op", "operator": "op",
        "value": "right_val", "suffix": "new_suffix",
    },
    "sort": {
        "columns": "by", "column": "by", "sort_by": "by",
        "ascending": "descending",  # handled specially below
    },
    "safe_filter": {
        "filter": "filters",
    },
    "groupby": {
        "group_by": "group_cols", "groupby": "group_cols", "columns": "group_cols",
        "aggregations": "aggs", "aggregate": "aggs",
    },
    "conditional": {
        "condition_column": "column", "value_if_true": "then_val",
        "value_if_false": "else_val", "output_column": "new_col",
        "operator": "op", "operation": "op", "compare_value": "threshold",
    },
    "rename": {
        "columns": "mapping", "renames": "mapping",
    },
    "string_case": {
        "case": "mode", "type": "mode",
    },
}

# Value normalization: wrong values → correct values
VALUE_ALIASES = {
    "math_horizontal": {
        "op": {"add": "sum", "addition": "sum", "+": "sum", "avg": "average", "mean": "average"},
    },
    "sort": {
        # "ascending" key needs to be inverted to "descending"
    },
}


class ValidationResult:
    def __init__(self):
        self.steps = []
        self.errors = []      # fatal — step cannot work
        self.warnings = []    # non-fatal — might work but suspicious
        self.fixes = []       # auto-corrections applied

    def error(self, step_idx: int, msg: str):
        self.errors.append(f"Step {step_idx}: {msg}")

    def warn(self, step_idx: int, msg: str):
        self.warnings.append(f"Step {step_idx}: {msg}")

    def fix(self, step_idx: int, msg: str):
        self.fixes.append(f"Step {step_idx}: {msg}")


def validate(steps: list[dict], file_schemas: list[dict]) -> ValidationResult:
    """
    Validate and fix the planner's output.
    file_schemas: [{"name": "employees.csv", "schema": {"salary": "integer", ...}}]
    """
    result = ValidationResult()

    # Build file schema lookup
    file_map = {}
    for fs in file_schemas:
        name = fs.get("name", "")
        schema = fs.get("schema", fs.get("columns", {}))
        if isinstance(schema, list):
            schema = {c: "string" for c in schema}
        file_map[name] = schema

    # Track live schema per alias
    alias_schemas = {}  # alias -> {col: type}

    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            result.error(i, "Step is not a dict")
            continue

        action = step.get("action", "")

        # Validate action exists
        if action not in VALID_ACTIONS:
            result.error(i, f"Unknown action '{action}'. Valid: {sorted(VALID_ACTIONS)}")
            continue

        config = step.get("config", {}) or {}
        if not isinstance(config, dict):
            config = {}
            step["config"] = config

        # Normalize config keys and values (fix common LLM mistakes)
        config = _normalize_config(i, action, config, result)
        step["config"] = config

        alias = step.get("alias", action)
        input_ref = step.get("input")

        # Resolve input schema
        input_schema = {}
        if action in ("upload_csv", "upload_excel"):
            source = step.get("source_file", "")
            if source in file_map:
                input_schema = dict(file_map[source])
            elif file_map:
                # Auto-fix: use the first file if source doesn't match
                first_file = list(file_map.keys())[0]
                input_schema = dict(file_map[first_file])
                step["source_file"] = first_file
                result.fix(i, f"source_file '{source}' not found, using '{first_file}'")
            alias_schemas[alias] = input_schema
        elif input_ref:
            if isinstance(input_ref, list):
                # Join: merge schemas from both inputs
                for ref in input_ref:
                    if ref in alias_schemas:
                        input_schema.update(alias_schemas[ref])
            elif input_ref in alias_schemas:
                input_schema = dict(alias_schemas[input_ref])
            else:
                # Try to find by action name
                for prev_alias, prev_schema in alias_schemas.items():
                    input_schema = dict(prev_schema)
                if not input_schema:
                    result.warn(i, f"Input '{input_ref}' not found in previous steps")
        elif i > 0 and alias_schemas:
            # Default: use the last step's schema
            last_alias = list(alias_schemas.keys())[-1]
            input_schema = dict(alias_schemas[last_alias])

        # Validate columns referenced in config
        live_schema = dict(input_schema)
        _validate_columns(i, action, config, live_schema, result)

        # Update live schema based on what this action does
        output_schema = _compute_output_schema(action, config, live_schema)
        alias_schemas[alias] = output_schema

        result.steps.append(step)

    return result


def _normalize_config(idx: int, action: str, config: dict, result: ValidationResult) -> dict:
    """
    Fix common LLM mistakes in config keys and values.
    E.g. "operation": "add" → "op": "sum" for math_horizontal.
    """
    normalized = {}
    key_map = CONFIG_KEY_ALIASES.get(action, {})
    val_map = VALUE_ALIASES.get(action, {})

    for key, value in config.items():
        # Normalize key name
        correct_key = key_map.get(key, key)
        if correct_key != key:
            result.fix(idx, f"Config key '{key}' → '{correct_key}'")

        # Special: "ascending" → invert to "descending" for sort
        if action == "sort" and key == "ascending":
            correct_key = "descending"
            value = not value if isinstance(value, bool) else value
            result.fix(idx, f"Converted 'ascending={not value}' to 'descending={value}'")

        # Normalize value
        if correct_key in val_map and isinstance(value, str):
            correct_val = val_map[correct_key].get(value.lower(), value)
            if correct_val != value:
                result.fix(idx, f"Config value '{value}' → '{correct_val}' for key '{correct_key}'")
                value = correct_val

        normalized[correct_key] = value

    # Special: sort "by" should be a list, and clean up tuple-like strings
    if action == "sort" and "by" in normalized:
        by_val = normalized["by"]
        if isinstance(by_val, str):
            normalized["by"] = [by_val]
        elif isinstance(by_val, list):
            # Clean tuple-like entries: ["(total_comp", "sum)"] → ["total_comp_sum"]
            cleaned = []
            i = 0
            while i < len(by_val):
                item = str(by_val[i]).strip("()")
                if i + 1 < len(by_val) and str(by_val[i]).startswith("("):
                    next_item = str(by_val[i + 1]).strip("()")
                    cleaned.append(f"{item}_{next_item}")
                    result.fix(idx, f"Merged tuple sort key ('{by_val[i]}', '{by_val[i+1]}') → '{item}_{next_item}'")
                    i += 2
                else:
                    cleaned.append(item)
                    i += 1
            normalized["by"] = cleaned

    # Special: math_horizontal must have "op" and "new_col"
    if action == "math_horizontal":
        if "op" not in normalized:
            normalized["op"] = "sum"
            result.fix(idx, "Added default op='sum' for math_horizontal")
        if "new_col" not in normalized:
            # Try to infer from other keys
            normalized["new_col"] = "result"
            result.fix(idx, "Added default new_col='result' for math_horizontal")

    # Special: sort must have "by" and "descending"
    if action == "sort":
        if "by" not in normalized:
            normalized["by"] = []
            result.fix(idx, "Added empty 'by' for sort")
        if "descending" not in normalized:
            normalized["descending"] = False

    # Special: groupby aggs values should be lists
    if action == "groupby" and "aggs" in normalized:
        aggs = normalized["aggs"]
        if isinstance(aggs, dict):
            for col, funcs in aggs.items():
                if isinstance(funcs, str):
                    aggs[col] = [funcs]
                    result.fix(idx, f"Wrapped aggs['{col}'] string '{funcs}' in list")

    return normalized


def _validate_columns(idx: int, action: str, config: dict, schema: dict, result: ValidationResult):
    """Check that columns in config exist in the current schema."""
    if not schema:
        return

    available = set(schema.keys())

    def check(col_name: str, context: str = ""):
        if col_name and col_name not in available:
            result.warn(idx, f"Column '{col_name}' not in schema{context}. Available: {sorted(available)}")

    def check_list(cols: list, context: str = ""):
        for c in (cols or []):
            check(c, context)

    # Single column references
    if "column" in config:
        check(config["column"])

    # List column references
    check_list(config.get("columns"))
    check_list(config.get("by"))
    check_list(config.get("group_cols"))
    check_list(config.get("left_cols"))
    check_list(config.get("features"))
    check_list(config.get("vec_a"))
    check_list(config.get("vec_b"))
    check_list(config.get("ordered_cols"))
    check_list(config.get("subset"))

    if "target" in config:
        check(config["target"])
    if "on" in config and isinstance(config["on"], str):
        check(config["on"])
    if "values" in config:
        check(config["values"])

    # Aggs keys
    if "aggs" in config and isinstance(config["aggs"], dict):
        for col in config["aggs"]:
            check(col, " (in aggs)")

    # Filter columns
    if "filters" in config and isinstance(config["filters"], list):
        for f in config["filters"]:
            if isinstance(f, dict):
                col = f.get("column", "")
                check(col, " (in filter)")
                # Type check: numeric ops need numeric columns
                op = f.get("operation", "")
                col_type = schema.get(col, "string")
                if op in NUMERIC_OPS and col_type == "string":
                    result.warn(idx, f"Filter op '{op}' on string column '{col}' — may fail. Consider column_type: 'string'")
                    # Auto-fix column_type
                    if col_type in ("integer", "float"):
                        f["column_type"] = "numeric"
                    else:
                        f["column_type"] = "string"
                    result.fix(idx, f"Auto-set column_type for '{col}' based on schema")

    # Mapping keys (rename)
    if "mapping" in config and isinstance(config["mapping"], dict):
        for old_name in config["mapping"]:
            check(old_name, " (rename source)")


def _compute_output_schema(action: str, config: dict, input_schema: dict) -> dict:
    """Compute what the schema looks like AFTER this step runs."""
    schema = dict(input_schema)

    if action == "select":
        cols = config.get("columns", [])
        if cols:
            schema = {c: schema.get(c, "string") for c in cols if c in schema}

    elif action == "drop":
        for c in config.get("columns", []):
            schema.pop(c, None)

    elif action == "rename":
        mapping = config.get("mapping", {})
        for old, new in mapping.items():
            if old in schema:
                schema[new] = schema.pop(old)

    elif action == "math_horizontal":
        new_col = config.get("new_col")
        if new_col:
            schema[new_col] = "float"

    elif action == "math_custom":
        suffix = config.get("new_suffix", "")
        for col in config.get("left_cols", []):
            schema[f"{col}_{suffix}"] = "float"

    elif action == "conditional":
        new_col = config.get("new_col")
        if new_col:
            schema[new_col] = "string"

    elif action == "groupby":
        group_cols = config.get("group_cols", [])
        aggs = config.get("aggs", {})
        new_schema = {c: input_schema.get(c, "string") for c in group_cols}
        for col, funcs in aggs.items():
            if isinstance(funcs, list):
                for func in funcs:
                    new_schema[f"{col}_{func}"] = "float"
            else:
                new_schema[f"{col}_{funcs}"] = "float"
        schema = new_schema

    elif action == "string_concat":
        new_col = config.get("new_col")
        if new_col:
            schema[new_col] = "string"

    elif action == "extract_date_parts":
        col = config.get("column", "")
        if col:
            schema[f"{col}_year"] = "integer"
            schema[f"{col}_month"] = "integer"
            schema[f"{col}_day"] = "integer"

    elif action == "cast":
        col = config.get("column", "")
        dtype = config.get("dtype", "str")
        type_map = {"int": "integer", "float": "float", "str": "string", "bool": "boolean"}
        if col in schema:
            schema[col] = type_map.get(dtype, "string")

    elif action == "math_multiply_bulk":
        suffix = config.get("suffix", "_adj")
        for col in config.get("columns", []):
            schema[f"{col}{suffix}"] = "float"

    return schema
