"""
SchemaInferrer — extracts column names and Polars dtypes from raw CSV bytes.
Returns a dict like: {"salary": "numeric", "name": "string", "hire_date": "date"}
"""

import io
import polars as pl


def infer_schema(raw_bytes: bytes, filename: str = "file.csv") -> dict:
    """Read up to 100 rows to infer column types. Returns {col: type_category}."""
    try:
        df = pl.read_csv(io.BytesIO(raw_bytes), n_rows=100)
    except Exception:
        return {}

    schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype in ("Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"):
            schema[col] = "integer"
        elif dtype in ("Float32", "Float64"):
            schema[col] = "float"
        elif dtype in ("Date", "Datetime", "Time", "Duration"):
            schema[col] = "date"
        elif dtype in ("Boolean",):
            schema[col] = "boolean"
        else:
            schema[col] = "string"
    return schema


def schema_to_prompt_block(file_schemas: list[dict]) -> str:
    """Format file schemas for the LLM prompt."""
    lines = []
    for s in file_schemas:
        cols = s.get("schema", s.get("columns", {}))
        if isinstance(cols, dict):
            col_str = ", ".join(f"{c} ({t})" for c, t in cols.items())
        elif isinstance(cols, list):
            col_str = ", ".join(cols)
        else:
            col_str = str(cols)
        lines.append(f"  File: {s['name']}\n  Columns: {col_str}")
    return "\n".join(lines)
