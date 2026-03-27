from fastapi import FastAPI, HTTPException, Body, Query, UploadFile, File, Depends
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Any, Optional, Union
from controllers.DataframeController import DataframeController
from controllers.WorkflowManager import WorkflowManager
from sqlalchemy.orm import Session
from database import get_db, create_tables
from models import Workflow, WorkflowPermission, WorkflowFile, WorkflowVersion, WorkflowRun, PermissionLevel
from middleware.security import InputValidationMiddleware, RateLimitMiddleware
from api.ai_workflow import router as ai_router
import os
import shutil
from datetime import datetime
import asyncio

app = FastAPI(title="Versor Complete Beast Mode API")

# Add CORS middleware first
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add security middleware after CORS
app.add_middleware(InputValidationMiddleware)
app.add_middleware(RateLimitMiddleware, requests_per_minute=100)

app.include_router(ai_router)

async def cleanup_task():
    while True:
        await asyncio.sleep(3600)  # Run every hour
        engine.cleanup_old_files(max_age_hours=24)

@app.on_event("startup")
async def startup_event():
    create_tables()
    engine.cleanup_old_files(max_age_hours=24)
    asyncio.create_task(cleanup_task())

engine = DataframeController()
workflow = WorkflowManager(engine)

# --- 1. IO & INGESTION ---

@app.post("/nodes/io/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    """Upload CSV file and create a new data source node"""
    try:
        res_id = engine.upload_csv(file)
        # Register the node with workflow manager
        workflow.nodes[res_id] = {
            "type": "upload_csv",
            "params": {"filename": file.filename},
            "parent": None,
            "children": []
        }
        metadata = engine.get_node_metadata(res_id)
        if isinstance(metadata, dict) and metadata.get("status") == "error":
            raise HTTPException(status_code=400, detail=metadata.get("msg", "Failed to get metadata"))
        return {"node_id": res_id, "metadata": metadata}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/io/upload_excel")
async def upload_excel(
    file: UploadFile = File(...),
    sheet_name: str = Query("Sheet1", description="Sheet name to read")
):
    """Upload Excel file and create a new data source node"""
    try:
        res_id = engine.upload_excel(file, sheet_name)
        # Register the node with workflow manager
        workflow.nodes[res_id] = {
            "type": "upload_excel",
            "params": {"filename": file.filename, "sheet_name": sheet_name},
            "parent": None,
            "children": []
        }
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/io/excel_sheets")
async def get_excel_sheets(file: UploadFile = File(...)):
    """Get list of sheet names from Excel file"""
    try:
        sheets = engine.get_excel_sheets(file)
        return {"sheets": sheets}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/io/read_from_db")
async def read_from_db(
    connection_string: str = Body(..., description="Database connection string"),
    query: str = Body(..., description="SQL query to execute")
):
    """Read data from database with parameterized queries"""
    try:
        # Validate connection string format
        if not connection_string or len(connection_string.strip()) == 0:
            raise HTTPException(status_code=400, detail="Connection string cannot be empty")
        
        # Basic SQL injection prevention - whitelist allowed operations
        query_lower = query.lower().strip()
        if not query_lower.startswith('select'):
            raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
        
        # Check for dangerous SQL keywords
        dangerous_keywords = ['drop', 'delete', 'insert', 'update', 'alter', 'create', 'truncate', 'exec', 'execute']
        if any(keyword in query_lower for keyword in dangerous_keywords):
            raise HTTPException(status_code=400, detail="Query contains prohibited SQL operations")
        
        res_id = engine.read_from_db(connection_string, query)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid query or connection: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database operation failed: {str(e)}")

# --- 2. COLUMN & ROW MANIPULATION ---

@app.get("/nodes/{node_id}/columns")
async def get_node_columns(node_id: str):
    """Get column names and types from a node"""
    try:
        if node_id not in engine.registry:
            raise HTTPException(status_code=404, detail="Node not found")
        schema = engine.registry[node_id].collect_schema()
        return {"columns": [name for name in schema.keys()]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/nodes/{node_id}/column_values")
async def get_column_values(node_id: str, column: str = Query(...)):
    """Get unique values from a column for filtering"""
    try:
        if node_id not in engine.registry:
            return {"values": []}
        
        df = engine.registry[node_id]
        schema = df.collect_schema()
        
        # Check if column exists
        if column not in schema.names():
            return {"values": []}
        
        # Get unique values, collect LazyFrame, and limit to 1000
        unique_vals = df.select(column).unique().sort(column).limit(1000).collect().to_dicts()
        values = [row[column] for row in unique_vals]
        
        return {"values": values}
    except Exception as e:
        return {"values": []}

@app.post("/nodes/transform/filter")
async def filter_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to filter"),
    operator: str = Body(..., description="Operator: ==, !=, >, <, >=, <=, contains, starts_with, ends_with, in"),
    value: Union[str, int, float, List[Union[str, int, float]]] = Body(..., description="Value(s) to compare")
):
    """Filter rows based on column condition"""
    try:
        # Build Polars expression based on operator
        if operator == "in":
            if not isinstance(value, list):
                value = [value]
            expression = f"pl.col('{column}').is_in({value})"
        elif operator in ["==", "!=", ">", "<", ">=", "<="]:
            expression = f"pl.col('{column}') {operator} {repr(value)}"
        elif operator == "contains":
            expression = f"pl.col('{column}').str.contains({repr(value)})"
        elif operator == "starts_with":
            expression = f"pl.col('{column}').str.starts_with({repr(value)})"
        elif operator == "ends_with":
            expression = f"pl.col('{column}').str.ends_with({repr(value)})"
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        
        res_id = workflow.create_node("filter_data", {"expression": expression}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/transform/select")
async def select_node(
    parent_id: str = Query(..., description="Parent node ID"),
    columns: List[str] = Body(..., description="List of columns to select")
):
    """Select specific columns"""
    try:
        res_id = workflow.create_node("select_columns", {"columns": columns}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/transform/safe_filter")
async def safe_filter_node(
    parent_id: str = Query(..., description="Parent node ID"),
    filters: List[Dict] = Body(..., description="List of filter conditions"),
    logic: str = Body("and", description="Combine filters with 'and' or 'or'")
):
    """Safe filter without eval"""
    try:
        res_id = workflow.create_node("safe_filter_data", {"filters": filters, "logic": logic}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/nodes/{node_id}/column_info")
async def get_column_info(node_id: str):
    """Get column names and types for building filters"""
    try:
        lf = engine.get_node_data(node_id)
        if lf is None:
            raise HTTPException(status_code=404, detail="Node not found")
        
        schema = lf.collect_schema()
        column_info = {}
        
        for col_name, col_type in schema.items():
            col_type_str = str(col_type)
            is_numeric = col_type_str in ['Int64', 'Int32', 'Float64', 'Float32', 'Int8', 'Int16', 'UInt8', 'UInt16', 'UInt32', 'UInt64']
            
            info = {
                "type": "numeric" if is_numeric else "string",
                "dtype": col_type_str
            }
            
            if not is_numeric:
                try:
                    unique_vals = lf.select(pl.col(col_name)).unique().limit(100).collect().to_series().to_list()
                    info["unique_values"] = [str(v) for v in unique_vals[:100]]
                except:
                    info["unique_values"] = []
            
            column_info[col_name] = info
        
        return {"columns": column_info}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/transform/drop")
async def drop_columns_node(
    parent_id: str = Query(..., description="Parent node ID"),
    columns: List[str] = Body(..., description="List of columns to drop")
):
    """Drop specific columns"""
    try:
        res_id = workflow.create_node("drop_columns", {"columns": columns}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/transform/sort")
async def sort_node(
    parent_id: str = Query(..., description="Parent node ID"),
    by: List[str] = Body(..., description="Columns to sort by"),
    descending: bool = Body(False, description="Sort in descending order")
):
    """Sort data by columns"""
    try:
        res_id = workflow.create_node("sort_data", {"by": by, "descending": descending}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/transform/rename")
async def rename_cols(
    parent_id: str = Query(..., description="Parent node ID"),
    mapping: Dict[str, str] = Body(..., description="Column rename mapping")
):
    """Rename columns using mapping dictionary"""
    try:
        res_id = workflow.create_node("rename_columns", {"mapping": mapping}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/transform/reorder")
async def reorder_columns_node(
    parent_id: str = Query(..., description="Parent node ID"),
    ordered_cols: List[str] = Body(..., description="Columns in desired order")
):
    """Reorder columns"""
    try:
        res_id = workflow.create_node("reorder_columns", {"ordered_cols": ordered_cols}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 3. STRING OPERATIONS ---

@app.post("/nodes/string/case")
async def change_case(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to transform"),
    mode: str = Body("upper", description="Case mode: upper, lower, title"),
    new_col: Optional[str] = Body(None, description="New column name (optional)")
):
    """Change string case"""
    try:
        method_map = {"upper": "str_to_upper", "lower": "str_to_lower", "title": "str_to_title"}
        params = {"column": column}
        if new_col:
            params["new_col"] = new_col
        res_id = workflow.create_node(method_map[mode], params, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/string/slice")
async def string_slice(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to slice"),
    n_chars: int = Body(..., description="Number of characters"),
    mode: str = Body("left", description="Slice mode: left, right"),
    new_col: Optional[str] = Body(None, description="New column name (optional)")
):
    """Extract left/right characters from string"""
    try:
        method = "str_left" if mode == "left" else "str_right"
        params = {"column": column, "n_chars": n_chars}
        if new_col:
            params["new_col"] = new_col
        res_id = workflow.create_node(method, params, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/string/mid")
async def string_mid(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to slice"),
    start: int = Body(..., description="Start position (1-indexed)"),
    length: int = Body(..., description="Length of substring"),
    new_col: Optional[str] = Body(None, description="New column name (optional)")
):
    """Extract middle characters from string (Excel MID function)"""
    try:
        params = {"column": column, "start": start, "length": length}
        if new_col:
            params["new_col"] = new_col
        res_id = workflow.create_node("str_mid", params, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/string/concat")
async def concat_columns_node(
    parent_id: str = Query(..., description="Parent node ID"),
    columns: List[str] = Body(..., description="Columns to concatenate"),
    separator: str = Body("", description="Separator between columns"),
    new_col: str = Body("concatenated", description="New column name")
):
    """Concatenate multiple columns"""
    try:
        res_id = workflow.create_node("concat_columns", {"columns": columns, "separator": separator, "new_col": new_col}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/string/prefix_suffix")
async def concat_with_literal_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to modify"),
    prefix: str = Body("", description="Text to add at start"),
    suffix: str = Body("", description="Text to add at end"),
    new_col: Optional[str] = Body(None, description="New column name (optional)")
):
    """Add prefix/suffix to column values"""
    try:
        params = {"column": column, "prefix": prefix, "suffix": suffix}
        if new_col:
            params["new_col"] = new_col
        res_id = workflow.create_node("concat_with_literal", params, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/string/clean")
async def clean_string_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to clean")
):
    """Clean string column (trim + uppercase)"""
    try:
        res_id = workflow.create_node("clean_string_column", {"column": column}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 4. MATH OPERATIONS ---

@app.post("/nodes/math/horizontal")
async def horizontal_op(
    parent_id: str = Query(..., description="Parent node ID"),
    columns: List[str] = Body(..., description="Columns for calculation"),
    new_col: str = Body(..., description="New column name"),
    op: str = Body("sum", description="Operation: sum or average")
):
    """Perform horizontal math operations"""
    try:
        method = "horizontal_sum" if op == "sum" else "horizontal_average"
        res_id = workflow.create_node(method, {"columns": columns, "new_col": new_col}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/math/custom")
async def custom_expr(
    parent_id: str = Query(..., description="Parent node ID"),
    left_cols: List[str] = Body(..., description="Left operand columns"),
    op: str = Body(..., description="Operator: +, -, *, /"),
    right_val: Union[str, float] = Body(..., description="Right operand (column name or constant)"),
    new_suffix: str = Body(..., description="Suffix for new columns")
):
    """Apply custom mathematical expression"""
    try:
        params = {"left_cols": left_cols, "op": op, "right_val": right_val, "new_suffix": new_suffix}
        res_id = workflow.create_node("apply_custom_expression", params, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/math/multiply_bulk")
async def multi_column_multiply_node(
    parent_id: str = Query(..., description="Parent node ID"),
    columns: List[str] = Body(..., description="Columns to multiply"),
    factor: float = Body(..., description="Multiplication factor"),
    suffix: str = Body("_adj", description="Suffix for new columns")
):
    """Multiply multiple columns by a factor"""
    try:
        res_id = workflow.create_node("multi_column_multiply", {"columns": columns, "factor": factor, "suffix": suffix}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 5. VECTOR OPERATIONS ---

@app.post("/nodes/vector/dot_product")
async def vector_dot_product_node(
    parent_id: str = Query(..., description="Parent node ID"),
    vec_a: List[str] = Body(..., description="First vector columns"),
    vec_b: List[str] = Body(..., description="Second vector columns"),
    new_col: str = Body(..., description="Result column name")
):
    """Calculate dot product of two vectors"""
    try:
        res_id = workflow.create_node("vector_dot_product", {"vec_a": vec_a, "vec_b": vec_b, "new_col": new_col}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/vector/linear_multiply")
async def vector_linear_multiply_node(
    parent_id: str = Query(..., description="Parent node ID"),
    vec_a: List[str] = Body(..., description="First vector columns"),
    vec_b: List[str] = Body(..., description="Second vector columns"),
    suffix: str = Body("_prod", description="Suffix for result columns")
):
    """Element-wise multiplication of two vectors"""
    try:
        res_id = workflow.create_node("vector_linear_multiply", {"vec_a": vec_a, "vec_b": vec_b, "suffix": suffix}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/vector/cross_product")
async def vector_cross_product_node(
    parent_id: str = Query(..., description="Parent node ID"),
    vec_a: List[str] = Body(..., description="First 3D vector columns"),
    vec_b: List[str] = Body(..., description="Second 3D vector columns"),
    prefix: str = Body("cross", description="Prefix for result columns")
):
    """Calculate cross product of two 3D vectors"""
    try:
        res_id = workflow.create_node("vector_cross_product", {"vec_a": vec_a, "vec_b": vec_b, "prefix": prefix}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 6. DATA CLEANING ---

@app.post("/nodes/clean/drop_na")
async def drop_na_node(
    parent_id: str = Query(..., description="Parent node ID"),
    subset: Optional[List[str]] = Body(None, description="Columns to check for nulls (optional)")
):
    """Drop rows with null values"""
    try:
        params = {}
        if subset:
            params["subset"] = subset
        res_id = workflow.create_node("drop_na", params, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/clean/drop_nulls")
async def drop_nulls_node(
    parent_id: str = Query(..., description="Parent node ID")
):
    """Drop all rows with any null values"""
    try:
        res_id = workflow.create_node("drop_nulls", {}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/clean/drop_duplicates")
async def drop_duplicates_node(
    parent_id: str = Query(..., description="Parent node ID"),
    subset: Optional[List[str]] = Body(None, description="Columns to check for duplicates (optional)")
):
    """Drop duplicate rows"""
    try:
        params = {}
        if subset:
            params["subset"] = subset
        res_id = workflow.create_node("drop_duplicates", params, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/clean/fill_missing")
async def fill_missing_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to fill"),
    value: Any = Body(..., description="Fill value")
):
    """Fill missing values with specified value"""
    try:
        res_id = workflow.create_node("fill_missing", {"column": column, "value": value}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 7. DATA TYPE OPERATIONS ---

@app.post("/nodes/dtype/cast")
async def cast_column_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to cast"),
    dtype: str = Body(..., description="Target data type: int, float, str, bool")
):
    """Cast column to different data type"""
    try:
        res_id = workflow.create_node("cast_column", {"column": column, "dtype": dtype}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 8. DATE/TIME OPERATIONS ---

@app.post("/nodes/datetime/extract_parts")
async def extract_date_parts_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Date column to extract from")
):
    """Extract year, month, day from date column"""
    try:
        res_id = workflow.create_node("extract_date_parts", {"column": column}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 9. JOINS & SET OPERATIONS ---

@app.post("/nodes/combine/join")
async def join_node(
    left_id: str = Query(..., description="Left node ID"),
    right_id: str = Query(..., description="Right node ID"),
    on: str = Body(..., description="Join column"),
    how: str = Body("inner", description="Join type: inner, left, outer, semi, anti")
):
    """Join two datasets"""
    try:
        params = {"right_id": right_id, "on": on, "how": how}
        res_id = workflow.create_node("join_nodes", params, parent_id=left_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/combine/union")
async def union_node(
    node_ids: List[str] = Body(..., description="List of node IDs to union")
):
    """Union multiple datasets vertically"""
    try:
        res_id = workflow.create_node("union_nodes", {"node_ids": node_ids}, parent_id=None)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/combine/append")
async def append_vertical_node(
    top_node_id: str = Query(..., description="Top node ID (first dataframe)"),
    bottom_node_id: str = Query(..., description="Bottom node ID (dataframe to append below)"),
    how: str = Body("vertical", description="Append strategy: vertical (strict), vertical_relaxed (by name), diagonal (union all columns)")
):
    """Append/stack one dataframe below another (vertical concatenation).
    
    - vertical: Strict column matching (same columns in same order)
    - vertical_relaxed: Match columns by name, fill missing with nulls
    - diagonal: Union of all columns from both dataframes, fill missing with nulls
    """
    try:
        res_id = engine.append_vertical(top_node_id, bottom_node_id, how)
        # Register with workflow manager
        workflow.nodes[res_id] = {
            "type": "append_vertical",
            "params": {"top_node_id": top_node_id, "bottom_node_id": bottom_node_id, "how": how},
            "parent": top_node_id,
            "children": []
        }
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 10. ADVANCED ANALYTICS ---

@app.post("/nodes/advanced/outliers")
async def filter_outliers(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to check for outliers")
):
    """Remove outliers using IQR method"""
    try:
        res_id = workflow.create_node("filter_outliers_iqr", {"column": column}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/advanced/groupby")
async def group_by_node(
    parent_id: str = Query(..., description="Parent node ID"),
    group_cols: List[str] = Body(..., description="Columns to group by"),
    aggs: Dict[str, List[str]] = Body(..., description="Aggregation functions per column")
):
    """Group by columns and apply aggregations"""
    try:
        res_id = workflow.create_node("group_by_agg", {"group_cols": group_cols, "aggs": aggs}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/advanced/stats")
async def col_stats_node(
    parent_id: str = Query(..., description="Parent node ID"),
    columns: List[str] = Body(..., description="Columns to calculate statistics for")
):
    """Calculate advanced statistics for columns"""
    try:
        res_id = workflow.create_node("col_stats_advanced", {"columns": columns}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/advanced/pivot")
async def pivot_table_node(
    parent_id: str = Query(..., description="Parent node ID"),
    values: str = Body(..., description="Values column"),
    index: List[str] = Body(..., description="Index columns"),
    on: str = Body(..., description="Pivot column"),
    agg: str = Body("sum", description="Aggregation function")
):
    """Create pivot table"""
    try:
        res_id = workflow.create_node("pivot_table", {"values": values, "index": index, "on": on, "agg": agg}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/advanced/moving_average")
async def moving_average_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to calculate moving average"),
    window: int = Body(..., description="Window size")
):
    """Calculate moving average"""
    try:
        res_id = workflow.create_node("moving_average", {"column": column, "window": window}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/advanced/conditional")
async def conditional_column_node(
    parent_id: str = Query(..., description="Parent node ID"),
    column: str = Body(..., description="Column to evaluate"),
    op: str = Body(..., description="Comparison operator: gt, gte, lt, lte, eq, ne"),
    threshold: Any = Body(..., description="Value to compare against"),
    then_val: Any = Body(..., description="Value when condition is true"),
    else_val: Any = Body(..., description="Value when condition is false"),
    new_col: str = Body(..., description="New column name")
):
    """Create conditional column (IF-THEN-ELSE) — safe, no eval()"""
    try:
        # Validate operator against whitelist before hitting the controller
        SAFE_OPS = {'gt', 'gte', 'lt', 'lte', 'eq', 'ne'}
        if op not in SAFE_OPS:
            raise HTTPException(status_code=400, detail=f"Invalid operator '{op}'. Allowed: {sorted(SAFE_OPS)}")
        res_id = workflow.create_node("conditional_column", {
            "column": column, "op": op, "threshold": threshold,
            "then_val": then_val, "else_val": else_val, "new_col": new_col
        }, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 11. MATRIX OPERATIONS ---

@app.post("/nodes/matrix/transpose")
async def matrix_transpose_node(
    parent_id: str = Query(..., description="Parent node ID")
):
    """Transpose the dataframe (rows become columns)"""
    try:
        res_id = workflow.create_node("matrix_transpose", {}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 12. MACHINE LEARNING ---

@app.post("/nodes/ml/linear_regression")
async def linear_regression_node(
    parent_id: str = Query(..., description="Parent node ID"),
    target: str = Body(..., description="Target column"),
    features: List[str] = Body(..., description="Feature columns")
):
    """Perform linear regression and store coefficients as a node"""
    try:
        res_id = workflow.create_node("linear_regression_node", {"target": target, "features": features}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/ml/logistic_prediction")
async def logistic_prediction_node(
    parent_id: str = Query(..., description="Parent node ID"),
    features: List[str] = Body(..., description="Feature columns"),
    weights: List[float] = Body(..., description="Model weights")
):
    """Apply logistic regression prediction — appends 'probability' column to the dataframe"""
    try:
        res_id = workflow.create_node("logistic_regression_prediction", {"features": features, "weights": weights}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/nodes/ml/correlation")
async def correlation_node(
    parent_id: str = Query(..., description="Parent node ID"),
    columns: List[str] = Body(..., description="Numeric columns to correlate")
):
    """Compute pairwise Pearson correlations as a tidy 1-D dataframe.
    Output columns: col_a, col_b, correlation.
    Fully chainable — downstream nodes receive a proper dataframe."""
    try:
        if len(columns) < 2:
            raise HTTPException(status_code=400, detail="At least 2 columns required for correlation")
        res_id = workflow.create_node("correlation_matrix_1d", {"columns": columns}, parent_id)
        return {"node_id": res_id, "metadata": engine.get_node_metadata(res_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 13. NODE INSPECTION & MANAGEMENT ---

@app.delete("/nodes/{node_id}")
async def clear_node_data(node_id: str):
    """Clear/reset node data and remove from registry"""
    try:
        if node_id in engine.registry:
            del engine.registry[node_id]
        
        if node_id in workflow.nodes:
            del workflow.nodes[node_id]
        
        return {"status": "success", "message": f"Node {node_id} cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/nodes/{node_id}")
async def inspect_node(
    node_id: str,
    limit: int = Query(50, description="Number of rows to preview")
):
    """Get node metadata and data preview"""
    try:
        # Check if node exists in workflow manager first
        if node_id not in workflow.nodes and node_id not in engine.registry:
            raise HTTPException(status_code=404, detail="Node not found. Please execute the node first.")
        
        metadata = engine.get_node_metadata(node_id, n_preview=limit)
        if isinstance(metadata, dict) and metadata.get("status") == "error":
            raise HTTPException(status_code=404, detail=metadata.get("msg", "Node execution failed"))
        return metadata
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/nodes/{node_id}/download")
async def download_node_data(
    node_id: str,
    format: str = Query("csv", description="Export format: csv, parquet, excel")
):
    """Download node data in specified format"""
    try:
        # Check if node exists in workflow manager first
        if node_id not in workflow.nodes and node_id not in engine.registry:
            raise HTTPException(status_code=404, detail="Node not found. Please execute the node first.")
        
        buffer = engine.export_node_to_buffer(node_id, format)
        
        # Map format to correct file extension
        ext_map = {"csv": "csv", "parquet": "parquet", "excel": "xlsx"}
        file_ext = ext_map.get(format.lower(), format.lower())
        
        media_type = "text/csv" if format == "csv" else "application/octet-stream"
        return StreamingResponse(
            buffer,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename=result_{node_id}.{file_ext}"}
        )
    except HTTPException:
        raise
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/nodes/{node_id}/cache")
async def cache_node(
    node_id: str,
    ttl: int = Query(3600, description="Cache TTL in seconds")
):
    """Cache node data to Redis"""
    try:
        result = engine.cache_to_redis(node_id, ttl)
        return {"status": "success", "message": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 14. WORKFLOW VALIDATION ---

@app.post("/workflow/validate-connection")
async def validate_connection(
    parent_id: str = Query(..., description="Parent node ID"),
    child_type: str = Body(..., description="Child node type"),
    child_params: Dict = Body(..., description="Child node parameters")
):
    """Validate if a connection between nodes is valid"""
    try:
        # Get parent schema
        parent_schema = engine.registry[parent_id].collect_schema()
        
        # Basic validation: check if columns in params exist in parent schema
        if "column" in child_params:
            if child_params["column"] not in parent_schema:
                return {"valid": False, "reason": f"Column {child_params['column']} missing."}
        
        if "columns" in child_params:
            missing_cols = [col for col in child_params["columns"] if col not in parent_schema]
            if missing_cols:
                return {"valid": False, "reason": f"Columns missing: {missing_cols}"}
        
        return {"valid": True}
    except Exception as e:
        return {"valid": False, "reason": str(e)}

# --- 15. WORKFLOW EXPORT/IMPORT ---

@app.get("/workflow/export")
async def export_workflow():
    """Export entire workflow as JSON"""
    try:
        import json
        from fastapi.responses import Response
        
        graph_data = workflow.nodes
        content = json.dumps(graph_data, indent=4)
        return Response(
            content=content,
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=workflow.json"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 16. HEALTH CHECK ---

@app.get("/health")
async def health_check():
    """API health check"""
    return {
        "status": "healthy",
        "nodes_in_registry": len(engine.registry),
        "workflow_nodes": len(workflow.nodes)
    }

# --- 17. WORKFLOW MANAGEMENT ---

@app.post("/workflows")
async def create_workflow(
    workflow_id: str = Body(...),
    name: str = Body(...),
    creator_id: int = Body(...),
    creator_username: str = Body(...),
    db: Session = Depends(get_db)
):
    """Create a new workflow"""
    try:
        wf = Workflow(
            id=workflow_id,
            name=name,
            creator_id=creator_id,
            creator_username=creator_username,
            last_run="PENDING"
        )
        db.add(wf)
        db.commit()
        return {"workflow_id": workflow_id, "status": "created"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows")
async def get_user_workflows(
    user_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Get all workflows accessible by user"""
    try:
        # Get workflows created by user
        created = db.query(Workflow).filter(Workflow.creator_id == user_id).all()
        
        # Get workflows where user has permissions
        perms = db.query(WorkflowPermission).filter(WorkflowPermission.user_id == user_id).all()
        shared_wf_ids = [p.workflow_id for p in perms]
        shared = db.query(Workflow).filter(Workflow.id.in_(shared_wf_ids)).all() if shared_wf_ids else []
        
        result = []
        for wf in created:
            result.append({
                "id": wf.id,
                "name": wf.name,
                "owner": wf.creator_username,
                "lastRun": wf.last_run,
                "lastModified": wf.updated_at.isoformat(),
                "role": "creator",
                "workflow_data": wf.workflow_data
            })
        
        for wf in shared:
            perm = next((p for p in perms if p.workflow_id == wf.id), None)
            result.append({
                "id": wf.id,
                "name": wf.name,
                "owner": wf.creator_username,
                "lastRun": wf.last_run,
                "lastModified": wf.updated_at.isoformat(),
                "role": perm.permission_level.value if perm else "viewer",
                "workflow_data": wf.workflow_data
            })
        
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/workflows/{workflow_id}")
async def update_workflow(
    workflow_id: str,
    name: Optional[str] = Body(None),
    last_run: Optional[str] = Body(None),
    workflow_data: Optional[str] = Body(None),
    db: Session = Depends(get_db)
):
    """Update workflow metadata"""
    try:
        wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
        if not wf:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        if name:
            wf.name = name
        if last_run:
            wf.last_run = last_run
        if workflow_data:
            wf.workflow_data = workflow_data
        wf.updated_at = datetime.utcnow()
        
        db.commit()
        return {"status": "updated"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/workflows/{workflow_id}")
async def delete_workflow(
    workflow_id: str,
    user_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Delete workflow (creator only)"""
    try:
        wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
        if not wf:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        if wf.creator_id != user_id:
            raise HTTPException(status_code=403, detail="Only creator can delete workflow")
        
        db.delete(wf)
        db.commit()
        return {"status": "deleted"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/workflows/{workflow_id}/permissions")
async def add_workflow_permission(
    workflow_id: str,
    user_id: int = Body(...),
    username: str = Body(...),
    permission_level: str = Body(...),
    db: Session = Depends(get_db)
):
    """Add user permission to workflow"""
    try:
        wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
        if not wf:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        # Check if permission already exists
        existing = db.query(WorkflowPermission).filter(
            WorkflowPermission.workflow_id == workflow_id,
            WorkflowPermission.user_id == user_id
        ).first()
        
        if existing:
            existing.permission_level = PermissionLevel[permission_level.upper()]
        else:
            perm = WorkflowPermission(
                workflow_id=workflow_id,
                user_id=user_id,
                username=username,
                permission_level=PermissionLevel[permission_level.upper()]
            )
            db.add(perm)
        
        db.commit()
        return {"status": "permission added"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows/{workflow_id}/permissions")
async def get_workflow_permissions(
    workflow_id: str,
    db: Session = Depends(get_db)
):
    """Get all permissions for a workflow"""
    try:
        perms = db.query(WorkflowPermission).filter(WorkflowPermission.workflow_id == workflow_id).all()
        return [{
            "user_id": p.user_id,
            "username": p.username,
            "permission_level": p.permission_level.value
        } for p in perms]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/workflows/{workflow_id}/permissions/{user_id}")
async def remove_workflow_permission(
    workflow_id: str,
    user_id: int,
    db: Session = Depends(get_db)
):
    """Remove user permission from workflow"""
    try:
        perm = db.query(WorkflowPermission).filter(
            WorkflowPermission.workflow_id == workflow_id,
            WorkflowPermission.user_id == user_id
        ).first()
        
        if not perm:
            raise HTTPException(status_code=404, detail="Permission not found")
        
        db.delete(perm)
        db.commit()
        return {"status": "permission removed"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows/{workflow_id}/access")
async def check_workflow_access(
    workflow_id: str,
    user_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Check user's access level to workflow"""
    try:
        wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
        if not wf:
            return {"access": "none"}
        
        if wf.creator_id == user_id:
            return {"access": "creator", "can_edit": True, "can_run": True, "can_view": True}
        
        perm = db.query(WorkflowPermission).filter(
            WorkflowPermission.workflow_id == workflow_id,
            WorkflowPermission.user_id == user_id
        ).first()
        
        if not perm:
            return {"access": "none"}
        
        level = perm.permission_level.value
        return {
            "access": level,
            "can_edit": level in ["creator", "editor"],
            "can_run": level in ["creator", "editor", "runner"],
            "can_view": True
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/workflows/{workflow_id}/transfer")
async def transfer_workflow_ownership(
    workflow_id: str,
    new_owner_id: int = Body(...),
    new_owner_username: str = Body(...),
    current_owner_id: int = Body(...),
    db: Session = Depends(get_db)
):
    """Transfer workflow ownership to another user"""
    try:
        wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
        if not wf:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        if wf.creator_id != current_owner_id:
            raise HTTPException(status_code=403, detail="Only creator can transfer ownership")
        
        # Remove new owner from permissions if they exist
        existing_perm = db.query(WorkflowPermission).filter(
            WorkflowPermission.workflow_id == workflow_id,
            WorkflowPermission.user_id == new_owner_id
        ).first()
        if existing_perm:
            db.delete(existing_perm)
        
        # Add current owner as editor
        new_perm = WorkflowPermission(
            workflow_id=workflow_id,
            user_id=current_owner_id,
            username=wf.creator_username,
            permission_level=PermissionLevel.EDITOR
        )
        db.add(new_perm)
        
        # Update workflow creator
        wf.creator_id = new_owner_id
        wf.creator_username = new_owner_username
        wf.updated_at = datetime.utcnow()
        
        db.commit()
        return {"status": "ownership transferred"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/workflows/{workflow_id}/files")
async def save_workflow_file(
    workflow_id: str,
    node_id: str = Body(...),
    backend_node_id: str = Body(...),
    filename: str = Body(...),
    saved_by: str = Body(...),
    rows: int = Body(0),
    columns: int = Body(0),
    db: Session = Depends(get_db)
):
    """Save workflow file metadata"""
    try:
        file_record = WorkflowFile(
            workflow_id=workflow_id,
            node_id=node_id,
            backend_node_id=backend_node_id,
            filename=filename,
            saved_by=saved_by,
            rows=rows,
            columns=columns
        )
        db.add(file_record)
        db.commit()
        db.refresh(file_record)
        return {"file_id": file_record.id, "status": "saved"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows/{workflow_id}/files")
async def get_workflow_files(
    workflow_id: str,
    db: Session = Depends(get_db)
):
    """Get all saved files for a workflow"""
    try:
        files = db.query(WorkflowFile).filter(WorkflowFile.workflow_id == workflow_id).order_by(WorkflowFile.created_at.desc()).all()
        return [{
            "id": f.id,
            "node_id": f.node_id,
            "backend_node_id": f.backend_node_id,
            "filename": f.filename,
            "saved_by": f.saved_by,
            "rows": f.rows,
            "columns": f.columns,
            "created_at": f.created_at.isoformat()
        } for f in files]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows/files/{file_id}/download")
async def download_workflow_file(
    file_id: int,
    db: Session = Depends(get_db)
):
    """Download saved workflow file"""
    try:
        file_record = db.query(WorkflowFile).filter(WorkflowFile.id == file_id).first()
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")
        
        buffer = engine.export_node_to_buffer(file_record.backend_node_id, "csv")
        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={file_record.filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/workflows/{workflow_id}/run")
async def mark_workflow_run(
    workflow_id: str,
    status: str = Body(..., description="Run status: RUNNING, SUCCESS, ERROR"),
    db: Session = Depends(get_db)
):
    """Update workflow run status"""
    try:
        wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
        if not wf:
            raise HTTPException(status_code=404, detail="Workflow not found")
        wf.last_run = status
        wf.updated_at = datetime.utcnow()
        db.commit()
        return {"status": "updated", "last_run": status}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/workflows/{workflow_id}/versions")
async def save_workflow_version(
    workflow_id: str,
    workflow_data: str = Body(...),
    created_by: str = Body(...),
    comment: str = Body(""),
    db: Session = Depends(get_db)
):
    """Save a new version of the workflow"""
    try:
        max_version = db.query(WorkflowVersion).filter(
            WorkflowVersion.workflow_id == workflow_id
        ).count()
        
        version = WorkflowVersion(
            workflow_id=workflow_id,
            version_number=max_version + 1,
            workflow_data=workflow_data,
            created_by=created_by,
            comment=comment
        )
        db.add(version)
        db.commit()
        db.refresh(version)
        return {"version_id": version.id, "version_number": version.version_number}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows/{workflow_id}/versions")
async def get_workflow_versions(
    workflow_id: str,
    db: Session = Depends(get_db)
):
    """Get all versions of a workflow"""
    try:
        versions = db.query(WorkflowVersion).filter(
            WorkflowVersion.workflow_id == workflow_id
        ).order_by(WorkflowVersion.version_number.desc()).all()
        
        return [{
            "id": v.id,
            "version_number": v.version_number,
            "created_by": v.created_by,
            "created_at": v.created_at.isoformat(),
            "comment": v.comment
        } for v in versions]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows/{workflow_id}/versions/{version_id}/download")
async def download_workflow_version(
    workflow_id: str,
    version_id: int,
    db: Session = Depends(get_db)
):
    """Download a specific workflow version"""
    try:
        version = db.query(WorkflowVersion).filter(
            WorkflowVersion.id == version_id,
            WorkflowVersion.workflow_id == workflow_id
        ).first()
        
        if not version:
            raise HTTPException(status_code=404, detail="Version not found")
        
        return Response(
            content=version.workflow_data,
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=workflow_{workflow_id}_v{version.version_number}.json"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 19. RUN HISTORY ---

@app.post("/workflows/{workflow_id}/runs")
async def create_workflow_run(
    workflow_id: str,
    version_id: Optional[int] = Body(None),
    run_by: str = Body(...),
    status: str = Body(...),
    saved_nodes: str = Body("[]"),
    db: Session = Depends(get_db)
):
    """Record a workflow run"""
    try:
        import json
        run = WorkflowRun(
            workflow_id=workflow_id,
            version_id=version_id,
            run_by=run_by,
            status=status,
            saved_nodes=saved_nodes
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        return {"run_id": run.id, "status": "recorded"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/workflows/{workflow_id}/versions/{version_id}/runs")
async def get_version_runs(
    workflow_id: str,
    version_id: int,
    db: Session = Depends(get_db)
):
    """Get all runs for a specific workflow version"""
    try:
        runs = db.query(WorkflowRun).filter(
            WorkflowRun.workflow_id == workflow_id,
            WorkflowRun.version_id == version_id
        ).order_by(WorkflowRun.run_at.desc()).all()
        return [{
            "id": r.id,
            "run_by": r.run_by,
            "run_at": r.run_at.isoformat(),
            "status": r.status,
            "saved_nodes": r.saved_nodes
        } for r in runs]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))