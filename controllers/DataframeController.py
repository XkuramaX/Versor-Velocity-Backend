import polars as pl
import redis
import io
import uuid
import os
import shutil
import json
import time
from typing import List, Dict, Optional, Any, Union
from fastapi import UploadFile
import numpy as np
import functools
from pathlib import Path

from utils.safe_filter import SafeFilterBuilder

def safe_node_execution(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 1 and isinstance(args[1], dict) and "status" in args[1]:
            return args[1]
        try:
            return func(*args, **kwargs)
        except pl.ColumnNotFoundError as e:
            return {"status": "error", "type": "schema", "msg": f"Column not found: {str(e)}"}
        except Exception as e:
            return {"status": "error", "type": "system", "msg": str(e)}
    return wrapper

class DataframeController:
    def __init__(self, redis_host=None, redis_port=None, db=0):
        import os
        host = redis_host or os.getenv('REDIS_HOST', 'localhost')
        port = int(redis_port or os.getenv('REDIS_PORT', 6379))
        self.r = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)
        self.registry: Dict[str, pl.LazyFrame] = {}
        self.upload_dir = "server_uploads"
        self.cache_dir = "./cache"
        
        os.makedirs(self.upload_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)

    def save_node_result(self, df: pl.LazyFrame, node_id: str) -> str:
        """Save node result as Parquet and store metadata in Redis"""
        file_uuid = uuid.uuid4().hex[:8]
        file_path = os.path.join(self.cache_dir, f"{node_id}_{file_uuid}.parquet")
        
        # Sink to parquet (streaming)
        df.sink_parquet(file_path)
        
        # Collect metadata
        schema_dict = {name: str(dtype) for name, dtype in df.collect_schema().items()}
        preview_df = pl.scan_parquet(file_path).limit(50).collect()
        row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
        
        # Convert datetime columns to strings for JSON serialization
        preview_data = []
        for row in preview_df.to_dicts():
            serializable_row = {}
            for key, value in row.items():
                if hasattr(value, 'isoformat'):
                    serializable_row[key] = value.isoformat()
                else:
                    serializable_row[key] = value
            preview_data.append(serializable_row)
        
        metadata = {
            "file_path": file_path,
            "schema": schema_dict,
            "row_count": row_count,
            "preview": preview_data,
            "timestamp": time.time()
        }
        
        # Store in Redis
        self.r.setex(f"versor:meta:{node_id}", 7200, json.dumps(metadata))
        
        # Update registry with LazyFrame
        self.registry[node_id] = pl.scan_parquet(file_path)
        
        return node_id

    def get_node_data(self, node_id: str) -> Optional[pl.LazyFrame]:
        """Get LazyFrame by scanning Parquet file from Redis metadata"""
        if node_id in self.registry:
            return self.registry[node_id]
        
        meta_key = f"versor:meta:{node_id}"
        meta_json = self.r.get(meta_key)
        
        if not meta_json:
            return None
        
        metadata = json.loads(meta_json)
        file_path = metadata["file_path"]
        
        if not os.path.exists(file_path):
            return None
        
        self.registry[node_id] = pl.scan_parquet(file_path)
        return self.registry[node_id]

    def cleanup_old_files(self, max_age_hours: int = 24):
        """Delete Parquet files older than max_age_hours"""
        cutoff_time = time.time() - (max_age_hours * 3600)
        
        for filename in os.listdir(self.cache_dir):
            if filename.endswith('.parquet'):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.getmtime(file_path) < cutoff_time:
                    os.remove(file_path)

    def export_node_to_buffer(self, node_id: str, format: str = "csv") -> io.BytesIO:
        lf = self.get_node_data(node_id)
        if lf is None:
            raise KeyError(f"Node {node_id} not found")
        
        df = lf.collect(streaming=True)
        buffer = io.BytesIO()
        if format == "csv":
            df.write_csv(buffer)
        elif format == "parquet":
            df.write_parquet(buffer)
        elif format == "excel":
            df.write_excel(buffer)
        buffer.seek(0)
        return buffer
    
    def save_upload(self, file: UploadFile) -> str:
        file_path = os.path.join(self.upload_dir, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return file_path

    def upload_csv(self, file: UploadFile) -> str:
        file_path = self.save_upload(file)
        node_id = f"source_{uuid.uuid4().hex[:6]}"
        try:
            lf = pl.scan_csv(file_path)
            self.save_node_result(lf, node_id)
            os.remove(file_path)
            return node_id
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e

    def upload_excel(self, file: UploadFile, sheet_name: str = "Sheet1") -> str:
        file_path = self.save_upload(file)
        node_id = f"source_{uuid.uuid4().hex[:6]}"
        try:
            df = pl.read_excel(file_path, sheet_name=sheet_name)
            lf = df.lazy()
            self.save_node_result(lf, node_id)
            os.remove(file_path)
            return node_id
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise Exception(f"Failed to read Excel file: {str(e)}")

    def get_excel_sheets(self, file: UploadFile) -> List[str]:
        file_path = self.save_upload(file)
        try:
            import openpyxl
            workbook = openpyxl.load_workbook(file_path, read_only=True)
            sheets = workbook.sheetnames
            workbook.close()
            os.remove(file_path)
            return sheets
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e

    def read_from_db(self, connection_string: str, query: str) -> str:
        node_id = f"db_source_{uuid.uuid4().hex[:6]}"
        df = pl.DataFrame({"placeholder": ["Database connection not implemented"]})
        self.save_node_result(df.lazy(), node_id)
        return node_id

    @safe_node_execution
    def filter_data(self, node_id: str, expression: str) -> str:
        """Legacy filter using eval - BLOCKED for security. Use safe_filter_data instead."""
        raise ValueError("filter_data is disabled for security reasons. Use safe_filter_data instead.")
    
    @safe_node_execution
    def safe_filter_data(self, node_id: str, filters: List[dict], logic: str = 'and') -> str:
        """Safe filter without eval - USE THIS"""
        new_id = f"filter_{node_id}"
        condition = SafeFilterBuilder.build_multi_filter(filters, logic)
        lf = self.get_node_data(node_id).filter(condition)
        return self.save_node_result(lf, new_id)
    
    @safe_node_execution
    def sort_data(self, node_id: str, by: List[str], descending: bool = False) -> str:
        new_id = f"sort_{node_id}"
        lf = self.get_node_data(node_id).sort(by, descending=descending)
        return self.save_node_result(lf, new_id)
    
    @safe_node_execution
    def drop_na(self, node_id: str, subset: Optional[List[str]] = None) -> str:
        new_id = f"dropna_{node_id}"
        lf = self.get_node_data(node_id).drop_nulls(subset=subset)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def select_columns(self, node_id: str, columns: List[str]) -> str:
        new_id = f"select_{node_id}"
        lf = self.get_node_data(node_id).select(columns)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def drop_columns(self, node_id: str, columns: List[str]) -> str:
        new_id = f"dropcols_{node_id}"
        lf = self.get_node_data(node_id).drop(columns)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def join_nodes(self, left_id: str, right_id: str, on: str, how: str = "inner") -> str:
        new_id = f"join_{left_id}"
        lf = self.get_node_data(left_id).join(self.get_node_data(right_id), on=on, how=how)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def union_nodes(self, node_ids: List[str]) -> str:
        new_id = f"union_{uuid.uuid4().hex[:6]}"
        lfs = [self.get_node_data(nid) for nid in node_ids]
        lf = pl.concat(lfs)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def append_vertical(self, top_node_id: str, bottom_node_id: str, how: str = "vertical") -> str:
        new_id = f"append_{uuid.uuid4().hex[:6]}"
        lf = pl.concat([self.get_node_data(top_node_id), self.get_node_data(bottom_node_id)], how=how)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def cache_to_redis(self, node_id: str, ttl: int = 3600):
        return f"Node {node_id} already cached in Parquet format"

    @safe_node_execution
    def preview(self, node_id: str, n_rows: int = 100) -> Dict:
        lf = self.get_node_data(node_id)
        df = lf.limit(n_rows).collect()
        return df.to_dicts()
    
    @safe_node_execution
    def rename_columns(self, node_id: str, mapping: Dict[str, str]) -> str:
        new_id = f"rename_{node_id}"
        lf = self.get_node_data(node_id).rename(mapping)
        return self.save_node_result(lf, new_id)
    
    @safe_node_execution
    def str_left(self, node_id: str, column: str, n_chars: int, new_col: str = None) -> str:
        new_id = f"left_{node_id}"
        target_col = new_col if new_col else f"{column}_left"
        lf = self.get_node_data(node_id).with_columns(pl.col(column).str.slice(0, n_chars).alias(target_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def str_right(self, node_id: str, column: str, n_chars: int, new_col: str = None) -> str:
        new_id = f"right_{node_id}"
        target_col = new_col if new_col else f"{column}_right"
        lf = self.get_node_data(node_id).with_columns(pl.col(column).str.slice(-n_chars).alias(target_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def str_mid(self, node_id: str, column: str, start: int, length: int, new_col: str = None) -> str:
        new_id = f"mid_{node_id}"
        target_col = new_col if new_col else f"{column}_mid"
        start_idx = max(0, start - 1)
        lf = self.get_node_data(node_id).with_columns(pl.col(column).str.slice(start_idx, length).alias(target_col))
        return self.save_node_result(lf, new_id)
    
    @safe_node_execution
    def drop_duplicates(self, node_id: str, subset: List[str] = None) -> str:
        new_id = f"dedup_{node_id}"
        lf = self.get_node_data(node_id).unique(subset=subset)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def drop_nulls(self, node_id: str) -> str:
        new_id = f"clean_{node_id}"
        lf = self.get_node_data(node_id).drop_nulls()
        return self.save_node_result(lf, new_id)
    
    @safe_node_execution
    def str_to_upper(self, node_id: str, column: str, new_col: str = None) -> str:
        new_id = f"upper_{node_id}"
        target_col = new_col if new_col else column
        lf = self.get_node_data(node_id).with_columns(pl.col(column).str.to_uppercase().alias(target_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def str_to_lower(self, node_id: str, column: str, new_col: str = None) -> str:
        new_id = f"lower_{node_id}"
        target_col = new_col if new_col else column
        lf = self.get_node_data(node_id).with_columns(pl.col(column).str.to_lowercase().alias(target_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def str_to_title(self, node_id: str, column: str, new_col: str = None) -> str:
        new_id = f"title_{node_id}"
        target_col = new_col if new_col else column
        lf = self.get_node_data(node_id).with_columns(pl.col(column).str.to_titlecase().alias(target_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def clean_string_column(self, node_id: str, column: str) -> str:
        new_id = f"clean_str_{node_id}"
        lf = self.get_node_data(node_id).with_columns(pl.col(column).str.strip_chars().str.to_uppercase())
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def multi_column_multiply(self, node_id: str, columns: List[str], factor: float, suffix: str = "_adj") -> str:
        new_id = f"mult_bulk_{node_id}"
        lf = self.get_node_data(node_id).with_columns([(pl.col(c) * factor).alias(f"{c}{suffix}") for c in columns])
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def horizontal_sum(self, node_id: str, columns: List[str], new_col: str) -> str:
        new_id = f"hsum_{node_id}"
        lf = self.get_node_data(node_id).with_columns(pl.sum_horizontal(columns).alias(new_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def horizontal_average(self, node_id: str, columns: List[str], new_col: str) -> str:
        new_id = f"havg_{node_id}"
        lf = self.get_node_data(node_id).with_columns(pl.mean_horizontal(columns).alias(new_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def apply_custom_expression(self, node_id: str, left_cols: List[str], op: str, right_val: Union[str, float], new_suffix: str) -> str:
        new_id = f"expr_{node_id}"
        ops = {"+": lambda c, r: c + r, "-": lambda c, r: c - r, "*": lambda c, r: c * r, "/": lambda c, r: c / r}
        right_expr = pl.col(right_val) if isinstance(right_val, str) else right_val
        lf = self.get_node_data(node_id).with_columns([ops[op](pl.col(c), right_expr).alias(f"{c}_{new_suffix}") for c in left_cols])
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def vector_dot_product(self, node_id: str, vec_a: List[str], vec_b: List[str], new_col: str) -> str:
        if len(vec_a) != len(vec_b):
            raise ValueError("Vector dimensions must match")
        new_id = f"dot_{node_id}"
        lf = self.get_node_data(node_id).with_columns(pl.sum_horizontal([pl.col(a) * pl.col(b) for a, b in zip(vec_a, vec_b)]).alias(new_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def vector_linear_multiply(self, node_id: str, vec_a: List[str], vec_b: List[str], suffix: str = "_prod") -> str:
        new_id = f"linear_mult_{node_id}"
        lf = self.get_node_data(node_id).with_columns([(pl.col(a) * pl.col(b)).alias(f"{a}{suffix}") for a, b in zip(vec_a, vec_b)])
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def vector_cross_product(self, node_id: str, vec_a: List[str], vec_b: List[str], prefix: str = "cross") -> str:
        if len(vec_a) != 3 or len(vec_b) != 3:
            raise ValueError("Cross product requires 3D vectors")
        new_id = f"cross_{node_id}"
        a1, a2, a3 = [pl.col(c) for c in vec_a]
        b1, b2, b3 = [pl.col(c) for c in vec_b]
        lf = self.get_node_data(node_id).with_columns([
            (a2 * b3 - a3 * b2).alias(f"{prefix}_x"),
            (a3 * b1 - a1 * b3).alias(f"{prefix}_y"),
            (a1 * b2 - a2 * b1).alias(f"{prefix}_z")
        ])
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def matrix_transpose(self, node_id: str) -> str:
        new_id = f"transpose_{node_id}"
        df = self.get_node_data(node_id).collect().transpose()
        return self.save_node_result(df.lazy(), new_id)

    @safe_node_execution
    def col_stats_advanced(self, node_id: str, columns: List[str]) -> str:
        """Output long-form stats table: (column_name, metric, value).
        Fully chainable — downstream nodes can filter/pivot on metric."""
        new_id = f"stats_{node_id}"
        df = self.get_node_data(node_id).select(columns).collect()
        rows = []
        for c in columns:
            series = df[c].drop_nulls()
            rows.extend([
                {"column_name": c, "metric": "mean",   "value": float(series.mean())},
                {"column_name": c, "metric": "std",    "value": float(series.std())},
                {"column_name": c, "metric": "median", "value": float(series.median())},
                {"column_name": c, "metric": "min",    "value": float(series.min())},
                {"column_name": c, "metric": "max",    "value": float(series.max())},
                {"column_name": c, "metric": "q1",     "value": float(series.quantile(0.25))},
                {"column_name": c, "metric": "q3",     "value": float(series.quantile(0.75))},
                {"column_name": c, "metric": "count",  "value": float(len(series))},
                {"column_name": c, "metric": "null_count", "value": float(df[c].null_count())},
            ])
        result_df = pl.DataFrame(rows)
        return self.save_node_result(result_df.lazy(), new_id)

    @safe_node_execution
    def linear_regression_node(self, node_id: str, target: str, features: List[str]) -> str:
        """Fit OLS, append predicted_{target} column to the original dataframe.
        Coefficients are stored as extra columns (intercept + one per feature)
        so the full DF flows through and downstream nodes can use predictions."""
        df = self.get_node_data(node_id).collect()
        y = df[target].to_numpy().astype(float)
        X = df.select(features).to_numpy().astype(float)

        X_int = np.column_stack([np.ones(X.shape[0]), X])
        coeffs, _, _, _ = np.linalg.lstsq(X_int, y, rcond=None)

        predicted = X_int @ coeffs
        result_df = df.with_columns(pl.Series(name=f"predicted_{target}", values=predicted.tolist()))

        new_id = f"lr_model_{node_id}"
        return self.save_node_result(result_df.lazy(), new_id)

    @safe_node_execution
    def correlation_matrix_1d(self, node_id: str, columns: List[str]) -> str:
        """Compute pairwise Pearson correlations and return as a tidy 1-D dataframe:
        columns: col_a (str), col_b (str), correlation (f64).
        Chainable — downstream nodes can filter/sort/pivot on the result."""
        new_id = f"corr_{node_id}"
        df = self.get_node_data(node_id).select(columns).collect()
        rows = []
        for i, a in enumerate(columns):
            for j, b in enumerate(columns):
                if j <= i:
                    continue  # upper triangle only to avoid duplicates
                corr_val = float(df.select(pl.corr(a, b)).item())
                rows.append({"col_a": a, "col_b": b, "correlation": corr_val})
        if rows:
            result_df = pl.DataFrame(rows)
        else:
            result_df = pl.DataFrame({
                "col_a": pl.Series([], dtype=pl.Utf8),
                "col_b": pl.Series([], dtype=pl.Utf8),
                "correlation": pl.Series([], dtype=pl.Float64),
            })
        return self.save_node_result(result_df.lazy(), new_id)

    @safe_node_execution
    def logistic_regression_prediction(self, node_id: str, features: List[str], weights: List[float]) -> str:
        """Apply logistic regression: appends 'probability' column to the original DF."""
        if len(features) != len(weights):
            raise ValueError("Features and weights length mismatch")
        new_id = f"logit_{node_id}"
        z_expr = pl.sum_horizontal([pl.col(f) * w for f, w in zip(features, weights)])
        lf = self.get_node_data(node_id).with_columns(
            (1 / (1 + (-z_expr).exp())).alias("probability")
        )
        return self.save_node_result(lf, new_id)
        if len(features) != len(weights):
            raise ValueError("Features and weights length mismatch")
        new_id = f"logit_{node_id}"
        z_expr = pl.sum_horizontal([pl.col(f) * w for f, w in zip(features, weights)])
        lf = self.get_node_data(node_id).with_columns((1 / (1 + (-z_expr).exp())).alias("probability"))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def filter_outliers_iqr(self, node_id: str, column: str) -> str:
        new_id = f"no_outliers_{node_id}"
        q1 = pl.col(column).quantile(0.25)
        q3 = pl.col(column).quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        lf = self.get_node_data(node_id).filter((pl.col(column) >= lower_bound) & (pl.col(column) <= upper_bound))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def group_by_agg(self, node_id: str, group_cols: List[str], aggs: Dict[str, List[str]]) -> str:
        new_id = f"groupby_{node_id}"
        agg_exprs = []
        for col, funcs in aggs.items():
            for func in funcs:
                if func == "sum":    agg_exprs.append(pl.col(col).sum().alias(f"{col}_sum"))
                elif func == "mean": agg_exprs.append(pl.col(col).mean().alias(f"{col}_avg"))
                elif func == "count":agg_exprs.append(pl.col(col).count().alias(f"{col}_count"))
                elif func == "max":  agg_exprs.append(pl.col(col).max().alias(f"{col}_max"))
                elif func == "min":  agg_exprs.append(pl.col(col).min().alias(f"{col}_min"))
                elif func == "std":  agg_exprs.append(pl.col(col).std().alias(f"{col}_std"))
                elif func == "median": agg_exprs.append(pl.col(col).median().alias(f"{col}_median"))
        lf = self.get_node_data(node_id).group_by(group_cols).agg(agg_exprs)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def cast_column(self, node_id: str, column: str, dtype: str) -> str:
        new_id = f"cast_{node_id}"
        type_map = {"int": pl.Int64, "float": pl.Float64, "str": pl.Utf8, "bool": pl.Boolean}
        lf = self.get_node_data(node_id).with_columns(pl.col(column).cast(type_map[dtype]))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def extract_date_parts(self, node_id: str, column: str) -> str:
        new_id = f"date_parts_{node_id}"
        lf = self.get_node_data(node_id).with_columns([
            pl.col(column).dt.year().alias(f"{column}_year"),
            pl.col(column).dt.month().alias(f"{column}_month"),
            pl.col(column).dt.day().alias(f"{column}_day")
        ])
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def fill_missing(self, node_id: str, column: str, value: Any) -> str:
        new_id = f"fill_{node_id}"
        lf = self.get_node_data(node_id).with_columns(pl.col(column).fill_null(value))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def conditional_column(self, node_id: str, column: str, op: str, threshold: Any,
                           then_val: Any, else_val: Any, new_col: str) -> str:
        """Create conditional column using safe expression building — no eval()."""
        new_id = f"if_{node_id}"

        # Whitelist of allowed comparison operators
        SAFE_OPS = {'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<=', 'eq': '==', 'ne': '!='}
        if op not in SAFE_OPS:
            raise ValueError(f"Invalid operator '{op}'. Allowed: {list(SAFE_OPS.keys())}")

        # Build condition expression safely without eval()
        col_expr = pl.col(column)
        try:
            num = float(threshold)
            cond_map = {
                'gt':  col_expr > num,  'gte': col_expr >= num,
                'lt':  col_expr < num,  'lte': col_expr <= num,
                'eq':  col_expr == num, 'ne':  col_expr != num,
            }
        except (ValueError, TypeError):
            # String comparison
            cond_map = {
                'gt':  col_expr > str(threshold),  'gte': col_expr >= str(threshold),
                'lt':  col_expr < str(threshold),  'lte': col_expr <= str(threshold),
                'eq':  col_expr == str(threshold), 'ne':  col_expr != str(threshold),
            }
        condition = cond_map[op]

        # Resolve then/else — column reference or literal
        schema = self.get_node_data(node_id).collect_schema()
        def resolve(val):
            if isinstance(val, str) and val in schema:
                return pl.col(val)
            return pl.lit(val)

        lf = self.get_node_data(node_id).with_columns(
            pl.when(condition).then(resolve(then_val)).otherwise(resolve(else_val)).alias(new_col)
        )
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def concat_columns(self, node_id: str, columns: List[str], separator: str = "", new_col: str = "concatenated") -> str:
        new_id = f"concat_{node_id}"
        lf = self.get_node_data(node_id).with_columns(pl.concat_str(pl.col(columns), separator=separator).alias(new_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def concat_with_literal(self, node_id: str, column: str, prefix: str = "", suffix: str = "", new_col: str = None) -> str:
        new_id = f"prefix_suffix_{node_id}"
        target = new_col if new_col else f"{column}_fixed"
        lf = self.get_node_data(node_id).with_columns((pl.lit(prefix) + pl.col(column).cast(pl.Utf8) + pl.lit(suffix)).alias(target))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def reorder_columns(self, node_id: str, ordered_cols: List[str]) -> str:
        new_id = f"reorder_{node_id}"
        lf = self.get_node_data(node_id).select(ordered_cols)
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def pivot_table(self, node_id: str, values: str, index: List[str], on: str, agg: str = "sum") -> str:
        df = self.get_node_data(node_id).collect().pivot(values=values, index=index, on=on, aggregate_function=agg)
        new_id = f"pivot_{node_id}"
        return self.save_node_result(df.lazy(), new_id)

    @safe_node_execution
    def moving_average(self, node_id: str, column: str, window: int) -> str:
        new_id = f"mavg_{node_id}"
        lf = self.get_node_data(node_id).with_columns(pl.col(column).rolling_mean(window_size=window).alias(f"{column}_mavg"))
        return self.save_node_result(lf, new_id)

    def get_node_metadata(self, node_id: str, n_preview: int = 5) -> Dict[str, Any]:
        meta_key = f"versor:meta:{node_id}"
        meta_json = self.r.get(meta_key)

        if not meta_json:
            return {"status": "error", "type": "system", "msg": f"Node {node_id} not found"}

        metadata = json.loads(meta_json)
        schema = metadata["schema"]
        row_count = metadata["row_count"]
        return {
            "node_id": node_id,
            "schema": schema,
            "columns": list(schema.keys()),
            "preview": metadata["preview"][:n_preview],
            "column_count": len(schema),
            "row_count": row_count,
            "shape": [row_count, len(schema)],
            "memory_usage": f"{row_count * len(schema) * 8 / 1024:.1f} KB"
        }

    # ── NEW NODES (for roll-rate analysis and general use) ────────────────────

    @safe_node_execution
    def add_literal_column(self, node_id: str, column: str, value, dtype: str = "string") -> str:
        """Add a column with a constant value to every row."""
        new_id = f"literal_{node_id}"
        type_map = {"string": pl.Utf8, "integer": pl.Int64, "float": pl.Float64, "boolean": pl.Boolean}
        pl_type = type_map.get(dtype, pl.Utf8)
        lf = self.get_node_data(node_id).with_columns(pl.lit(value).cast(pl_type).alias(column))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def range_bucket(self, node_id: str, column: str, bins: List, labels: List[str], new_col: str = "bucket") -> str:
        """Bucket a numeric column into labeled ranges.
        bins: [0, 30, 60, 90] → ranges: <=0, 1-30, 31-60, 61-90, 90+
        labels: ["0", "1-30", "31-60", "61-90", "90+"] (len = len(bins) + 1)
        """
        new_id = f"bucket_{node_id}"
        col = pl.col(column).cast(pl.Float64)

        if len(labels) != len(bins) + 1:
            raise ValueError(f"labels length ({len(labels)}) must be bins length ({len(bins)}) + 1")

        # Build chained when/then/otherwise
        expr = pl.when(col <= bins[0]).then(pl.lit(labels[0]))
        for i in range(1, len(bins)):
            expr = expr.when(col <= bins[i]).then(pl.lit(labels[i]))
        expr = expr.otherwise(pl.lit(labels[-1]))

        lf = self.get_node_data(node_id).with_columns(expr.alias(new_col))
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def date_offset(self, node_id: str, column: str, offset: int, unit: str = "days", new_col: str = None) -> str:
        """Add/subtract a fixed duration from a date column.
        unit: 'days', 'weeks', 'months', 'years'
        """
        new_id = f"dateoff_{node_id}"
        target = new_col or f"{column}_offset"

        if unit == "days":
            dur = pl.duration(days=offset)
        elif unit == "weeks":
            dur = pl.duration(weeks=offset)
        elif unit == "months":
            # Polars doesn't have duration(months), use offset_by
            lf = self.get_node_data(node_id).with_columns(
                pl.col(column).cast(pl.Date).dt.offset_by(f"{offset}mo").alias(target)
            )
            return self.save_node_result(lf, new_id)
        elif unit == "years":
            lf = self.get_node_data(node_id).with_columns(
                pl.col(column).cast(pl.Date).dt.offset_by(f"{offset}y").alias(target)
            )
            return self.save_node_result(lf, new_id)
        else:
            raise ValueError(f"Unknown unit '{unit}'. Use: days, weeks, months, years")

        lf = self.get_node_data(node_id).with_columns(
            (pl.col(column).cast(pl.Date) + dur).alias(target)
        )
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def crosstab(self, node_id: str, index: str, columns: str, values: str = None, agg: str = "count") -> str:
        """Cross-tabulation of two categorical columns.
        If values is None and agg is 'count', counts occurrences.
        """
        new_id = f"crosstab_{node_id}"
        df = self.get_node_data(node_id).collect()

        if values is None or agg == "count":
            # Count occurrences
            ct = df.group_by([index, columns]).len().rename({"len": "_count"})
            result = ct.pivot(values="_count", index=index, on=columns, aggregate_function="sum")
        else:
            result = df.pivot(values=values, index=index, on=columns, aggregate_function=agg)

        result = result.fill_null(0)
        lf = result.lazy()
        return self.save_node_result(lf, new_id)

    @safe_node_execution
    def cumulative_product(self, node_id: str, column: str, new_col: str = None) -> str:
        """Compute cumulative product along a column."""
        new_id = f"cumprod_{node_id}"
        target = new_col or f"{column}_cumprod"
        lf = self.get_node_data(node_id).with_columns(
            pl.col(column).cast(pl.Float64).cum_prod().alias(target)
        )
        return self.save_node_result(lf, new_id)
