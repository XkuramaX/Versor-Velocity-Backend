# Versor Backend API - Complete Documentation

## Base URL
```
http://localhost:8000
```

## Response Format
All endpoints return JSON responses:
- **Success**: `{"node_id": "string", "metadata": {...}}`
- **Error**: `{"detail": "error message"}` or `{"status": "error", "type": "error_type", "msg": "message"}`

---

## 1. Data Ingestion

### Upload CSV File
```http
POST /nodes/io/upload_csv
Content-Type: multipart/form-data

Body:
- file: CSV file (required)

Response:
{
  "node_id": "source_abc123",
  "metadata": {
    "schema": {"col1": "Int64", "col2": "Utf8"},
    "preview": [...],
    "column_count": 2
  }
}
```

### Upload Excel File
```http
POST /nodes/io/upload_excel?sheet_name=Sheet1
Content-Type: multipart/form-data

Body:
- file: Excel file (required)

Query Parameters:
- sheet_name: Sheet name to read (default: "Sheet1")

Response: Same as CSV upload
```

### Get Excel Sheets
```http
POST /nodes/io/excel_sheets
Content-Type: multipart/form-data

Body:
- file: Excel file (required)

Response:
{
  "sheets": ["Sheet1", "Data_Analysis", "Summary"]
}
```

### Read from Database
```http
POST /nodes/io/read_from_db
Content-Type: application/json

{
  "connection_string": "postgresql://user:pass@host:port/db",
  "query": "SELECT * FROM table"
}

Response:
{
  "node_id": "db_source_abc123",
  "metadata": {...}
}
```

---

## 2. Data Transformation

### Filter Data (DEPRECATED - Use Safe Filter)
```http
POST /nodes/transform/filter?parent_id=source_abc123
Content-Type: application/json

{
  "column": "salary",
  "operator": ">",
  "value": 50000
}

⚠️ DEPRECATED: This endpoint uses eval() internally. Use /nodes/transform/safe_filter instead.
```

### Safe Filter Data (RECOMMENDED)
```http
POST /nodes/transform/safe_filter?parent_id=source_abc123
Content-Type: application/json

{
  "filters": [
    {
      "column": "salary",
      "operation": "gt",
      "value": 50000,
      "column_type": "numeric"
    }
  ],
  "logic": "and"
}

Supported Operations:
- Numeric: "gt" (>), "gte" (>=), "lt" (<), "lte" (<=), "eq" (=), "ne" (!=), "between", "in"
- String: "eq", "ne", "contains", "starts_with", "ends_with", "in"

Multiple Filters:
{
  "filters": [
    {"column": "salary", "operation": "gt", "value": 50000, "column_type": "numeric"},
    {"column": "department", "operation": "eq", "value": "IT", "column_type": "string"}
  ],
  "logic": "and"  // "and" or "or"
}

Between Operation:
{
  "filters": [
    {"column": "age", "operation": "between", "value": [25, 65], "column_type": "numeric"}
  ],
  "logic": "and"
}

In List Operation:
{
  "filters": [
    {"column": "status", "operation": "in", "value": ["Active", "Pending"], "column_type": "string"}
  ],
  "logic": "and"
}

Response:
{
  "node_id": "safe_filter_abc123",
  "metadata": {...}
}
```

### Get Column Information
```http
GET /nodes/{node_id}/column_info

Response:
{
  "columns": {
    "salary": {
      "type": "numeric",
      "dtype": "Float64"
    },
    "name": {
      "type": "string",
      "dtype": "String",
      "unique_values": ["John", "Jane", "Bob", ...]
    }
  }
}
```

### Get Column Values
```http
GET /nodes/{node_id}/column_values?column=department

Response:
{
  "values": ["IT", "HR", "Finance", "Marketing"]
}
```

### Get Node Columns
```http
GET /nodes/{node_id}/columns

Response:
{
  "columns": ["name", "salary", "department", "age"]
}
```

### Select Columns
```http
POST /nodes/transform/select?parent_id=source_abc123
Content-Type: application/json

["name", "salary", "department"]

Note: Send array directly, not {"columns": [...]}
```

### Drop Columns
```http
POST /nodes/transform/drop?parent_id=source_abc123
Content-Type: application/json

["unwanted_col1", "unwanted_col2"]

Note: Send array directly, not {"columns": [...]}
```

### Sort Data
```http
POST /nodes/transform/sort?parent_id=source_abc123
Content-Type: application/json

{
  "by": ["salary", "name"],
  "descending": false
}
```

### Rename Columns
```http
POST /nodes/transform/rename?parent_id=source_abc123
Content-Type: application/json

{
  "mapping": {"old_name": "new_name", "col1": "column_1"}
}
```

### Reorder Columns
```http
POST /nodes/transform/reorder?parent_id=source_abc123
Content-Type: application/json

{
  "ordered_cols": ["name", "salary", "department"]
}
```

---

## 3. String Operations

### Change Case
```http
POST /nodes/string/case?parent_id=source_abc123
Content-Type: application/json

{
  "column": "name",
  "mode": "upper",  // "upper", "lower", "title"
  "new_col": "name_upper"  // optional
}
```

### String Slice (Left/Right)
```http
POST /nodes/string/slice?parent_id=source_abc123
Content-Type: application/json

{
  "column": "phone",
  "n_chars": 3,
  "mode": "left",  // "left" or "right"
  "new_col": "area_code"  // optional
}
```

### String Mid (Excel MID function)
```http
POST /nodes/string/mid?parent_id=source_abc123
Content-Type: application/json

{
  "column": "text",
  "start": 2,  // 1-indexed position
  "length": 5,
  "new_col": "extracted"  // optional
}
```

### String Concatenation
```http
POST /nodes/string/concat?parent_id=source_abc123
Content-Type: application/json

{
  "columns": ["first_name", "last_name"],
  "separator": " ",
  "new_col": "full_name"
}
```

### Add Prefix/Suffix
```http
POST /nodes/string/prefix_suffix?parent_id=source_abc123
Content-Type: application/json

{
  "column": "id",
  "prefix": "USER_",
  "suffix": "_ACTIVE",
  "new_col": "formatted_id"  // optional
}
```

### Clean String Column
```http
POST /nodes/string/clean?parent_id=source_abc123
Content-Type: application/json

{
  "column": "messy_text"
}
```

---

## 4. Math Operations

### Horizontal Math Operations
```http
POST /nodes/math/horizontal?parent_id=source_abc123
Content-Type: application/json

{
  "columns": ["val_1", "val_2", "val_3"],
  "new_col": "total",
  "op": "sum"  // "sum" or "average"
}
```

### Custom Mathematical Expression
```http
POST /nodes/math/custom?parent_id=source_abc123
Content-Type: application/json

{
  "left_cols": ["salary", "bonus"],
  "op": "*",  // "+", "-", "*", "/"
  "right_val": 1.1,  // number or column name
  "new_suffix": "adjusted"
}
```

### Bulk Column Multiplication
```http
POST /nodes/math/multiply_bulk?parent_id=source_abc123
Content-Type: application/json

{
  "columns": ["price", "cost", "tax"],
  "factor": 1.08,
  "suffix": "_inflated"
}
```

---

## 5. Vector Operations

### Vector Dot Product
```http
POST /nodes/vector/dot_product?parent_id=source_abc123
Content-Type: application/json

{
  "vec_a": ["x1", "y1", "z1"],
  "vec_b": ["x2", "y2", "z2"],
  "new_col": "dot_product"
}
```

### Vector Linear Multiplication
```http
POST /nodes/vector/linear_multiply?parent_id=source_abc123
Content-Type: application/json

{
  "vec_a": ["a1", "a2", "a3"],
  "vec_b": ["b1", "b2", "b3"],
  "suffix": "_product"
}
```

### Vector Cross Product (3D only)
```http
POST /nodes/vector/cross_product?parent_id=source_abc123
Content-Type: application/json

{
  "vec_a": ["x1", "y1", "z1"],
  "vec_b": ["x2", "y2", "z2"],
  "prefix": "cross"
}
```

---

## 6. Data Cleaning

### Drop Null Values
```http
POST /nodes/clean/drop_na?parent_id=source_abc123
Content-Type: application/json

{
  "subset": ["salary", "name"]  // optional, specific columns
}
```

### Drop All Nulls
```http
POST /nodes/clean/drop_nulls?parent_id=source_abc123
Content-Type: application/json

{}
```

### Drop Duplicates
```http
POST /nodes/clean/drop_duplicates?parent_id=source_abc123
Content-Type: application/json

{
  "subset": ["email"]  // optional, specific columns
}
```

### Fill Missing Values
```http
POST /nodes/clean/fill_missing?parent_id=source_abc123
Content-Type: application/json

{
  "column": "salary",
  "value": 0
}
```

---

## 7. Data Type Operations

### Cast Column Type
```http
POST /nodes/dtype/cast?parent_id=source_abc123
Content-Type: application/json

{
  "column": "age",
  "dtype": "int"  // "int", "float", "str", "bool"
}
```

---

## 8. Date/Time Operations

### Extract Date Parts
```http
POST /nodes/datetime/extract_parts?parent_id=source_abc123
Content-Type: application/json

{
  "column": "created_date"
}

Response: Creates columns: {column}_year, {column}_month, {column}_day
```

---

## 9. Joins & Set Operations

### Join Datasets
```http
POST /nodes/combine/join?left_id=source_abc123&right_id=source_def456
Content-Type: application/json

{
  "on": "user_id",
  "how": "inner"  // "inner", "left", "outer", "semi", "anti"
}
```

### Union Datasets
```http
POST /nodes/combine/union
Content-Type: application/json

{
  "node_ids": ["source_abc123", "source_def456", "source_ghi789"]
}
```

---

## 10. Advanced Analytics

### Remove Outliers (IQR Method)
```http
POST /nodes/advanced/outliers?parent_id=source_abc123
Content-Type: application/json

{
  "column": "salary"
}
```

### Group By Aggregation
```http
POST /nodes/advanced/groupby?parent_id=source_abc123
Content-Type: application/json

{
  "group_cols": ["department"],
  "aggs": {
    "salary": ["sum", "mean", "count"],
    "score": ["max", "mean"]
  }
}
```

### Advanced Column Statistics
```http
POST /nodes/advanced/stats?parent_id=source_abc123
Content-Type: application/json

{
  "columns": ["salary", "score", "age"]
}

Response: Creates mean, std, median, q1, q3 for each column
```

### Pivot Table
```http
POST /nodes/advanced/pivot?parent_id=source_abc123
Content-Type: application/json

{
  "values": "salary",
  "index": ["department"],
  "on": "status",
  "agg": "sum"  // "sum", "mean", "count", "max", "min"
}
```

### Moving Average
```http
POST /nodes/advanced/moving_average?parent_id=source_abc123
Content-Type: application/json

{
  "column": "price",
  "window": 7
}
```

### Conditional Column (IF-THEN-ELSE)
```http
POST /nodes/advanced/conditional?parent_id=source_abc123
Content-Type: application/json

{
  "condition_expr": "pl.col('salary') > 50000",
  "then_val": "High",
  "else_val": "Low",
  "new_col": "salary_category"
}
```

---

## 11. Matrix Operations

### Matrix Transpose
```http
POST /nodes/matrix/transpose?parent_id=source_abc123
Content-Type: application/json

{}
```

---

## 12. Machine Learning

### Linear Regression
```http
POST /nodes/ml/linear_regression?parent_id=source_abc123
Content-Type: application/json

{
  "target": "salary",
  "features": ["experience", "age", "score"]
}

Response:
{
  "node_id": "lr_model_abc123",
  "metadata": {
    "schema": {"feature": "Utf8", "coefficient": "Float64", "abs_coefficient": "Float64"},
    "preview": [
      {"feature": "intercept", "coefficient": 45000, "abs_coefficient": 45000},
      {"feature": "experience", "coefficient": 1200, "abs_coefficient": 1200}
    ]
  }
}
```

### Logistic Regression Prediction
```http
POST /nodes/ml/logistic_prediction?parent_id=source_abc123
Content-Type: application/json

{
  "features": ["experience", "score"],
  "weights": [0.5, -0.3]
}

Response: Adds "probability" column with logistic regression predictions
```

---

## 13. Node Management

### Inspect Node Data
```http
GET /nodes/{node_id}?limit=50

Response:
{
  "node_id": "source_abc123",
  "schema": {"name": "Utf8", "salary": "Int64"},
  "preview": [...],
  "column_count": 10
}
```

### Download Node Data
```http
GET /nodes/{node_id}/download?format=csv

Query Parameters:
- format: "csv", "parquet", or "excel"

Response: File download (binary data)
```

### Cache Node to Redis
```http
POST /nodes/{node_id}/cache?ttl=3600

Query Parameters:
- ttl: Cache time-to-live in seconds (default: 3600)

Response:
{
  "message": "Node {node_id} cached successfully."
}
```

---

## 14. Workflow Management

### Validate Connection
```http
POST /workflow/validate-connection?parent_id=source_abc123
Content-Type: application/json

{
  "child_type": "filter_data",
  "child_params": {"column": "salary"}
}

Response:
{
  "valid": true
}
```

### Export Workflow
```http
GET /workflow/export

Response: JSON file download with complete workflow structure
```

---

## 15. Health Check

### API Health Status
```http
GET /health

Response:
{
  "status": "healthy",
  "nodes_in_registry": 5,
  "workflow_nodes": 3
}
```

---

## Error Handling

### HTTP Status Codes
- **200**: Success
- **400**: Bad Request (invalid parameters, missing columns)
- **404**: Not Found (node doesn't exist)
- **413**: Payload Too Large (file > 1GB or JSON > 10MB)
- **422**: Validation Error (missing required fields)
- **429**: Too Many Requests (rate limit exceeded)
- **500**: Internal Server Error

### Error Response Examples

#### Rate Limit Error
```json
{
  "detail": "Rate limit exceeded. Maximum 100 requests per minute."
}
```

#### File Too Large Error
```json
{
  "detail": "File too large. Maximum size is 1024.0MB"
}
```

#### Missing Column Error
```json
{
  "status": "error",
  "type": "schema",
  "msg": "Column 'salary' not found in schema: ['name', 'age', 'department']"
}
```

#### Node Not Found Error
```json
{
  "detail": "Node source_abc123 not found. Please execute the node first."
}
```

#### Validation Error
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "expression"],
      "msg": "Field required"
    }
  ]
}
```

---

## Data Types

### Supported Polars Data Types
- **Int64**: 64-bit integers
- **Float64**: 64-bit floating point
- **Utf8**: UTF-8 encoded strings
- **Boolean**: True/False values
- **Date**: Date values
- **Datetime**: Date and time values

### File Upload Limits
- **Max file size**: 1GB per upload
- **Max JSON body**: 10MB
- **Supported formats**: CSV, Excel (.xlsx, .xls)
- **Cache duration**: 2 hours for uploaded files

### Rate Limiting
- **Default limit**: 100 requests per minute per IP
- **Window**: 60 seconds (sliding window)
- **Response on limit**: HTTP 429 with error message

### Security Middleware
- **Input Validation**: File size and JSON body size limits
- **Rate Limiting**: Per-IP request throttling
- **CORS**: Enabled for cross-origin requests (configure for production)

### Performance Notes
- All operations use Polars lazy evaluation for memory efficiency
- Large datasets (>1M rows) are automatically streamed
- Redis caching available for frequently accessed nodes
- File uploads are automatically cleaned after processing

---

## Complete Workflow Example

### Using Safe Filter (Recommended)
```javascript
// 1. Upload CSV data
const uploadResponse = await fetch('/nodes/io/upload_csv', {
  method: 'POST',
  body: formData
});
const {node_id: sourceId} = await uploadResponse.json();

// 2. Get column information for building filters
const colInfoResponse = await fetch(`/nodes/${sourceId}/column_info`);
const {columns} = await colInfoResponse.json();

// 3. Apply safe filter (no eval)
const filterResponse = await fetch(`/nodes/transform/safe_filter?parent_id=${sourceId}`, {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({
    filters: [
      {column: 'salary', operation: 'gt', value: 50000, column_type: 'numeric'},
      {column: 'department', operation: 'eq', value: 'IT', column_type: 'string'}
    ],
    logic: 'and'
  })
});
const {node_id: filterId} = await filterResponse.json();

// 4. Select specific columns
const selectResponse = await fetch(`/nodes/transform/select?parent_id=${filterId}`, {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify(['name', 'salary', 'department'])
});
const {node_id: selectId} = await selectResponse.json();

// 5. Group by category and sum
const groupResponse = await fetch(`/nodes/advanced/groupby?parent_id=${selectId}`, {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({
    group_cols: ["department"],
    aggs: {"salary": ["sum", "count", "mean"]}
  })
});
const {node_id: groupId} = await groupResponse.json();

// 6. Download results as Excel
window.open(`/nodes/${groupId}/download?format=excel`);
```