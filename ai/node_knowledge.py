"""
Node Knowledge Base for Versor-Velocity
────────────────────────────────────────
Each node is a document with:
  - id: exact nodeType string used in React Flow
  - category: grouping for display
  - name: human-readable name
  - description: what it does
  - config_schema: exact JSON config shape
  - use_cases: natural-language descriptions of when to use this node
  - examples: concrete config examples
"""

NODE_DOCUMENTS = [
    # ── DATA INGESTION ──────────────────────────────────────────────────────
    {
        "id": "upload_csv",
        "category": "Data Ingestion",
        "name": "Upload CSV",
        "description": "Reads a CSV file uploaded by the user. Always the first node for CSV input. One per file.",
        "config_schema": {},
        "use_cases": [
            "Load a CSV file into the workflow",
            "Import data from a comma-separated file",
            "Start a workflow with CSV input data",
        ],
        "examples": ["User uploads employees.csv → creates a data source node"],
    },
    {
        "id": "upload_excel",
        "category": "Data Ingestion",
        "name": "Upload Excel",
        "description": "Reads an Excel file (.xlsx/.xls). Can specify which sheet to read.",
        "config_schema": {"sheet_name": "Sheet1"},
        "use_cases": ["Load an Excel spreadsheet", "Import data from a specific Excel sheet"],
        "examples": ["Upload report.xlsx with sheet_name='Q4 Data'"],
    },
    {
        "id": "read_from_db",
        "category": "Data Ingestion",
        "name": "Read from Database",
        "description": "Executes a SELECT query against a SQL database and loads the result.",
        "config_schema": {"connection_string": "postgresql://user:pass@host:port/db", "query": "SELECT * FROM table"},
        "use_cases": ["Query a PostgreSQL/MySQL database", "Load data from SQL"],
        "examples": [],
    },

    # ── DATA TRANSFORMATION ─────────────────────────────────────────────────
    {
        "id": "safe_filter",
        "category": "Data Transformation",
        "name": "Safe Filter",
        "description": "Filters rows based on conditions. Numeric ops: gt, gte, lt, lte, eq, ne, between, in. String ops: eq, ne, contains, starts_with, ends_with, in. Multiple filters with AND/OR.",
        "config_schema": {"filters": [{"column": "salary", "operation": "gt", "value": 50000, "column_type": "numeric"}], "logic": "and"},
        "use_cases": [
            "Filter rows where a column is greater/less than a value",
            "Keep only rows matching a condition",
            "Filter by multiple conditions with AND/OR",
            "Select rows where salary > 50000",
            "Filter employees in a specific department",
            "Keep rows where age is between 25 and 65",
            "Filter strings that contain a keyword",
            "Where clause equivalent",
        ],
        "examples": [
            "Filter salary > 50000: {filters: [{column:'salary', operation:'gt', value:50000, column_type:'numeric'}], logic:'and'}",
            "Filter dept == 'IT' AND age >= 30: {filters: [{column:'department', operation:'eq', value:'IT', column_type:'string'}, {column:'age', operation:'gte', value:30, column_type:'numeric'}], logic:'and'}",
        ],
    },
    {
        "id": "select",
        "category": "Data Transformation",
        "name": "Select Columns",
        "description": "Keeps only the specified columns and removes all others.",
        "config_schema": {"columns": ["col1", "col2"]},
        "use_cases": ["Keep only specific columns", "Select a subset of columns", "Choose which columns to include"],
        "examples": ["Select name and salary: {columns: ['name', 'salary']}"],
    },
    {
        "id": "drop",
        "category": "Data Transformation",
        "name": "Drop Columns",
        "description": "Removes the specified columns from the dataset.",
        "config_schema": {"columns": ["unwanted_col1"]},
        "use_cases": ["Remove unwanted columns", "Delete specific columns", "Exclude certain fields"],
        "examples": ["Drop internal_id: {columns: ['internal_id']}"],
    },
    {
        "id": "sort",
        "category": "Data Transformation",
        "name": "Sort Data",
        "description": "Sorts rows by one or more columns in ascending or descending order.",
        "config_schema": {"by": ["col1"], "descending": False},
        "use_cases": ["Sort data by a column", "Order rows ascending or descending", "Rank by a value"],
        "examples": ["Sort by salary desc: {by: ['salary'], descending: true}"],
    },
    {
        "id": "rename",
        "category": "Data Transformation",
        "name": "Rename Columns",
        "description": "Renames columns using a mapping dictionary.",
        "config_schema": {"mapping": {"old_name": "new_name"}},
        "use_cases": ["Rename a column", "Change column names", "Alias a column"],
        "examples": ["Rename emp_name to employee_name: {mapping: {'emp_name': 'employee_name'}}"],
    },
    {
        "id": "reorder",
        "category": "Data Transformation",
        "name": "Reorder Columns",
        "description": "Rearranges columns into a specific order.",
        "config_schema": {"ordered_cols": ["col1", "col2", "col3"]},
        "use_cases": ["Change column order", "Rearrange columns"],
        "examples": [],
    },

    # ── STRING OPERATIONS ───────────────────────────────────────────────────
    {
        "id": "string_case",
        "category": "String Operations",
        "name": "Change Case",
        "description": "Changes string case to upper, lower, or title case. Optionally creates a new column.",
        "config_schema": {"column": "name", "mode": "upper", "new_col": None},
        "use_cases": ["Convert to uppercase", "Make lowercase", "Title case a name column", "Normalize casing"],
        "examples": ["Uppercase name: {column: 'name', mode: 'upper'}"],
    },
    {
        "id": "string_slice",
        "category": "String Operations",
        "name": "String Slice (Left/Right)",
        "description": "Extracts the first N (left) or last N (right) characters from a string column.",
        "config_schema": {"column": "phone", "n_chars": 3, "mode": "left", "new_col": None},
        "use_cases": ["Get first N characters", "Extract last N characters", "Get area code from phone"],
        "examples": ["First 3 chars: {column: 'phone', n_chars: 3, mode: 'left', new_col: 'area_code'}"],
    },
    {
        "id": "string_mid",
        "category": "String Operations",
        "name": "String Mid",
        "description": "Extracts a substring from the middle (like Excel MID). Start is 1-indexed.",
        "config_schema": {"column": "text", "start": 2, "length": 5, "new_col": None},
        "use_cases": ["Extract middle characters", "Get substring from position", "Excel MID equivalent"],
        "examples": [],
    },
    {
        "id": "string_concat",
        "category": "String Operations",
        "name": "Concatenate Strings",
        "description": "Joins multiple string columns into one new column with a separator.",
        "config_schema": {"columns": ["first_name", "last_name"], "separator": " ", "new_col": "full_name"},
        "use_cases": ["Combine first and last name", "Join columns with separator", "Merge text columns"],
        "examples": ["Full name: {columns: ['first_name', 'last_name'], separator: ' ', new_col: 'full_name'}"],
    },
    {
        "id": "string_prefix_suffix",
        "category": "String Operations",
        "name": "Add Prefix/Suffix",
        "description": "Prepends and/or appends literal text to every value in a string column.",
        "config_schema": {"column": "id", "prefix": "USER_", "suffix": "_ACTIVE", "new_col": None},
        "use_cases": ["Add a prefix to values", "Append text to a column"],
        "examples": [],
    },
    {
        "id": "string_clean",
        "category": "String Operations",
        "name": "Clean String",
        "description": "Trims whitespace and normalizes a string column.",
        "config_schema": {"column": "messy_text"},
        "use_cases": ["Clean up messy text", "Trim whitespace", "Remove extra spaces"],
        "examples": [],
    },

    # ── MATH OPERATIONS ─────────────────────────────────────────────────────
    {
        "id": "math_horizontal",
        "category": "Math Operations",
        "name": "Horizontal Math (Sum/Average)",
        "description": "Computes row-wise sum or average across multiple columns into a NEW column. This is the primary way to add columns together or compute averages.",
        "config_schema": {"columns": ["a", "b"], "new_col": "total", "op": "sum"},
        "use_cases": [
            "Create a new column that is the sum of two or more columns",
            "Add columns together into a total",
            "Create column C = A + B",
            "Total compensation = salary + bonus",
            "Sum of multiple numeric columns",
            "Average of scores",
            "Row-wise addition",
            "Compute total from parts",
        ],
        "examples": [
            "C = A + B: {columns: ['A', 'B'], new_col: 'C', op: 'sum'}",
            "Total comp: {columns: ['salary', 'bonus'], new_col: 'total_compensation', op: 'sum'}",
            "Avg score: {columns: ['s1', 's2', 's3'], new_col: 'avg', op: 'average'}",
        ],
    },
    {
        "id": "math_custom",
        "category": "Math Operations",
        "name": "Custom Math Expression",
        "description": "Applies arithmetic (+, -, *, /) between column(s) and a scalar or another column. Creates new columns with a suffix.",
        "config_schema": {"left_cols": ["salary"], "op": "*", "right_val": 1.1, "new_suffix": "adjusted"},
        "use_cases": [
            "Multiply a column by a number",
            "Divide values by a constant",
            "Calculate percentage",
            "Increase salary by 10%",
            "Subtract a value from a column",
        ],
        "examples": [
            "Salary * 1.1: {left_cols: ['salary'], op: '*', right_val: 1.1, new_suffix: 'raised'}",
        ],
    },
    {
        "id": "math_multiply_bulk",
        "category": "Math Operations",
        "name": "Bulk Multiply",
        "description": "Multiplies several columns by the same factor.",
        "config_schema": {"columns": ["price", "cost"], "factor": 1.08, "suffix": "_inflated"},
        "use_cases": ["Apply same multiplier to multiple columns", "Inflate prices"],
        "examples": [],
    },

    # ── VECTOR OPERATIONS ───────────────────────────────────────────────────
    {
        "id": "vector_dot_product",
        "category": "Vector Operations",
        "name": "Dot Product",
        "description": "Computes the dot product of two vectors (column groups).",
        "config_schema": {"vec_a": ["x1", "y1", "z1"], "vec_b": ["x2", "y2", "z2"], "new_col": "dot"},
        "use_cases": ["Dot product", "Vector similarity"],
        "examples": [],
    },
    {
        "id": "vector_linear_multiply",
        "category": "Vector Operations",
        "name": "Element-wise Multiply",
        "description": "Element-wise multiplication of two vectors.",
        "config_schema": {"vec_a": ["a1", "a2"], "vec_b": ["b1", "b2"], "suffix": "_prod"},
        "use_cases": ["Element-wise vector multiplication"],
        "examples": [],
    },
    {
        "id": "vector_cross_product",
        "category": "Vector Operations",
        "name": "Cross Product",
        "description": "Cross product of two 3D vectors. Both must have exactly 3 columns.",
        "config_schema": {"vec_a": ["x1", "y1", "z1"], "vec_b": ["x2", "y2", "z2"], "prefix": "cross"},
        "use_cases": ["3D cross product"],
        "examples": [],
    },

    # ── DATA CLEANING ───────────────────────────────────────────────────────
    {
        "id": "drop_na",
        "category": "Data Cleaning",
        "name": "Drop NA Values",
        "description": "Drops rows with null/missing values. Can target specific columns.",
        "config_schema": {"subset": ["col1"]},
        "use_cases": ["Remove rows with missing values", "Drop nulls in specific columns", "Clean missing data"],
        "examples": ["Drop rows where salary is null: {subset: ['salary']}"],
    },
    {
        "id": "drop_nulls",
        "category": "Data Cleaning",
        "name": "Drop All Nulls",
        "description": "Drops ALL rows that contain any null value.",
        "config_schema": {},
        "use_cases": ["Remove all rows with any null", "Keep only complete rows"],
        "examples": [],
    },
    {
        "id": "drop_duplicates",
        "category": "Data Cleaning",
        "name": "Drop Duplicates",
        "description": "Removes duplicate rows. Can check specific columns.",
        "config_schema": {"subset": ["email"]},
        "use_cases": ["Remove duplicate rows", "Deduplicate by a column", "Keep unique rows"],
        "examples": [],
    },
    {
        "id": "fill_missing",
        "category": "Data Cleaning",
        "name": "Fill Missing Values",
        "description": "Fills null values in a column with a constant.",
        "config_schema": {"column": "salary", "value": 0},
        "use_cases": ["Replace nulls with a default", "Fill missing with zero", "Impute missing data"],
        "examples": ["Fill null salaries with 0: {column: 'salary', value: 0}"],
    },

    # ── DATA TYPES ──────────────────────────────────────────────────────────
    {
        "id": "cast",
        "category": "Data Types",
        "name": "Cast Column Type",
        "description": "Converts a column to int, float, str, or bool.",
        "config_schema": {"column": "age", "dtype": "int"},
        "use_cases": ["Convert column to integer", "Cast to float", "Change data type"],
        "examples": [],
    },

    # ── DATE/TIME ───────────────────────────────────────────────────────────
    {
        "id": "extract_date_parts",
        "category": "Date/Time",
        "name": "Extract Date Parts",
        "description": "Splits a date column into year, month, and day columns.",
        "config_schema": {"column": "created_date"},
        "use_cases": ["Extract year from date", "Get month and day", "Split date into parts"],
        "examples": [],
    },

    # ── JOINS & COMBINE ─────────────────────────────────────────────────────
    {
        "id": "join",
        "category": "Joins & Combine",
        "name": "Join Datasets",
        "description": "Joins two datasets on a common column. Supports inner, left, outer, semi, anti. Requires TWO parent edges.",
        "config_schema": {"on": "user_id", "how": "inner"},
        "use_cases": ["Join two tables on a key", "Merge datasets", "Left join", "VLOOKUP equivalent", "Bring columns from another table"],
        "examples": ["Join on department: {on: 'department', how: 'left'}"],
    },
    {
        "id": "union",
        "category": "Joins & Combine",
        "name": "Union Datasets",
        "description": "Stacks multiple datasets vertically. All must have same columns.",
        "config_schema": {},
        "use_cases": ["Stack datasets vertically", "Append rows from multiple files", "Union all"],
        "examples": [],
    },

    # ── ADVANCED ANALYTICS ──────────────────────────────────────────────────
    {
        "id": "outliers",
        "category": "Advanced Analytics",
        "name": "Remove Outliers",
        "description": "Removes outliers from a numeric column using IQR method.",
        "config_schema": {"column": "salary"},
        "use_cases": ["Remove outliers", "Filter extreme values", "IQR outlier removal"],
        "examples": [],
    },
    {
        "id": "groupby",
        "category": "Advanced Analytics",
        "name": "Group By Aggregation",
        "description": "Groups rows by columns and applies aggregation functions: sum, mean, count, min, max, std, median.",
        "config_schema": {"group_cols": ["department"], "aggs": {"salary": ["sum", "mean", "count"]}},
        "use_cases": ["Group by and sum", "Aggregate by category", "Average per group", "Count per group", "Department-level stats", "Total sales per region"],
        "examples": ["Avg salary by dept: {group_cols: ['department'], aggs: {'salary': ['mean', 'count']}}"],
    },
    {
        "id": "stats",
        "category": "Advanced Analytics",
        "name": "Column Statistics",
        "description": "Computes mean, std, median, q1, q3 for numeric columns.",
        "config_schema": {"columns": ["salary", "score"]},
        "use_cases": ["Calculate statistics", "Descriptive statistics", "Summary stats"],
        "examples": [],
    },
    {
        "id": "pivot",
        "category": "Advanced Analytics",
        "name": "Pivot Table",
        "description": "Creates a pivot table with values, index, pivot column, and aggregation.",
        "config_schema": {"values": "salary", "index": ["department"], "on": "status", "agg": "sum"},
        "use_cases": ["Create a pivot table", "Cross-tabulation"],
        "examples": [],
    },
    {
        "id": "moving_average",
        "category": "Advanced Analytics",
        "name": "Moving Average",
        "description": "Computes a rolling mean with a specified window size.",
        "config_schema": {"column": "price", "window": 7},
        "use_cases": ["Rolling average", "Moving average", "Smooth time series"],
        "examples": [],
    },
    {
        "id": "conditional",
        "category": "Advanced Analytics",
        "name": "Conditional Column (IF-THEN-ELSE)",
        "description": "Creates a new column: IF column op threshold THEN then_val ELSE else_val. Ops: gt, gte, lt, lte, eq, ne. Use this to label, categorize, tag, or flag rows.",
        "config_schema": {"column": "salary", "op": "gt", "threshold": 50000, "then_val": "High", "else_val": "Low", "new_col": "salary_band"},
        "use_cases": [
            "Label rows as High/Low based on a value",
            "Categorize data based on a threshold",
            "IF salary > 50000 THEN High ELSE Low",
            "Tag rows based on condition",
            "Create a flag column",
            "Mark rows as pass/fail",
            "Assign labels based on a rule",
            "Bucket values into categories",
        ],
        "examples": [
            "Label salary: {column: 'salary', op: 'gt', threshold: 50000, then_val: 'High', else_val: 'Low', new_col: 'band'}",
            "Pass/fail: {column: 'score', op: 'gte', threshold: 60, then_val: 'Pass', else_val: 'Fail', new_col: 'result'}",
        ],
    },

    # ── MATRIX OPERATIONS ───────────────────────────────────────────────────
    {
        "id": "transpose",
        "category": "Matrix Operations",
        "name": "Matrix Transpose",
        "description": "Transposes the dataframe — rows become columns.",
        "config_schema": {},
        "use_cases": ["Transpose data", "Flip rows and columns"],
        "examples": [],
    },

    # ── UTILITY NODES (NEW) ─────────────────────────────────────────────────
    {
        "id": "add_literal_column",
        "category": "Utility",
        "name": "Add Constant Column",
        "description": "Adds a new column with a constant value to every row. Use for tagging, labeling, or adding fixed metadata.",
        "config_schema": {"column": "tag", "value": "2023Q1", "dtype": "string"},
        "use_cases": ["Add a tag column", "Add constant value", "Label all rows", "Add metadata column"],
        "examples": ["Add period tag: {column: 'period', value: '2023Q1', dtype: 'string'}"],
    },
    {
        "id": "range_bucket",
        "category": "Utility",
        "name": "Range Bucket",
        "description": "Buckets a numeric column into labeled ranges. Bins define boundaries, labels name each range.",
        "config_schema": {"column": "DPD", "bins": [0, 30, 60, 90], "labels": ["0", "1-30", "31-60", "61-90", "90+"], "new_col": "bucket"},
        "use_cases": ["DPD bucketing", "Age groups", "Salary bands", "Score ranges", "Bin numeric values"],
        "examples": ["DPD buckets: {column: 'DPD', bins: [0,30,60,90], labels: ['0','1-30','31-60','61-90','90+'], new_col: 'bucket'}"],
    },
    {
        "id": "date_offset",
        "category": "Utility",
        "name": "Date Offset",
        "description": "Adds or subtracts a fixed duration from a date column. Supports days, weeks, months, years.",
        "config_schema": {"column": "date", "offset": 1, "unit": "months", "new_col": "next_month"},
        "use_cases": ["Shift dates forward/backward", "Create next month column", "Date arithmetic"],
        "examples": ["Next month: {column: 'month', offset: 1, unit: 'months', new_col: 'next_month'}"],
    },
    {
        "id": "crosstab",
        "category": "Utility",
        "name": "Cross Tabulation",
        "description": "Creates a cross-tabulation of two categorical columns. Counts or aggregates values.",
        "config_schema": {"index": "from_bucket", "columns": "to_bucket", "values": None, "agg": "count"},
        "use_cases": ["Transition matrix", "Contingency table", "Cross-tab two categories"],
        "examples": ["Transition counts: {index: 'from_bucket', columns: 'to_bucket', agg: 'count'}"],
    },
    {
        "id": "cumulative_product",
        "category": "Utility",
        "name": "Cumulative Product",
        "description": "Computes the cumulative product along a column. Useful for chain probabilities.",
        "config_schema": {"column": "probability", "new_col": "chain_prob"},
        "use_cases": ["Chain probability", "Compound interest", "Cumulative multiplication"],
        "examples": ["Chain prob: {column: 'transition_prob', new_col: 'chain_prob'}"],
    },

    # ── MACHINE LEARNING ────────────────────────────────────────────────────
    {
        "id": "linear_regression",
        "category": "Machine Learning",
        "name": "Linear Regression",
        "description": "Fits OLS linear regression. Output is a coefficient table.",
        "config_schema": {"target": "salary", "features": ["experience", "age"]},
        "use_cases": ["Linear regression", "Predict a numeric value", "Feature importance", "OLS regression"],
        "examples": [],
    },

    # ── ENHANCED REGRESSION + STATS + VIZ ─────────────────────────────────
    {
        "id": "ols_regression",
        "category": "Statistical Tests",
        "name": "OLS Regression (Full)",
        "description": "Full OLS with coefficients, std errors, t-stats, p-values, R-squared, F-statistic.",
        "config_schema": {"target": "salary", "features": ["experience", "age"]},
        "use_cases": ["Full regression analysis", "Get p-values", "R-squared", "Feature significance"],
        "examples": [],
    },
    {
        "id": "t_test",
        "category": "Statistical Tests",
        "name": "T-Test",
        "description": "Student's t-test. One-sample, two-sample (Welch's), or paired.",
        "config_schema": {"column_a": "salary", "column_b": "bonus", "test_type": "two_sample", "popmean": 0},
        "use_cases": ["Compare means of two groups", "Test if mean differs from a value"],
        "examples": [],
    },
    {
        "id": "f_test",
        "category": "Statistical Tests",
        "name": "F-Test",
        "description": "F-test for equality of variances between two numeric columns.",
        "config_schema": {"column_a": "salary", "column_b": "bonus"},
        "use_cases": ["Compare variances", "Test variance equality"],
        "examples": [],
    },
    {
        "id": "chi_square_test",
        "category": "Statistical Tests",
        "name": "Chi-Square Test",
        "description": "Chi-square test of independence between two categorical columns.",
        "config_schema": {"column_a": "department", "column_b": "status"},
        "use_cases": ["Test independence of categories", "Association test"],
        "examples": [],
    },
    {
        "id": "dw_test",
        "category": "Statistical Tests",
        "name": "Durbin-Watson Test",
        "description": "Tests for autocorrelation in regression residuals.",
        "config_schema": {"residuals_col": "residuals"},
        "use_cases": ["Check residual autocorrelation", "Model diagnostics"],
        "examples": [],
    },
    {
        "id": "anova_test",
        "category": "Statistical Tests",
        "name": "ANOVA",
        "description": "One-way ANOVA. Tests if means differ across multiple groups.",
        "config_schema": {"value_col": "salary", "group_col": "department"},
        "use_cases": ["Compare means across groups", "Multi-group comparison"],
        "examples": [],
    },
    {
        "id": "chart",
        "category": "Visualization",
        "name": "Chart",
        "description": "Generate bar, line, scatter, histogram, heatmap, or pie chart.",
        "config_schema": {"chart_type": "bar", "x_col": "department", "y_col": "salary", "title": ""},
        "use_cases": ["Bar chart", "Line graph", "Scatter plot", "Histogram", "Heatmap", "Pie chart", "Visualize data"],
        "examples": [],
    },

    {
        "id": "logistic_prediction",
        "category": "Machine Learning",
        "name": "Logistic Regression Prediction",
        "description": "Applies a logistic regression model with given weights. Adds a probability column.",
        "config_schema": {"features": ["experience", "score"], "weights": [0.5, -0.3]},
        "use_cases": ["Logistic regression prediction", "Binary classification probability"],
        "examples": [],
    },
]


def get_all_node_ids():
    return [doc["id"] for doc in NODE_DOCUMENTS]


def get_node_by_id(node_id):
    return next((doc for doc in NODE_DOCUMENTS if doc["id"] == node_id), None)


def build_embedding_texts():
    """Returns (node_id, text_to_embed) pairs for vector store indexing."""
    pairs = []
    for doc in NODE_DOCUMENTS:
        parts = [
            f"Node: {doc['name']} (type: {doc['id']})",
            f"Category: {doc['category']}",
            f"Description: {doc['description']}",
            f"Use cases: {'; '.join(doc['use_cases'])}",
        ]
        if doc["examples"]:
            parts.append(f"Examples: {'; '.join(doc['examples'])}")
        pairs.append((doc["id"], "\n".join(parts)))
    return pairs


def build_node_spec(node_id):
    """Build a detailed spec string for a single node (used in LLM prompts)."""
    import json as _json
    doc = get_node_by_id(node_id)
    if not doc:
        return ""
    lines = [
        f"* {doc['id']}  ({doc['name']})",
        f"  Category: {doc['category']}",
        f"  Description: {doc['description']}",
        f"  Config: {_json.dumps(doc['config_schema'])}",
    ]
    if doc["use_cases"]:
        lines.append(f"  Use cases: {'; '.join(doc['use_cases'][:5])}")
    if doc["examples"]:
        lines.append(f"  Examples: {'; '.join(doc['examples'][:3])}")
    return "\n".join(lines)
