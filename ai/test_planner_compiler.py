"""
Test Suite for the Planner-Compiler Architecture
─────────────────────────────────────────────────
Tests the WorkflowCompiler deterministically (no LLM needed).
Tests the SchemaInferrer with real CSV data.
Tests 5 scenarios: simple filter, aggregation, multi-step, join, invalid schema.

Run: cd backend && python -m pytest ai/test_planner_compiler.py -v
"""

import json
import pytest
from ai.workflow_compiler import WorkflowCompiler
from ai.schema_inferrer import infer_schema
from ai.node_knowledge import get_all_node_ids


# ── Test data ─────────────────────────────────────────────────────────────────

EMPLOYEES_CSV = b"""emp_id,name,department,salary,age,hire_date
1,Alice,Engineering,85000,32,2020-01-15
2,Bob,Sales,52000,28,2021-06-01
3,Carol,Engineering,95000,35,2019-03-20
4,Dave,HR,48000,41,2022-11-10
5,Eve,Sales,61000,29,2020-08-05
"""

PRODUCTS_CSV = b"""product_id,product_name,category,price
101,Widget,Electronics,29.99
102,Gadget,Electronics,49.99
103,Chair,Furniture,199.99
"""

SALES_CSV = b"""sale_id,product_id,quantity,region
1,101,5,North
2,102,3,South
3,101,8,North
4,103,1,East
"""


# ── SchemaInferrer tests ──────────────────────────────────────────────────────

class TestSchemaInferrer:
    def test_infer_employees(self):
        schema = infer_schema(EMPLOYEES_CSV, "employees.csv")
        assert "emp_id" in schema
        assert "name" in schema
        assert "salary" in schema
        assert schema["emp_id"] == "integer"
        assert schema["name"] == "string"
        assert schema["salary"] == "integer"
        assert schema["age"] == "integer"

    def test_infer_products(self):
        schema = infer_schema(PRODUCTS_CSV, "products.csv")
        assert schema["product_id"] == "integer"
        assert schema["product_name"] == "string"
        assert schema["price"] == "float"

    def test_infer_empty(self):
        schema = infer_schema(b"", "empty.csv")
        assert schema == {}

    def test_infer_invalid(self):
        schema = infer_schema(b"not a csv at all {{{{", "bad.csv")
        assert isinstance(schema, dict)


# ── Scenario 1: Simple Filter ────────────────────────────────────────────────

class TestSimpleFilter:
    """'Show me employees with salary over 50000'"""

    def setup_method(self):
        self.schemas = [{"name": "employees.csv", "schema": infer_schema(EMPLOYEES_CSV, "employees.csv")}]
        self.recipe = [
            {"node_type": "upload_csv", "config": {}, "label": "employees.csv", "source_file": "employees.csv"},
            {"node_type": "safe_filter", "config": {
                "filters": [{"column": "salary", "operation": "gt", "value": 50000, "column_type": "numeric"}],
                "logic": "and"
            }, "label": "Filter salary > 50000"},
        ]

    def test_compiles_successfully(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        assert len(result["nodes"]) == 2
        assert len(result["edges"]) == 1
        assert result["errors"] == []

    def test_node_types_correct(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        assert result["nodes"][0]["data"]["nodeType"] == "upload_csv"
        assert result["nodes"][1]["data"]["nodeType"] == "safe_filter"

    def test_edge_connects_upload_to_filter(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        edge = result["edges"][0]
        assert edge["source"] == result["nodes"][0]["id"]
        assert edge["target"] == result["nodes"][1]["id"]

    def test_positions_are_sequential(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        x0 = result["nodes"][0]["position"]["x"]
        x1 = result["nodes"][1]["position"]["x"]
        assert x1 > x0

    def test_data_structure_complete(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        for node in result["nodes"]:
            assert node["type"] == "custom"
            assert "position" in node
            data = node["data"]
            assert "label" in data
            assert "nodeType" in data
            assert "config" in data
            assert data["status"] == "idle"
            assert data["backendNodeId"] is None


# ── Scenario 2: Aggregation ──────────────────────────────────────────────────

class TestAggregation:
    """'Group by department and show average age'"""

    def setup_method(self):
        self.schemas = [{"name": "employees.csv", "schema": infer_schema(EMPLOYEES_CSV, "employees.csv")}]
        self.recipe = [
            {"node_type": "upload_csv", "config": {}, "label": "employees.csv", "source_file": "employees.csv"},
            {"node_type": "groupby", "config": {
                "group_cols": ["department"],
                "aggs": {"age": ["mean"]}
            }, "label": "Avg age by department"},
        ]

    def test_compiles_with_correct_nodes(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        assert len(result["nodes"]) == 2
        assert result["nodes"][1]["data"]["nodeType"] == "groupby"
        assert result["nodes"][1]["data"]["config"]["group_cols"] == ["department"]

    def test_no_column_warnings(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        assert result["warnings"] == []


# ── Scenario 3: Multi-step ───────────────────────────────────────────────────

class TestMultiStep:
    """'Clean the name column, then sort by hire_date'"""

    def setup_method(self):
        self.schemas = [{"name": "employees.csv", "schema": infer_schema(EMPLOYEES_CSV, "employees.csv")}]
        self.recipe = [
            {"node_type": "upload_csv", "config": {}, "label": "employees.csv", "source_file": "employees.csv"},
            {"node_type": "string_clean", "config": {"column": "name"}, "label": "Clean name"},
            {"node_type": "sort", "config": {"by": ["hire_date"], "descending": False}, "label": "Sort by hire_date"},
        ]

    def test_three_nodes_two_edges(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        assert len(result["nodes"]) == 3
        assert len(result["edges"]) == 2

    def test_linear_chain(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        # Edge 1: upload -> clean
        assert result["edges"][0]["source"] == result["nodes"][0]["id"]
        assert result["edges"][0]["target"] == result["nodes"][1]["id"]
        # Edge 2: clean -> sort
        assert result["edges"][1]["source"] == result["nodes"][1]["id"]
        assert result["edges"][1]["target"] == result["nodes"][2]["id"]

    def test_x_positions_increase(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        xs = [n["position"]["x"] for n in result["nodes"]]
        assert xs == sorted(xs)
        assert xs[0] < xs[1] < xs[2]


# ── Scenario 4: Complex Join ─────────────────────────────────────────────────

class TestComplexJoin:
    """'Join the sales data with the product list on product_id'"""

    def setup_method(self):
        self.schemas = [
            {"name": "sales.csv", "schema": infer_schema(SALES_CSV, "sales.csv")},
            {"name": "products.csv", "schema": infer_schema(PRODUCTS_CSV, "products.csv")},
        ]
        self.recipe = [
            {"node_type": "upload_csv", "config": {}, "label": "sales.csv", "source_file": "sales.csv"},
            {"node_type": "upload_csv", "config": {}, "label": "products.csv", "source_file": "products.csv"},
            {"node_type": "join", "config": {"on": "product_id", "how": "inner"}, "label": "Join on product_id", "join_sources": [0, 1]},
        ]

    def test_join_has_two_input_edges(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        assert len(result["nodes"]) == 3
        # Join node should have 2 incoming edges
        join_id = result["nodes"][2]["id"]
        incoming = [e for e in result["edges"] if e["target"] == join_id]
        assert len(incoming) == 2

    def test_join_sources_are_upload_nodes(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        join_id = result["nodes"][2]["id"]
        incoming = [e for e in result["edges"] if e["target"] == join_id]
        source_ids = {e["source"] for e in incoming}
        upload_ids = {result["nodes"][0]["id"], result["nodes"][1]["id"]}
        assert source_ids == upload_ids


# ── Scenario 5: Invalid Schema ───────────────────────────────────────────────

class TestInvalidSchema:
    """User asks to filter a column that doesn't exist"""

    def setup_method(self):
        self.schemas = [{"name": "employees.csv", "schema": infer_schema(EMPLOYEES_CSV, "employees.csv")}]
        self.recipe = [
            {"node_type": "upload_csv", "config": {}, "label": "employees.csv", "source_file": "employees.csv"},
            {"node_type": "safe_filter", "config": {
                "filters": [{"column": "nonexistent_column", "operation": "gt", "value": 100, "column_type": "numeric"}],
                "logic": "and"
            }, "label": "Filter nonexistent"},
        ]

    def test_compiles_with_warning(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        # Should still compile (workflow is structurally valid)
        assert len(result["nodes"]) == 2
        # But should have a warning about the missing column
        assert len(result["warnings"]) > 0
        assert "nonexistent_column" in result["warnings"][0]

    def test_invalid_node_type_produces_error(self):
        recipe = [
            {"node_type": "upload_csv", "config": {}, "label": "test.csv"},
            {"node_type": "neural_network", "config": {}, "label": "Train NN"},
        ]
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(recipe)
        assert len(result["errors"]) > 0
        assert "neural_network" in result["errors"][0]


# ── Scenario 6: Sum of two columns (the original failing case) ────────────────

class TestSumColumns:
    """'Create column C = A + B'"""

    def setup_method(self):
        self.schemas = [{"name": "data.csv", "schema": {"A": "integer", "B": "integer"}}]
        self.recipe = [
            {"node_type": "upload_csv", "config": {}, "label": "data.csv", "source_file": "data.csv"},
            {"node_type": "math_horizontal", "config": {
                "columns": ["A", "B"], "new_col": "C", "op": "sum"
            }, "label": "C = A + B"},
        ]

    def test_compiles_correctly(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        assert len(result["nodes"]) == 2
        assert len(result["edges"]) == 1
        assert result["errors"] == []
        assert result["warnings"] == []

    def test_math_horizontal_config_preserved(self):
        compiler = WorkflowCompiler(self.schemas)
        result = compiler.compile(self.recipe)
        math_node = result["nodes"][1]
        assert math_node["data"]["nodeType"] == "math_horizontal"
        assert math_node["data"]["config"]["columns"] == ["A", "B"]
        assert math_node["data"]["config"]["new_col"] == "C"
        assert math_node["data"]["config"]["op"] == "sum"


# ── Node catalogue completeness ───────────────────────────────────────────────

class TestNodeCatalogue:
    def test_all_38_nodes_registered(self):
        ids = get_all_node_ids()
        assert len(ids) == 38

    def test_no_duplicate_ids(self):
        ids = get_all_node_ids()
        assert len(ids) == len(set(ids))

    def test_all_ids_are_valid_strings(self):
        for nid in get_all_node_ids():
            assert isinstance(nid, str)
            assert len(nid) > 0
            assert " " not in nid


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
