"""
Tests for the Planner → Validator → Compiler pipeline.
Tests the Validator and Compiler deterministically (no LLM needed).
Run: cd backend && python -m pytest ai/test_pipeline.py -v
"""

import pytest
from ai.validator import validate
from ai.compiler import compile_to_react_flow
from ai.schema_inferrer import infer_schema


EMP_CSV = b"emp_id,name,department,salary,age\n1,Alice,Eng,85000,32\n2,Bob,Sales,52000,28\n"
EMP_SCHEMA = [{"name": "employees.csv", "schema": infer_schema(EMP_CSV, "e.csv")}]

PRODUCTS_CSV = b"product_id,product_name,price\n1,Widget,29.99\n2,Gadget,49.99\n"
SALES_CSV = b"sale_id,product_id,quantity\n1,1,5\n2,2,3\n"


class TestSimpleFilter:
    def test_filter_salary(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "safe_filter", "input": "emp", "config": {
                "filters": [{"column": "salary", "operation": "gt", "value": 50000, "column_type": "numeric"}],
                "logic": "and"
            }},
        ]
        vr = validate(steps, EMP_SCHEMA)
        assert vr.errors == []
        wf = compile_to_react_flow(vr.steps)
        assert len(wf["nodes"]) == 2
        assert len(wf["edges"]) == 1
        assert wf["nodes"][0]["data"]["nodeType"] == "upload_csv"
        assert wf["nodes"][1]["data"]["nodeType"] == "safe_filter"


class TestAggregation:
    def test_groupby_avg_age(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "groupby", "input": "emp", "config": {
                "group_cols": ["department"], "aggs": {"age": ["mean"]}
            }},
        ]
        vr = validate(steps, EMP_SCHEMA)
        assert vr.errors == []
        wf = compile_to_react_flow(vr.steps)
        assert wf["nodes"][1]["data"]["nodeType"] == "groupby"


class TestMultiStep:
    def test_clean_then_sort(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "string_clean", "input": "emp", "config": {"column": "name"}},
            {"action": "sort", "input": "string_clean", "config": {"by": ["salary"], "descending": True}},
        ]
        vr = validate(steps, EMP_SCHEMA)
        assert vr.errors == []
        wf = compile_to_react_flow(vr.steps)
        assert len(wf["nodes"]) == 3
        assert len(wf["edges"]) == 2
        # Linear chain
        assert wf["edges"][0]["target"] == wf["nodes"][1]["id"]
        assert wf["edges"][1]["target"] == wf["nodes"][2]["id"]


class TestJoin:
    def test_join_two_files(self):
        schemas = [
            {"name": "sales.csv", "schema": infer_schema(SALES_CSV, "s.csv")},
            {"name": "products.csv", "schema": infer_schema(PRODUCTS_CSV, "p.csv")},
        ]
        steps = [
            {"action": "upload_csv", "alias": "sales", "source_file": "sales.csv"},
            {"action": "upload_csv", "alias": "products", "source_file": "products.csv"},
            {"action": "join", "input": ["sales", "products"], "config": {"on": "product_id", "how": "inner"}},
        ]
        vr = validate(steps, schemas)
        assert vr.errors == []
        wf = compile_to_react_flow(vr.steps)
        assert len(wf["nodes"]) == 3
        join_edges = [e for e in wf["edges"] if e["target"] == wf["nodes"][2]["id"]]
        assert len(join_edges) == 2


class TestInvalidSchema:
    def test_missing_column_warns(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "safe_filter", "input": "emp", "config": {
                "filters": [{"column": "nonexistent", "operation": "gt", "value": 100, "column_type": "numeric"}],
                "logic": "and"
            }},
        ]
        vr = validate(steps, EMP_SCHEMA)
        assert len(vr.warnings) > 0
        assert "nonexistent" in vr.warnings[0]

    def test_invalid_action_errors(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "neural_network", "config": {}},
        ]
        vr = validate(steps, EMP_SCHEMA)
        assert len(vr.errors) > 0
        assert "neural_network" in vr.errors[0]


class TestSumColumns:
    def test_c_equals_a_plus_b(self):
        schemas = [{"name": "data.csv", "schema": {"A": "integer", "B": "integer"}}]
        steps = [
            {"action": "upload_csv", "alias": "data", "source_file": "data.csv"},
            {"action": "math_horizontal", "input": "data", "config": {
                "columns": ["A", "B"], "new_col": "C", "op": "sum"
            }},
        ]
        vr = validate(steps, schemas)
        assert vr.errors == []
        assert vr.warnings == []
        wf = compile_to_react_flow(vr.steps)
        assert wf["nodes"][1]["data"]["config"]["new_col"] == "C"


class TestSchemaTracking:
    def test_rename_updates_schema(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "rename", "input": "emp", "alias": "renamed", "config": {"mapping": {"name": "employee_name"}}},
            {"action": "string_clean", "input": "renamed", "config": {"column": "employee_name"}},
        ]
        vr = validate(steps, EMP_SCHEMA)
        assert vr.errors == []
        assert vr.warnings == []  # employee_name should exist after rename

    def test_select_narrows_schema(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "select", "input": "emp", "alias": "selected", "config": {"columns": ["name", "salary"]}},
            {"action": "sort", "input": "selected", "config": {"by": ["department"], "descending": False}},
        ]
        vr = validate(steps, EMP_SCHEMA)
        # department was dropped by select, so sort should warn
        assert any("department" in w for w in vr.warnings)


class TestCompilerOutput:
    def test_all_nodes_have_required_fields(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "safe_filter", "input": "emp", "config": {"filters": [], "logic": "and"}},
        ]
        vr = validate(steps, EMP_SCHEMA)
        wf = compile_to_react_flow(vr.steps)
        for node in wf["nodes"]:
            assert node["type"] == "custom"
            assert "position" in node
            assert "x" in node["position"]
            assert "y" in node["position"]
            d = node["data"]
            assert "label" in d
            assert "nodeType" in d
            assert "config" in d
            assert d["status"] == "idle"
            assert d["backendNodeId"] is None

    def test_positions_increase(self):
        steps = [
            {"action": "upload_csv", "alias": "emp", "source_file": "employees.csv"},
            {"action": "safe_filter", "input": "emp", "config": {}},
            {"action": "sort", "input": "safe_filter", "config": {"by": ["salary"]}},
        ]
        vr = validate(steps, EMP_SCHEMA)
        wf = compile_to_react_flow(vr.steps)
        xs = [n["position"]["x"] for n in wf["nodes"]]
        assert xs[0] < xs[1] < xs[2]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
