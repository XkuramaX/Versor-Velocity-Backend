"""
Tests for the 5 new utility nodes.
Run: cd backend && python -m pytest ai/test_new_nodes.py -v
"""

import pytest
import polars as pl
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controllers.DataframeController import DataframeController


@pytest.fixture
def engine():
    e = DataframeController()
    return e


@pytest.fixture
def sample_node(engine):
    """Create a sample node with test data."""
    df = pl.DataFrame({
        "account": ["A1", "A2", "A3", "A4", "A5"],
        "dpd": [0, 15, 45, 75, 120],
        "salary": [50000, 60000, 70000, 80000, 90000],
        "date": ["2023-01-15", "2023-02-15", "2023-03-15", "2023-04-15", "2023-05-15"],
        "bucket_from": ["0", "1-30", "31-60", "61-90", "90+"],
        "bucket_to": ["1-30", "31-60", "61-90", "90+", "90+"],
        "prob": [0.8, 0.6, 0.5, 0.4, 0.3],
    }).lazy()
    node_id = engine.save_node_result(df, "test_source")
    return node_id


class TestAddLiteralColumn:
    def test_add_string_constant(self, engine, sample_node):
        result_id = engine.add_literal_column(sample_node, column="tag", value="Q1-2023", dtype="string")
        df = engine.registry[result_id].collect()
        assert "tag" in df.columns
        assert df["tag"].to_list() == ["Q1-2023"] * 5

    def test_add_integer_constant(self, engine, sample_node):
        result_id = engine.add_literal_column(sample_node, column="year", value=2023, dtype="integer")
        df = engine.registry[result_id].collect()
        assert df["year"].to_list() == [2023] * 5

    def test_add_float_constant(self, engine, sample_node):
        result_id = engine.add_literal_column(sample_node, column="rate", value=0.05, dtype="float")
        df = engine.registry[result_id].collect()
        assert all(abs(v - 0.05) < 0.001 for v in df["rate"].to_list())


class TestRangeBucket:
    def test_dpd_bucketing(self, engine, sample_node):
        result_id = engine.range_bucket(
            sample_node,
            column="dpd",
            bins=[0, 30, 60, 90],
            labels=["0", "1-30", "31-60", "61-90", "90+"],
            new_col="dpd_bucket"
        )
        df = engine.registry[result_id].collect()
        assert "dpd_bucket" in df.columns
        buckets = df["dpd_bucket"].to_list()
        assert buckets == ["0", "1-30", "31-60", "61-90", "90+"]

    def test_salary_bands(self, engine, sample_node):
        result_id = engine.range_bucket(
            sample_node,
            column="salary",
            bins=[55000, 75000],
            labels=["Low", "Mid", "High"],
            new_col="band"
        )
        df = engine.registry[result_id].collect()
        bands = df["band"].to_list()
        assert bands == ["Low", "Mid", "Mid", "High", "High"]

    def test_wrong_label_count_raises(self, engine, sample_node):
        result = engine.range_bucket(sample_node, column="dpd", bins=[0, 30], labels=["A"], new_col="x")
        # The @safe_node_execution decorator catches errors and returns error dict
        assert isinstance(result, dict) and result.get("status") == "error"


class TestDateOffset:
    def test_add_days(self, engine):
        df = pl.DataFrame({"dt": ["2023-01-15", "2023-02-15"]}).with_columns(
            pl.col("dt").str.to_date("%Y-%m-%d")
        ).lazy()
        node_id = engine.save_node_result(df, "date_src")

        result_id = engine.date_offset(node_id, column="dt", offset=10, unit="days", new_col="dt_plus10")
        result = engine.registry[result_id].collect()
        assert "dt_plus10" in result.columns
        dates = result["dt_plus10"].to_list()
        assert str(dates[0]) == "2023-01-25"

    def test_add_months(self, engine):
        df = pl.DataFrame({"dt": ["2023-01-15", "2023-06-15"]}).with_columns(
            pl.col("dt").str.to_date("%Y-%m-%d")
        ).lazy()
        node_id = engine.save_node_result(df, "date_src2")

        result_id = engine.date_offset(node_id, column="dt", offset=1, unit="months", new_col="next_month")
        result = engine.registry[result_id].collect()
        dates = result["next_month"].to_list()
        assert str(dates[0]) == "2023-02-15"
        assert str(dates[1]) == "2023-07-15"


class TestCrosstab:
    def test_count_crosstab(self, engine, sample_node):
        result_id = engine.crosstab(sample_node, index="bucket_from", columns="bucket_to", agg="count")
        df = engine.registry[result_id].collect()
        assert "bucket_from" in df.columns
        # Should have columns for each unique bucket_to value
        assert df.shape[0] > 0

    def test_crosstab_has_correct_shape(self, engine):
        df = pl.DataFrame({
            "from": ["A", "A", "B", "B"],
            "to": ["X", "Y", "X", "Y"],
        }).lazy()
        node_id = engine.save_node_result(df, "ct_src")

        result_id = engine.crosstab(node_id, index="from", columns="to", agg="count")
        result = engine.registry[result_id].collect()
        assert result.shape == (2, 3)  # 2 rows (A,B) x 3 cols (from, X, Y)


class TestCumulativeProduct:
    def test_basic_cumprod(self, engine, sample_node):
        result_id = engine.cumulative_product(sample_node, column="prob", new_col="chain")
        df = engine.registry[result_id].collect()
        assert "chain" in df.columns
        chain = df["chain"].to_list()
        assert abs(chain[0] - 0.8) < 0.001
        assert abs(chain[1] - 0.48) < 0.001  # 0.8 * 0.6
        assert abs(chain[2] - 0.24) < 0.001  # 0.8 * 0.6 * 0.5

    def test_cumprod_default_col_name(self, engine, sample_node):
        result_id = engine.cumulative_product(sample_node, column="prob")
        df = engine.registry[result_id].collect()
        assert "prob_cumprod" in df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
