"""
Tests for statistical tests, enhanced regression, and visualization nodes.
Run: cd backend && python -m pytest ai/test_stats_viz.py -v
"""

import pytest
import polars as pl
import numpy as np
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from controllers.DataframeController import DataframeController


@pytest.fixture
def engine():
    return DataframeController()


@pytest.fixture
def stats_node(engine):
    np.random.seed(42)
    df = pl.DataFrame({
        "salary": np.random.normal(70000, 15000, 100).tolist(),
        "bonus": np.random.normal(5000, 2000, 100).tolist(),
        "age": np.random.randint(22, 65, 100).tolist(),
        "experience": np.random.randint(0, 30, 100).tolist(),
        "department": np.random.choice(["Eng", "Sales", "HR", "Marketing"], 100).tolist(),
        "status": np.random.choice(["Active", "Inactive"], 100).tolist(),
    }).lazy()
    return engine.save_node_result(df, "stats_src")


class TestOLSRegression:
    def test_produces_coefficients(self, engine, stats_node):
        result_id = engine.ols_regression(stats_node, target="salary", features=["age", "experience"])
        df = engine.registry[result_id].collect()
        assert "feature" in df.columns
        assert "coefficient" in df.columns
        assert "p_value" in df.columns
        assert "t_statistic" in df.columns
        assert "r_squared" in df.columns
        assert df.shape[0] == 3  # intercept + 2 features

    def test_r_squared_in_range(self, engine, stats_node):
        result_id = engine.ols_regression(stats_node, target="salary", features=["age", "experience"])
        df = engine.registry[result_id].collect()
        r2 = df["r_squared"][0]
        assert 0 <= r2 <= 1

    def test_features_listed(self, engine, stats_node):
        result_id = engine.ols_regression(stats_node, target="salary", features=["age", "experience"])
        df = engine.registry[result_id].collect()
        features = df["feature"].to_list()
        assert features == ["intercept", "age", "experience"]


class TestTTest:
    def test_two_sample(self, engine, stats_node):
        result_id = engine.t_test(stats_node, column_a="salary", column_b="bonus", test_type="two_sample")
        df = engine.registry[result_id].collect()
        assert "t_statistic" in df.columns
        assert "p_value" in df.columns
        assert df["n_a"][0] == 100

    def test_one_sample(self, engine, stats_node):
        result_id = engine.t_test(stats_node, column_a="salary", test_type="one_sample", popmean=70000)
        df = engine.registry[result_id].collect()
        assert "One-sample" in df["test"][0]

    def test_paired(self, engine, stats_node):
        result_id = engine.t_test(stats_node, column_a="salary", column_b="bonus", test_type="paired")
        df = engine.registry[result_id].collect()
        assert "Paired" in df["test"][0]


class TestFTest:
    def test_variance_comparison(self, engine, stats_node):
        result_id = engine.f_test(stats_node, column_a="salary", column_b="bonus")
        df = engine.registry[result_id].collect()
        assert "f_statistic" in df.columns
        assert "variance_a" in df.columns
        assert df["f_statistic"][0] > 0


class TestChiSquare:
    def test_independence(self, engine, stats_node):
        result_id = engine.chi_square_test(stats_node, column_a="department", column_b="status")
        df = engine.registry[result_id].collect()
        assert "chi2_statistic" in df.columns
        assert "p_value" in df.columns
        assert df["n_categories_a"][0] == 4


class TestDWTest:
    def test_autocorrelation(self, engine):
        np.random.seed(42)
        df = pl.DataFrame({"residuals": np.random.normal(0, 1, 100).tolist()}).lazy()
        node_id = engine.save_node_result(df, "dw_src")
        result_id = engine.dw_test(node_id, residuals_col="residuals")
        df = engine.registry[result_id].collect()
        assert "dw_statistic" in df.columns
        dw = df["dw_statistic"][0]
        assert 0 <= dw <= 4


class TestANOVA:
    def test_group_comparison(self, engine, stats_node):
        result_id = engine.anova_test(stats_node, value_col="salary", group_col="department")
        df = engine.registry[result_id].collect()
        assert "f_statistic" in df.columns
        assert "p_value" in df.columns


class TestChart:
    def test_bar_chart(self, engine, stats_node):
        result_id = engine.chart_node(stats_node, chart_type="bar", x_col="department", y_col="salary", title="Test Bar")
        assert result_id is not None
        img = engine.get_chart_image(result_id)
        assert img is not None
        assert len(img) > 100  # base64 string should be substantial

    def test_histogram(self, engine, stats_node):
        result_id = engine.chart_node(stats_node, chart_type="histogram", x_col="salary", bins=15)
        img = engine.get_chart_image(result_id)
        assert img is not None

    def test_scatter(self, engine, stats_node):
        result_id = engine.chart_node(stats_node, chart_type="scatter", x_col="age", y_col="salary")
        img = engine.get_chart_image(result_id)
        assert img is not None

    def test_heatmap(self, engine, stats_node):
        result_id = engine.chart_node(stats_node, chart_type="heatmap")
        img = engine.get_chart_image(result_id)
        assert img is not None

    def test_data_passes_through(self, engine, stats_node):
        result_id = engine.chart_node(stats_node, chart_type="bar", x_col="department", y_col="salary")
        df = engine.registry[result_id].collect()
        assert df.shape[0] == 100  # original data preserved
        assert "salary" in df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
