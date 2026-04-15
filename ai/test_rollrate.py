"""
Tests for Roll-Rate Analysis Nodes
───────────────────────────────────
Tests: monthly_snapshot, transition_matrix, period_average_matrix, chain_probability
Uses synthetic DPD data mimicking the real credit risk dataset.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import polars as pl
import pytest
from controllers.DataframeController import DataframeController
from controllers.WorkflowManager import WorkflowManager


@pytest.fixture
def engine():
    return DataframeController()


@pytest.fixture
def wm(engine):
    return WorkflowManager(engine)


def _make_dpd_data(engine):
    """Create synthetic DPD data: 10 accounts, 6 months, varying DPD values."""
    rows = []
    accounts = [f"ACC{i:03d}" for i in range(1, 11)]
    months = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]

    # Each account has multiple observations per month (different dates)
    import random
    random.seed(42)
    for acc in accounts:
        dpd = 0
        for m in months:
            # 2-3 observations per month
            for day in [5, 15, 25]:
                rows.append({
                    "account_id": acc,
                    "reporting_date": f"{m}-{day:02d}",
                    "dpd": dpd + random.randint(-5, 15),
                    "segment": "Retail" if int(acc[-1]) % 2 == 0 else "SME",
                })
            # DPD tends to increase over time for some accounts
            dpd = max(0, dpd + random.randint(-10, 20))

    df = pl.DataFrame(rows)
    node_id = engine.save_node_result(df.lazy(), "test_dpd_src")
    return node_id


# ── monthly_snapshot tests ────────────────────────────────────────────────────

def test_monthly_snapshot_max(engine):
    """Monthly snapshot with max aggregation should produce one row per (account, month)."""
    src = _make_dpd_data(engine)
    result_id = engine.monthly_snapshot(src, "account_id", "reporting_date", "dpd", "max")
    df = engine.get_node_data(result_id).collect()

    assert "year_month" in df.columns
    assert "account_id" in df.columns
    assert "dpd" in df.columns

    # 10 accounts × 6 months = 60 rows
    assert df.height == 60

    # Each (account, year_month) should be unique
    unique = df.select(["account_id", "year_month"]).unique()
    assert unique.height == 60


def test_monthly_snapshot_last(engine):
    """Monthly snapshot with 'last' should take the last observation by date."""
    src = _make_dpd_data(engine)
    result_id = engine.monthly_snapshot(src, "account_id", "reporting_date", "dpd", "last")
    df = engine.get_node_data(result_id).collect()

    assert df.height == 60


def test_monthly_snapshot_preserves_id(engine):
    """The id column should be preserved in the output."""
    src = _make_dpd_data(engine)
    result_id = engine.monthly_snapshot(src, "account_id", "reporting_date", "dpd", "max")
    df = engine.get_node_data(result_id).collect()

    accounts = df["account_id"].unique().sort().to_list()
    assert len(accounts) == 10
    assert accounts[0] == "ACC001"


# ── range_bucket + monthly_snapshot pipeline ──────────────────────────────────

def _make_bucketed_snapshot(engine):
    """Full pipeline: source → monthly_snapshot → range_bucket."""
    src = _make_dpd_data(engine)
    snap_id = engine.monthly_snapshot(src, "account_id", "reporting_date", "dpd", "max")
    bucket_id = engine.range_bucket(
        snap_id, "dpd",
        bins=[0, 30, 60, 90],
        labels=["0", "1-30", "31-60", "61-90", "90+"],
        new_col="dpd_bucket"
    )
    return bucket_id


def test_bucketed_snapshot(engine):
    """Bucketing after snapshot should produce valid bucket labels."""
    bid = _make_bucketed_snapshot(engine)
    df = engine.get_node_data(bid).collect()

    assert "dpd_bucket" in df.columns
    valid_buckets = {"0", "1-30", "31-60", "61-90", "90+"}
    actual = set(df["dpd_bucket"].unique().to_list())
    assert actual.issubset(valid_buckets)


# ── transition_matrix tests ───────────────────────────────────────────────────

def test_transition_matrix_basic(engine):
    """Transition matrix should produce from/to bucket pairs with counts and proportions."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(
        bid, "account_id", "year_month", "dpd_bucket",
        bucket_order=["0", "1-30", "31-60", "61-90", "90+"]
    )
    df = engine.get_node_data(tm_id).collect()

    assert set(df.columns) == {"period_from", "period_to", "from_bucket", "to_bucket", "count", "proportion"}
    assert df.height > 0

    # Proportions should be between 0 and 1
    assert df["proportion"].min() >= 0
    assert df["proportion"].max() <= 1.0


def test_transition_matrix_proportions_sum_to_one(engine):
    """For each (period_from, from_bucket), proportions should sum to ~1.0."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(
        bid, "account_id", "year_month", "dpd_bucket",
        bucket_order=["0", "1-30", "31-60", "61-90", "90+"]
    )
    df = engine.get_node_data(tm_id).collect()

    sums = df.group_by(["period_from", "from_bucket"]).agg(
        pl.col("proportion").sum().alias("total")
    )
    for row in sums.to_dicts():
        assert abs(row["total"] - 1.0) < 0.01, f"Proportions don't sum to 1 for {row}"


def test_transition_matrix_consecutive_periods(engine):
    """Period pairs should be consecutive months."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(
        bid, "account_id", "year_month", "dpd_bucket"
    )
    df = engine.get_node_data(tm_id).collect()

    pairs = df.select(["period_from", "period_to"]).unique().sort("period_from").to_dicts()
    for p in pairs:
        # period_from and period_to should be consecutive months
        assert p["period_from"] < p["period_to"]


# ── period_average_matrix tests ───────────────────────────────────────────────

def test_period_average_basic(engine):
    """Period average with window=3 on 5 transition pairs should produce 1 period."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(bid, "account_id", "year_month", "dpd_bucket")
    # 6 months → 5 transition pairs. window=3 → 1 full period
    pa_id = engine.period_average_matrix(tm_id, window=3)
    df = engine.get_node_data(pa_id).collect()

    assert "performance_period" in df.columns
    assert "from_bucket" in df.columns
    assert "to_bucket" in df.columns
    assert "avg_proportion" in df.columns
    assert df.height > 0

    periods = df["performance_period"].unique().to_list()
    assert "Year_1" in periods


def test_period_average_window_5(engine):
    """Window=5 on 5 pairs should produce exactly 1 period."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(bid, "account_id", "year_month", "dpd_bucket")
    pa_id = engine.period_average_matrix(tm_id, window=5)
    df = engine.get_node_data(pa_id).collect()

    periods = df["performance_period"].unique().to_list()
    assert len(periods) == 1
    assert periods[0] == "Year_1"


def test_period_average_proportions_valid(engine):
    """Averaged proportions should be between 0 and 1."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(bid, "account_id", "year_month", "dpd_bucket")
    pa_id = engine.period_average_matrix(tm_id, window=3)
    df = engine.get_node_data(pa_id).collect()

    assert df["avg_proportion"].min() >= 0
    assert df["avg_proportion"].max() <= 1.0


# ── chain_probability tests ──────────────────────────────────────────────────

def test_chain_probability_basic(engine):
    """Chain probability should produce prob_default and prob_cure per bucket per period."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(bid, "account_id", "year_month", "dpd_bucket")
    pa_id = engine.period_average_matrix(tm_id, window=5)
    cp_id = engine.chain_probability(pa_id)
    df = engine.get_node_data(cp_id).collect()

    assert set(df.columns) == {"performance_period", "from_bucket", "prob_default", "prob_cure"}
    assert df.height > 0


def test_chain_probability_boundary_values(engine):
    """Worst bucket should have prob_default=1, best bucket should have prob_cure=1."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(bid, "account_id", "year_month", "dpd_bucket")
    pa_id = engine.period_average_matrix(tm_id, window=5)
    cp_id = engine.chain_probability(pa_id)
    df = engine.get_node_data(cp_id).collect()

    # 90+ bucket: prob_default should be 1.0
    worst = df.filter(pl.col("from_bucket") == "90+")
    if worst.height > 0:
        assert worst["prob_default"][0] == 1.0

    # 0 bucket: prob_cure should be 1.0
    best = df.filter(pl.col("from_bucket") == "0")
    if best.height > 0:
        assert best["prob_cure"][0] == 1.0


def test_chain_probability_values_in_range(engine):
    """All probabilities should be between 0 and 1."""
    bid = _make_bucketed_snapshot(engine)
    tm_id = engine.transition_matrix(bid, "account_id", "year_month", "dpd_bucket")
    pa_id = engine.period_average_matrix(tm_id, window=5)
    cp_id = engine.chain_probability(pa_id)
    df = engine.get_node_data(cp_id).collect()

    assert df["prob_default"].min() >= 0
    assert df["prob_default"].max() <= 1.0
    assert df["prob_cure"].min() >= 0
    assert df["prob_cure"].max() <= 1.0


# ── Full pipeline test ────────────────────────────────────────────────────────

def test_full_roll_rate_pipeline(engine):
    """End-to-end: source → fill_missing → monthly_snapshot → range_bucket →
    transition_matrix → period_average → chain_probability."""
    src = _make_dpd_data(engine)

    # Fill missing DPD
    fill_id = engine.fill_missing(src, "dpd", 0)

    # Monthly snapshot
    snap_id = engine.monthly_snapshot(fill_id, "account_id", "reporting_date", "dpd", "max")

    # Bucket DPD
    bucket_id = engine.range_bucket(
        snap_id, "dpd",
        bins=[0, 30, 60, 90],
        labels=["0", "1-30", "31-60", "61-90", "90+"],
        new_col="dpd_bucket"
    )

    # Transition matrix
    tm_id = engine.transition_matrix(
        bucket_id, "account_id", "year_month", "dpd_bucket",
        bucket_order=["0", "1-30", "31-60", "61-90", "90+"]
    )

    # Period average (window=5 since we have 5 transition pairs from 6 months)
    pa_id = engine.period_average_matrix(tm_id, window=5)

    # Chain probability
    cp_id = engine.chain_probability(pa_id)
    df = engine.get_node_data(cp_id).collect()

    # Validate final output
    assert df.height == 5  # 5 buckets × 1 period
    assert set(df.columns) == {"performance_period", "from_bucket", "prob_default", "prob_cure"}

    # prob_default should decrease as we go from worst to best bucket
    # (90+ has highest prob_default = 1.0, 0 has lowest)
    worst = df.filter(pl.col("from_bucket") == "90+")["prob_default"][0]
    best = df.filter(pl.col("from_bucket") == "0")["prob_default"][0]
    assert worst >= best

    print("\n✅ Full roll-rate pipeline test passed!")
    print(df)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
