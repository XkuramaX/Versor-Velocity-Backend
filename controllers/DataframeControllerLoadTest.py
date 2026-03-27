import time
import numpy as np
from controllers.DataframeController import DataframeController
import polars as pl

def run_beast_load_test():
    controller = DataframeController()
    
    print("🚀 Generating 1,000,000 rows of synthetic data...")
    # Create 1M rows: ID, Amount, Category, and a random Score
    df = pl.DataFrame({
        "tx_id": np.arange(1_000_000),
        "amount": np.random.uniform(10, 5000, 1_000_000),
        "category": np.random.choice(["Retail", "Food", "Travel", "Tech"], 1_000_000),
        "risk_score": np.random.uniform(0, 1, 1_000_000)
    }).lazy()
    
    node_id = "big_data_source"
    controller.registry[node_id] = df

    print("⏱️ Starting complex pipeline execution...")
    start_time = time.time()

    # 1. Filter high-value transactions
    step1 = controller.filter_data(node_id, 'pl.col("amount") > 1000')
    
    # 2. Add a conditional 'Risk Level' column
    step2 = controller.conditional_column(step1, 'pl.col("risk_score") > 0.8', "High", "Low", "risk_level")
    
    # 3. Group by Category and sum the amounts
    step3 = controller.group_by_agg(step2, ["category"], {"amount": ["sum"]})
    print(step3)
    # 4. TRIGGER EXECUTION (This is where the real work happens)
    final_result = controller.registry[step3].collect(streaming=True)
    
    end_time = time.time()
    
    print("-" * 30)
    print(f"✅ Success! Processed 1M rows in: {end_time - start_time:.4f} seconds")
    print("Summary of Grouped Data:")
    print(final_result)

if __name__ == "__main__":
    run_beast_load_test()