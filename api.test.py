import requests
import time
import pandas as pd
import numpy as np
import os

os.environ['no_proxy'] = 'localhost,127.0.0.1'

BASE_URL = "http://localhost:8000"

def run_60_action_test():
    print("🏗️ Initializing 1M row, 50-column test environment...")
    # Setup data
    data_path = "heavy_load.csv"
    # pd.DataFrame({f"val_{i}": np.random.randn(1000000) for i in range(50)}).to_csv(data_path, index=False)

    # Start Test
    start_time = time.time()

    # Action 1: Ingest
    try:
        response = requests.post(f"{BASE_URL}/nodes/io/csv", params={"file_path": data_path})
        response.raise_for_status()
        current_node = response.json()["node_id"]
        print("current_node", current_node)
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to ingest data: {e}")
        return
    except KeyError as e:
        print(f"❌ Invalid response format: {e}")
        return

    # Actions 2-11: Bulk String Transformations & Type Casting
    # We rename columns and cast them to ensure the schema is heavy
    for i in range(20):
        try:
            response = requests.post(f"{BASE_URL}/nodes/transform/rename", 
                                   json={"mapping": {f"val_{i}": f"metric_{i}"}}, params={"parent_id": current_node})
            response.raise_for_status()
            current_node = response.json()["node_id"]
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to rename columns at iteration {i}: {e}")
            break
        
    # Actions 12-21: Multi-stage Filtering
    # We layer 10 different filters to test Predicate Pushdown
    for i in range(20):
        expr = f'pl.col("val_{i+10}") > -2.0'
        current_node = requests.post(f"{BASE_URL}/nodes/transform/filter", 
        params={"parent_id": current_node, "expression": expr}).json()["node_id"]

    # Actions 22-36: Horizontal Math (Matrix Logic)
    # Perform 15 different row-wise calculations
    for i in range(25):
        # print(current_node)
        cols = ["val_"+str(i), "val_"+str(i+1), "val_"+str(i+2)]
        current_node = requests.post(
            f"{BASE_URL}/nodes/math/horizontal", 
            params={"parent_id": current_node},  # <-- Query parameter
            json={"new_col": "sum_"+str(i), "columns": cols, "op": "sum"} # <-- JSON body
        ).json()["node_id"]
        # print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

    # Actions 37-46: Advanced Stats & Windowing
    # Rolling averages on the generated sums
    # for i in range(10):
    #     current_node = requests.post(f"{BASE_URL}/nodes/math/moving_average",
    #                                 params={"parent_id": current_node, "column": f"sum_{i}", "window": 5}).json()["node_id"]

    # Actions 47-56: String Logic & Literals
    # Simulate adding identifiers and categories
    # for i in range(10):
    #     current_node = requests.post(f"{BASE_URL}/nodes/string/prefix_suffix", 
    #                                 json={"parent_id": current_node, "column": "val_49", "prefix": f"GRP_{i}_"}).json()["node_id"]

    # Actions 57-60: The "Grand Final" Aggregations
    # Final grouping and multi-format export
    final_node = requests.post(f"{BASE_URL}/nodes/advanced/groupby", json={"parent_id": current_node, "group_cols": ["val_49_fixed"], 
                            "aggs": {"sum_0": ["sum", "mean"], "sum_9": ["max"]}}).json()["node_id"]

    print(f"🏁 60 Nodes Registered in {time.time() - start_time:.4f}s")

    # EXECUTION PHASE (Materializing 4 different output slices)
    print("📥 Materializing Results...")
    formats = ["csv", "parquet", "excel", "json"]
    for fmt in formats:
        m_start = time.time()
        try:
            res = requests.get(f"{BASE_URL}/nodes/{final_node}/download", params={"format": fmt})
            res.raise_for_status()
            with open(f"beast_60_output.{fmt}", "wb") as f:
                f.write(res.content)
            print(f"💾 {fmt.upper()} saved in {time.time() - m_start:.2f}s")
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to download {fmt.upper()}: {e}")
        except IOError as e:
            print(f"❌ Failed to save {fmt.upper()} file: {e}")

if __name__ == "__main__":
    run_60_action_test()
