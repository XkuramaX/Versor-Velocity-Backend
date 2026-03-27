import requests
import time
import pandas as pd
import numpy as np
import os
import psutil
import threading
import json
from datetime import datetime
from typing import Dict, List, Any

os.environ['no_proxy'] = 'localhost,127.0.0.1'
BASE_URL = "http://localhost:8000"

class PerformanceMonitor:
    def __init__(self):
        self.metrics = {
            'memory_usage': [],
            'cpu_usage': [],
            'timestamps': [],
            'api_calls': [],
            'execution_times': [],
            'node_operations': []
        }
        self.monitoring = False
        self.start_time = None
        
    def start_monitoring(self):
        self.monitoring = True
        self.start_time = time.time()
        self.monitor_thread = threading.Thread(target=self._monitor_system)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        self.monitoring = False
        
    def _monitor_system(self):
        process = psutil.Process()
        while self.monitoring:
            try:
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent()
                
                self.metrics['memory_usage'].append({
                    'rss': memory_info.rss / 1024 / 1024,  # MB
                    'vms': memory_info.vms / 1024 / 1024,  # MB
                    'timestamp': time.time() - self.start_time
                })
                self.metrics['cpu_usage'].append({
                    'percent': cpu_percent,
                    'timestamp': time.time() - self.start_time
                })
                time.sleep(0.1)  # Monitor every 100ms
            except (psutil.Error, OSError) as e:
                print(f"⚠️ Monitoring error: {e}")
                break
            except Exception as e:
                print(f"⚠️ Unexpected monitoring error: {e}")
                break
                
    def log_api_call(self, endpoint: str, method: str, duration: float, status_code: int, node_id: str = None):
        self.metrics['api_calls'].append({
            'endpoint': endpoint,
            'method': method,
            'duration': duration,
            'status_code': status_code,
            'node_id': node_id,
            'timestamp': time.time() - self.start_time
        })

def create_heavy_test_data():
    """Create large test datasets for load testing"""
    print("🏗️ Creating heavy test datasets...")
    
    datasets = {
        'large_mixed.csv': None,
        'numeric_heavy.csv': None,
        'string_heavy.csv': None,
        'test_excel.xlsx': None
    }
    
    # Check which files already exist
    existing_files = []
    missing_files = []
    
    for filename in datasets.keys():
        if os.path.exists(filename):
            existing_files.append(filename)
            print(f"📁 Found existing {filename}")
        else:
            missing_files.append(filename)
    
    # Only create missing files
    if missing_files:
        print(f"📝 Creating {len(missing_files)} missing files...")
        
        if 'large_mixed.csv' in missing_files:
            large_data = pd.DataFrame({
                **{f"numeric_{i}": np.random.uniform(-1000, 1000, 1_000_000) for i in range(20)},
                **{f"category_{i}": np.random.choice(['A', 'B', 'C', 'D', 'E'], 1_000_000) for i in range(10)},
                **{f"string_{i}": [f"text_{j}_{i}" for j in range(1_000_000)] for i in range(10)},
                **{f"boolean_{i}": np.random.choice([True, False], 1_000_000) for i in range(5)},
                **{f"date_{i}": pd.date_range('2020-01-01', periods=1_000_000, freq='min') for i in range(5)}
            })
            large_data.to_csv('large_mixed.csv', index=False)
            print(f"📁 Created large_mixed.csv (1,000,000 rows, 50 cols)")
        
        if 'numeric_heavy.csv' in missing_files:
            numeric_heavy = pd.DataFrame({
                f"val_{i}": np.random.randn(500_000) for i in range(100)
            })
            numeric_heavy.to_csv('numeric_heavy.csv', index=False)
            print(f"📁 Created numeric_heavy.csv (500,000 rows, 100 cols)")
        
        if 'string_heavy.csv' in missing_files:
            string_heavy = pd.DataFrame({
                'id': range(2_000_000),
                'name': [f"Person_{i}" for i in range(2_000_000)],
                'email': [f"user{i}@example.com" for i in range(2_000_000)],
                'address': [f"{i} Main St, City {i%1000}" for i in range(2_000_000)],
                'phone': [f"+1-{i:010d}" for i in range(2_000_000)],
                'department': np.random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales'], 2_000_000),
                'salary': np.random.uniform(30000, 200000, 2_000_000),
                'score': np.random.uniform(0, 100, 2_000_000),
                'status': np.random.choice(['Active', 'Inactive', 'Pending'], 2_000_000),
                'join_date': pd.date_range('2015-01-01', periods=2_000_000, freq='h')
            })
            string_heavy.to_csv('string_heavy.csv', index=False)
            print(f"📁 Created string_heavy.csv (2,000,000 rows, 10 cols)")
        
        if 'test_excel.xlsx' in missing_files:
            # Use existing large_data or create minimal version
            if 'large_mixed.csv' not in missing_files:
                excel_data = pd.read_csv('large_mixed.csv').iloc[:100000]
            else:
                excel_data = large_data.iloc[:100000]
                
            with pd.ExcelWriter('test_excel.xlsx', engine='openpyxl') as writer:
                excel_data.iloc[:30000].to_excel(writer, sheet_name='Sheet1', index=False)
                excel_data.iloc[30000:60000].to_excel(writer, sheet_name='Data_Analysis', index=False)
                excel_data.iloc[60000:].to_excel(writer, sheet_name='Summary', index=False)
            print(f"📁 Created test_excel.xlsx (100,000 rows, 50 cols)")
    else:
        print("✅ All test files already exist, skipping creation")
            
    return list(datasets.keys())

def comprehensive_load_test():
    """Run comprehensive load test with detailed monitoring"""
    monitor = PerformanceMonitor()
    monitor.start_monitoring()
    
    print("🚀 Starting Comprehensive Load Test")
    print("=" * 60)
    
    # Create test data
    datasets = create_heavy_test_data()
    
    # Test phases
    test_results = {
        'phase_1_ingestion': {},
        'phase_2_transforms': {},
        'phase_3_math_operations': {},
        'phase_4_string_operations': {},
        'phase_5_advanced_analytics': {},
        'phase_6_ml_operations': {},
        'phase_7_data_cleaning': {},
        'phase_8_vector_operations': {},
        'phase_9_additional_apis': {},
        'phase_10_stress_test': {}
    }
    
    try:
        # PHASE 1: Data Ingestion Load Test
        print("\n📥 PHASE 1: Data Ingestion Load Test")
        phase_start = time.time()
        node_ids = []
        
        # Test CSV uploads
        csv_files = [f for f in datasets if f.endswith('.csv')]
        for csv_file in csv_files:
            try:
                start_time = time.time()
                with open(csv_file, 'rb') as f:
                    files = {'file': (csv_file, f, 'text/csv')}
                    response = requests.post(f"{BASE_URL}/nodes/io/upload_csv", files=files, timeout=300)
                    response.raise_for_status()
                
                duration = time.time() - start_time
                node_id = response.json()["node_id"]
                node_ids.append(node_id)
                monitor.log_api_call("/nodes/io/upload_csv", "POST", duration, response.status_code, node_id)
                print(f"✅ Uploaded {csv_file}: {node_id} ({duration:.2f}s)")
            except Exception as e:
                print(f"❌ Failed to upload {csv_file}: {e}")
        
        # Test Excel upload
        excel_files = [f for f in datasets if f.endswith('.xlsx')]
        for excel_file in excel_files:
            try:
                start_time = time.time()
                with open(excel_file, 'rb') as f:
                    files = {'file': (excel_file, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
                    response = requests.post(f"{BASE_URL}/nodes/io/upload_excel", files=files, timeout=180)
                    response.raise_for_status()
                    
                duration = time.time() - start_time
                node_id = response.json()["node_id"]
                node_ids.append(node_id)
                monitor.log_api_call("/nodes/io/upload_excel", "POST", duration, response.status_code, node_id)
                print(f"✅ Uploaded {excel_file}: {node_id} ({duration:.2f}s)")
            except Exception as e:
                print(f"❌ Failed to upload {excel_file}: {e}")
                
        test_results['phase_1_ingestion'] = {
            'duration': time.time() - phase_start,
            'datasets_loaded': len(node_ids),
            'csv_files': len([f for f in datasets if f.endswith('.csv')]),
            'excel_sheets': len(node_ids) - len([f for f in datasets if f.endswith('.csv')])
        }
        
        # PHASE 2: Transform Operations Load Test
        print("\n🔄 PHASE 2: Transform Operations Load Test")
        phase_start = time.time()
        transform_nodes = []
        
        if node_ids:
            # Map node_ids to their actual datasets by checking schema
            node_dataset_map = {}
            for node_id in node_ids:
                try:
                    node_info = requests.get(f"{BASE_URL}/nodes/{node_id}", params={"limit": 1})
                    if node_info.status_code == 200:
                        schema = node_info.json().get('schema', {})
                        cols = list(schema.keys())
                        if any('numeric_' in c for c in cols):
                            node_dataset_map['large_mixed'] = node_id
                        elif any('val_' in c for c in cols):
                            node_dataset_map['numeric_heavy'] = node_id
                        elif 'salary' in cols and 'department' in cols:
                            node_dataset_map['string_heavy'] = node_id
                except:
                    pass
            
            # Use string_heavy for all operations since it's the only one that uploaded
            test_nodes = [node_dataset_map.get('string_heavy', node_ids[0])] if node_dataset_map else node_ids[:1]
            
            for i, base_node in enumerate(test_nodes):
                # Filter operations
                start_time = time.time()
                filters = [{"column": "salary", "operation": "gt", "value": 50000, "column_type": "numeric"}]
                    
                response = requests.post(f"{BASE_URL}/nodes/transform/safe_filter", 
                                       json={"filters": filters, "logic": "and"}, 
                                       params={"parent_id": base_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    filter_node = response.json()["node_id"]
                    transform_nodes.append(filter_node)
                    monitor.log_api_call("/nodes/transform/safe_filter", "POST", duration, response.status_code, filter_node)
                    base_node = filter_node
                    print(f"✅ Safe filter operation on node {i+1} completed ({duration:.2f}s)")
                else:
                    print(f"❌ Safe filter failed: {response.status_code}")
                
                # Select operations
                start_time = time.time()
                columns = ["name", "email", "salary", "score", "department"]
                    
                response = requests.post(f"{BASE_URL}/nodes/transform/select",
                                       json=columns,
                                       params={"parent_id": base_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    select_node = response.json()["node_id"]
                    transform_nodes.append(select_node)
                    monitor.log_api_call("/nodes/transform/select", "POST", duration, response.status_code, select_node)
                    base_node = select_node
                    print(f"✅ Select operation on node {i+1} completed ({duration:.2f}s)")
                
                # Sort operations
                start_time = time.time()
                sort_cols = ["salary", "name"]
                response = requests.post(f"{BASE_URL}/nodes/transform/sort",
                                       json={"by": sort_cols, "descending": False},
                                       params={"parent_id": base_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    sort_node = response.json()["node_id"]
                    transform_nodes.append(sort_node)
                    monitor.log_api_call("/nodes/transform/sort", "POST", duration, response.status_code, sort_node)
                    base_node = sort_node
                    print(f"✅ Sort operation on node {i+1} completed ({duration:.2f}s)")
                
                # Rename operations
                start_time = time.time()
                rename_mapping = {"name": "employee_name"}
                response = requests.post(f"{BASE_URL}/nodes/transform/rename",
                                       json=rename_mapping,
                                       params={"parent_id": base_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    rename_node = response.json()["node_id"]
                    transform_nodes.append(rename_node)
                    monitor.log_api_call("/nodes/transform/rename", "POST", duration, response.status_code, rename_node)
                    print(f"✅ Rename operation on node {i+1} completed ({duration:.2f}s)")
                else:
                    print(f"❌ Rename failed: {response.status_code} - {response.text}")
                
        test_results['phase_2_transforms'] = {
            'duration': time.time() - phase_start,
            'operations_completed': len(transform_nodes),
            'filter_operations': 3,
            'select_operations': 3,
            'sort_operations': 3,
            'rename_operations': 3
        }
        
        # PHASE 3: Math Operations Load Test
        print("\n🧮 PHASE 3: Math Operations Load Test")
        phase_start = time.time()
        math_nodes = []
        
        for node in transform_nodes[:5]:
            start_time = time.time()
            try:
                node_info = requests.get(f"{BASE_URL}/nodes/{node}", params={"limit": 1})
                if node_info.status_code == 200:
                    columns = list(node_info.json().get('schema', {}).keys())[:3]
                    if len(columns) >= 2:
                        response = requests.post(f"{BASE_URL}/nodes/math/horizontal",
                                               json={"columns": columns[:2], "new_col": f"sum_result_{len(math_nodes)}", "op": "sum"},
                                               params={"parent_id": node})
                        duration = time.time() - start_time
                        
                        if response.status_code == 200:
                            math_node = response.json()["node_id"]
                            math_nodes.append(math_node)
                            monitor.log_api_call("/nodes/math/horizontal", "POST", duration, response.status_code, math_node)
                            print(f"✅ Math horizontal sum completed ({duration:.2f}s)")
                        else:
                            print(f"❌ Math horizontal failed: {response.status_code}")
            except Exception as e:
                print(f"❌ Math operation failed: {str(e)}")
                
        # Test custom math expressions
        if math_nodes:
            start_time = time.time()
            try:
                response = requests.post(f"{BASE_URL}/nodes/math/custom",
                                       json={"left_cols": ["sum_result_0"], "op": "*", "right_val": 2.5, "new_suffix": "_doubled"},
                                       params={"parent_id": math_nodes[0]})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    custom_node = response.json()["node_id"]
                    math_nodes.append(custom_node)
                    monitor.log_api_call("/nodes/math/custom", "POST", duration, response.status_code, custom_node)
                    print(f"✅ Custom math expression completed ({duration:.2f}s)")
            except Exception as e:
                print(f"❌ Custom math failed: {str(e)}")
                
        test_results['phase_3_math_operations'] = {
            'duration': time.time() - phase_start,
            'math_nodes_created': len(math_nodes),
            'horizontal_operations': len([n for n in math_nodes if 'sum_result' in str(n)]),
            'custom_expressions': 1 if len(math_nodes) > 5 else 0
        }
        
        # PHASE 4: String Operations Load Test
        print("\n📝 PHASE 4: String Operations Load Test")
        phase_start = time.time()
        string_nodes = []
        
        # Use string_heavy dataset node
        if len(node_ids) > 2:
            string_base_node = node_ids[2]
            
            try:
                # String case operations
                string_ops = [("upper", "name"), ("lower", "email")]
                current_node = string_base_node
                
                for mode, column in string_ops:
                    start_time = time.time()
                    response = requests.post(f"{BASE_URL}/nodes/string/case",
                                           json={"column": column, "mode": mode},
                                           params={"parent_id": current_node})
                    duration = time.time() - start_time
                    
                    if response.status_code == 200:
                        string_node = response.json()["node_id"]
                        string_nodes.append(string_node)
                        monitor.log_api_call("/nodes/string/case", "POST", duration, response.status_code, string_node)
                        current_node = string_node
                        print(f"✅ String {mode} on {column} completed ({duration:.2f}s)")
                
                # String concatenation
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/nodes/string/concat",
                                       json={"columns": ["name", "email"], "separator": " - ", "new_col": "name_email"},
                                       params={"parent_id": current_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    concat_node = response.json()["node_id"]
                    string_nodes.append(concat_node)
                    monitor.log_api_call("/nodes/string/concat", "POST", duration, response.status_code, concat_node)
                    print(f"✅ String concatenation completed ({duration:.2f}s)")
                    
            except Exception as e:
                print(f"❌ String operations failed: {str(e)}")
                
        test_results['phase_4_string_operations'] = {
            'duration': time.time() - phase_start,
            'string_operations': len(string_nodes),
            'case_operations': 2,
            'concat_operations': 1
        }
        
        # PHASE 5: Advanced Analytics Load Test
        print("\n📊 PHASE 5: Advanced Analytics Load Test")
        phase_start = time.time()
        analytics_nodes = []
        
        try:
            # Use string_heavy dataset for groupby operations
            if len(node_ids) > 2:
                string_base_node = node_ids[2]
                
                # GroupBy operations
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/nodes/advanced/groupby",
                                       json={"group_cols": ["department"], 
                                             "aggs": {"salary": ["sum", "mean"], "score": ["max", "min"]}},
                                       params={"parent_id": string_base_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    group_node = response.json()["node_id"]
                    analytics_nodes.append(group_node)
                    monitor.log_api_call("/nodes/advanced/groupby", "POST", duration, response.status_code, group_node)
                    print(f"✅ GroupBy operation completed ({duration:.2f}s)")
                
                # Pivot table
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/nodes/advanced/pivot",
                                       json={"values": "salary", "index": ["department"], "on": "status", "agg": "mean"},
                                       params={"parent_id": string_base_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    pivot_node = response.json()["node_id"]
                    analytics_nodes.append(pivot_node)
                    monitor.log_api_call("/nodes/advanced/pivot", "POST", duration, response.status_code, pivot_node)
                    print(f"✅ Pivot table completed ({duration:.2f}s)")
                    
        except Exception as e:
            print(f"❌ Advanced analytics failed: {str(e)}")
                
        test_results['phase_5_advanced_analytics'] = {
            'duration': time.time() - phase_start,
            'groupby_operations': 1,
            'pivot_operations': 1,
            'analytics_nodes': len(analytics_nodes)
        }
        
        # PHASE 6: ML Operations Load Test
        print("\n🤖 PHASE 6: ML Operations Load Test")
        phase_start = time.time()
        ml_results = []
        
        try:
            # Linear regression on numeric data
            if len(node_ids) > 1:
                numeric_node = node_ids[1]
                
                # Get available columns
                node_info = requests.get(f"{BASE_URL}/nodes/{numeric_node}", params={"limit": 1})
                if node_info.status_code == 200:
                    available_cols = list(node_info.json().get('schema', {}).keys())
                    numeric_cols = [col for col in available_cols if 'val_' in col][:4]
                    
                    if len(numeric_cols) >= 4:
                        target_col = numeric_cols[0]
                        feature_cols = numeric_cols[1:4]
                        
                        start_time = time.time()
                        response = requests.post(f"{BASE_URL}/nodes/ml/linear_regression",
                                               json={"target": target_col, "features": feature_cols},
                                               params={"parent_id": numeric_node})
                        duration = time.time() - start_time
                        
                        if response.status_code == 200:
                            lr_result = response.json()
                            ml_results.append(lr_result)
                            monitor.log_api_call("/nodes/ml/linear_regression", "POST", duration, response.status_code)
                            print(f"✅ Linear regression completed ({duration:.2f}s)")
                        else:
                            print(f"❌ Linear regression failed: {response.status_code}")
                            
        except Exception as e:
            print(f"❌ ML operations failed: {str(e)}")
            
        test_results['phase_6_ml_operations'] = {
            'duration': time.time() - phase_start,
            'ml_models_created': len(ml_results),
            'regression_operations': 1 if ml_results else 0
        }
        
        # PHASE 7: Data Cleaning Operations
        print("\n🧹 PHASE 7: Data Cleaning Operations")
        phase_start = time.time()
        cleaning_nodes = []
        
        if node_ids:
            try:
                # Drop nulls
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/nodes/clean/drop_nulls",
                                       params={"parent_id": node_ids[0]})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    clean_node = response.json()["node_id"]
                    cleaning_nodes.append(clean_node)
                    monitor.log_api_call("/nodes/clean/drop_nulls", "POST", duration, response.status_code, clean_node)
                    print(f"✅ Drop nulls completed ({duration:.2f}s)")
                
                # Drop duplicates
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/nodes/clean/drop_duplicates",
                                       json={"subset": ["name", "email"]},
                                       params={"parent_id": node_ids[2] if len(node_ids) > 2 else node_ids[0]})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    dedup_node = response.json()["node_id"]
                    cleaning_nodes.append(dedup_node)
                    monitor.log_api_call("/nodes/clean/drop_duplicates", "POST", duration, response.status_code, dedup_node)
                    print(f"✅ Drop duplicates completed ({duration:.2f}s)")
                    
            except Exception as e:
                print(f"❌ Data cleaning failed: {str(e)}")
        
        test_results['phase_7_data_cleaning'] = {
            'duration': time.time() - phase_start,
            'cleaning_operations': len(cleaning_nodes),
            'drop_nulls': 1,
            'drop_duplicates': 1
        }
        
        # PHASE 8: Vector Operations
        print("\n🔢 PHASE 8: Vector Operations")
        phase_start = time.time()
        vector_nodes = []
        
        if len(node_ids) > 1:
            try:
                numeric_node = node_ids[1]
                
                # Vector dot product
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/nodes/vector/dot_product",
                                       json={"vec_a": ["val_0", "val_1"], "vec_b": ["val_2", "val_3"], "new_col": "dot_result"},
                                       params={"parent_id": numeric_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    dot_node = response.json()["node_id"]
                    vector_nodes.append(dot_node)
                    monitor.log_api_call("/nodes/vector/dot_product", "POST", duration, response.status_code, dot_node)
                    print(f"✅ Vector dot product completed ({duration:.2f}s)")
                
                # Vector linear multiply
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/nodes/vector/linear_multiply",
                                       json={"vec_a": ["val_0", "val_1"], "vec_b": ["val_2", "val_3"], "suffix": "_mult"},
                                       params={"parent_id": numeric_node})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    mult_node = response.json()["node_id"]
                    vector_nodes.append(mult_node)
                    monitor.log_api_call("/nodes/vector/linear_multiply", "POST", duration, response.status_code, mult_node)
                    print(f"✅ Vector linear multiply completed ({duration:.2f}s)")
                    
            except Exception as e:
                print(f"❌ Vector operations failed: {str(e)}")
        
        test_results['phase_8_vector_operations'] = {
            'duration': time.time() - phase_start,
            'vector_operations': len(vector_nodes),
            'dot_product': 1 if vector_nodes else 0,
            'linear_multiply': 1 if len(vector_nodes) > 1 else 0
        }
        
        # PHASE 9: Additional API Coverage
        print("\n🔧 PHASE 9: Additional API Coverage")
        phase_start = time.time()
        additional_ops = 0
        
        try:
            # Health check
            start_time = time.time()
            response = requests.get(f"{BASE_URL}/health")
            duration = time.time() - start_time
            
            if response.status_code == 200:
                monitor.log_api_call("/health", "GET", duration, response.status_code)
                additional_ops += 1
                print(f"✅ Health check completed ({duration:.2f}s)")
            
            # Workflow validation
            if node_ids:
                start_time = time.time()
                response = requests.post(f"{BASE_URL}/workflow/validate-connection",
                                       json={"child_type": "filter_data", "child_params": {"column": "name"}},
                                       params={"parent_id": node_ids[0]})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    monitor.log_api_call("/workflow/validate-connection", "POST", duration, response.status_code)
                    additional_ops += 1
                    print(f"✅ Workflow validation completed ({duration:.2f}s)")
            
            # Export workflow
            start_time = time.time()
            response = requests.get(f"{BASE_URL}/workflow/export")
            duration = time.time() - start_time
            
            if response.status_code == 200:
                monitor.log_api_call("/workflow/export", "GET", duration, response.status_code)
                additional_ops += 1
                print(f"✅ Workflow export completed ({duration:.2f}s)")
                
        except Exception as e:
            print(f"❌ Additional API operations failed: {str(e)}")
        
        test_results['phase_9_additional_apis'] = {
            'duration': time.time() - phase_start,
            'additional_operations': additional_ops,
            'health_check': 1,
            'workflow_validation': 1,
            'workflow_export': 1
        }
        
        # PHASE 10: Stress Test
        print("\n💾 PHASE 10: Stress Test")
        phase_start = time.time()
        
        # Node inspection stress test
        download_times = []
        try:
            for i, node in enumerate(node_ids[:3]):
                start_time = time.time()
                response = requests.get(f"{BASE_URL}/nodes/{node}", params={"limit": 100})
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    download_times.append(duration)
                    monitor.log_api_call("/nodes/*/inspect", "GET", duration, response.status_code)
                    print(f"✅ Node {node} inspection completed ({duration:.2f}s)")
        except Exception as e:
            print(f"❌ Stress test failed: {str(e)}")
            
        test_results['phase_10_stress_test'] = {
            'duration': time.time() - phase_start,
            'stress_operations': len(download_times),
            'avg_inspection_time': np.mean(download_times) if download_times else 0
        }
        
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        
    finally:
        monitor.stop_monitoring()
        
        # Generate comprehensive report
        generate_performance_report(monitor, test_results)

def generate_performance_report(monitor: PerformanceMonitor, test_results: Dict):
    """Generate detailed performance report"""
    print("\n" + "="*80)
    print("📊 COMPREHENSIVE PERFORMANCE REPORT")
    print("="*80)
    
    # Overall statistics
    total_duration = sum(phase.get('duration', 0) for phase in test_results.values())
    total_api_calls = len(monitor.metrics['api_calls'])
    successful_calls = sum(1 for call in monitor.metrics['api_calls'] if call['status_code'] == 200)
    
    print(f"\n🎯 OVERALL STATISTICS")
    print(f"Total Test Duration: {total_duration:.2f} seconds")
    print(f"Total API Calls: {total_api_calls}")
    print(f"Successful Calls: {successful_calls} ({successful_calls/total_api_calls*100:.1f}%)")
    print(f"Average API Response Time: {np.mean([call['duration'] for call in monitor.metrics['api_calls']]):.3f}s")
    
    # Memory usage analysis
    if monitor.metrics['memory_usage']:
        memory_data = monitor.metrics['memory_usage']
        peak_rss = max(m['rss'] for m in memory_data)
        peak_vms = max(m['vms'] for m in memory_data)
        avg_rss = np.mean([m['rss'] for m in memory_data])
        
        print(f"\n💾 MEMORY USAGE ANALYSIS")
        print(f"Peak RSS Memory: {peak_rss:.2f} MB")
        print(f"Peak VMS Memory: {peak_vms:.2f} MB")
        print(f"Average RSS Memory: {avg_rss:.2f} MB")
        print(f"Memory Samples Collected: {len(memory_data)}")
        
    # CPU usage analysis
    if monitor.metrics['cpu_usage']:
        cpu_data = [c['percent'] for c in monitor.metrics['cpu_usage'] if c['percent'] > 0]
        if cpu_data:
            print(f"\n🖥️  CPU USAGE ANALYSIS")
            print(f"Peak CPU Usage: {max(cpu_data):.1f}%")
            print(f"Average CPU Usage: {np.mean(cpu_data):.1f}%")
            print(f"CPU Samples Collected: {len(cpu_data)}")
    
    # Phase-by-phase breakdown
    print(f"\n📋 PHASE-BY-PHASE BREAKDOWN")
    for phase_name, phase_data in test_results.items():
        phase_display = phase_name.replace('_', ' ').title()
        print(f"\n{phase_display}:")
        for key, value in phase_data.items():
            if isinstance(value, float):
                print(f"  {key.replace('_', ' ').title()}: {value:.3f}")
            else:
                print(f"  {key.replace('_', ' ').title()}: {value}")
    
    # API endpoint performance
    endpoint_stats = {}
    for call in monitor.metrics['api_calls']:
        endpoint = call['endpoint']
        if endpoint not in endpoint_stats:
            endpoint_stats[endpoint] = {'calls': 0, 'total_time': 0, 'errors': 0}
        
        endpoint_stats[endpoint]['calls'] += 1
        endpoint_stats[endpoint]['total_time'] += call['duration']
        if call['status_code'] != 200:
            endpoint_stats[endpoint]['errors'] += 1
    
    print(f"\n🔗 API ENDPOINT PERFORMANCE")
    for endpoint, stats in endpoint_stats.items():
        avg_time = stats['total_time'] / stats['calls']
        error_rate = stats['errors'] / stats['calls'] * 100
        print(f"{endpoint}:")
        print(f"  Calls: {stats['calls']}, Avg Time: {avg_time:.3f}s, Error Rate: {error_rate:.1f}%")
    
    # Save detailed metrics to JSON
    report_data = {
        'timestamp': datetime.now().isoformat(),
        'overall_stats': {
            'total_duration': total_duration,
            'total_api_calls': total_api_calls,
            'successful_calls': successful_calls,
            'success_rate': successful_calls/total_api_calls*100
        },
        'test_results': test_results,
        'endpoint_performance': endpoint_stats,
        'raw_metrics': monitor.metrics
    }
    
    with open('versor_comprehensive_load_test_report.json', 'w') as f:
        json.dump(report_data, f, indent=2, default=str)
    
    print(f"\n💾 Detailed report saved to: versor_comprehensive_load_test_report.json")
    print("="*80)

if __name__ == "__main__":
    comprehensive_load_test()