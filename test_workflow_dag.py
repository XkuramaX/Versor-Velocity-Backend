"""
Workflow DAG Test - Direct Controller Testing
Tests complete workflow with 100 rows, saves all intermediate results
"""
import sys
sys.path.append('.')

from controllers.DataframeController import DataframeController
from controllers.WorkflowManager import WorkflowManager
import pandas as pd
import numpy as np
from pathlib import Path
import json

class WorkflowDAGTest:
    def __init__(self):
        self.controller = DataframeController()
        self.workflow = WorkflowManager(self.controller)
        self.output_dir = Path("workflow_audit")
        self.output_dir.mkdir(exist_ok=True)
        self.test_dir = Path("test_data")
        self.test_dir.mkdir(exist_ok=True)
        
    def create_test_data(self):
        """Create 100-row test datasets"""
        np.random.seed(42)
        
        # Employees dataset
        employees = pd.DataFrame({
            'emp_id': range(1, 101),
            'name': [f'Employee_{i}' for i in range(1, 101)],
            'department': np.random.choice(['Engineering', 'Sales', 'HR', 'Marketing'], 100),
            'salary': np.random.randint(40000, 120000, 100),
            'experience': np.random.randint(0, 20, 100),
            'age': np.random.randint(22, 65, 100),
            'performance_score': np.random.uniform(1.0, 5.0, 100).round(2)
        })
        employees.to_csv(self.test_dir / "employees.csv", index=False)
        
        # Departments dataset for join
        departments = pd.DataFrame({
            'department': ['Engineering', 'Sales', 'HR', 'Marketing'],
            'budget': [500000, 300000, 200000, 250000],
            'manager': ['Sarah Tech', 'Mike Sales', 'Linda HR', 'Tom Marketing'],
            'location': ['Building A', 'Building B', 'Building C', 'Building B']
        })
        departments.to_csv(self.test_dir / "departments.csv", index=False)
        
        print("✓ Created test data: 100 employees, 4 departments")
        
    def save_node_data(self, node_id, label):
        """Save node data to CSV for manual review"""
        try:
            df = self.controller.registry[node_id].collect()
            filepath = self.output_dir / f"{label}_{node_id}.csv"
            df.write_csv(filepath)
            print(f"  → Saved: {filepath} ({len(df)} rows, {len(df.columns)} cols)")
            return filepath
        except Exception as e:
            print(f"  ✗ Error saving {label}: {e}")
            return None
    
    def execute_workflow_dag(self):
        """Execute complete workflow DAG"""
        print("\n" + "="*70)
        print("WORKFLOW DAG EXECUTION TEST")
        print("="*70 + "\n")
        
        nodes = {}
        
        # NODE 1: Load employees CSV directly
        print("NODE 1: Load Employees CSV")
        import polars as pl
        nodes['emp'] = f"source_{hash(str(self.test_dir / 'employees.csv')) % 1000000}"
        self.controller.registry[nodes['emp']] = pl.scan_csv(str(self.test_dir / "employees.csv"))
        self.save_node_data(nodes['emp'], "01_employees_raw")
        
        # NODE 2: Load departments CSV directly
        print("\nNODE 2: Load Departments CSV")
        nodes['dept'] = f"source_{hash(str(self.test_dir / 'departments.csv')) % 1000000}"
        self.controller.registry[nodes['dept']] = pl.scan_csv(str(self.test_dir / "departments.csv"))
        self.save_node_data(nodes['dept'], "02_departments_raw")
        
        # NODE 3: Filter high performers (score > 3.5)
        print("\nNODE 3: Filter High Performers (score > 3.5)")
        nodes['high_perf'] = self.controller.filter_data(
            nodes['emp'],
            "pl.col('performance_score') > 3.5"
        )
        self.save_node_data(nodes['high_perf'], "03_high_performers")
        
        # NODE 4: Select specific columns
        print("\nNODE 4: Select Key Columns")
        nodes['selected'] = self.controller.select_columns(
            nodes['high_perf'],
            ['emp_id', 'name', 'department', 'salary', 'experience', 'performance_score']
        )
        self.save_node_data(nodes['selected'], "04_selected_columns")
        
        # NODE 5: Join with departments
        print("\nNODE 5: Join with Departments")
        nodes['joined'] = self.controller.join_nodes(
            nodes['selected'],
            nodes['dept'],
            on='department',
            how='left'
        )
        self.save_node_data(nodes['joined'], "05_joined_with_dept")
        
        # NODE 6: Sort by salary descending
        print("\nNODE 6: Sort by Salary (Descending)")
        nodes['sorted'] = self.controller.sort_data(
            nodes['joined'],
            by=['salary'],
            descending=True
        )
        self.save_node_data(nodes['sorted'], "06_sorted_by_salary")
        
        # NODE 7: Rename columns
        print("\nNODE 7: Rename Columns")
        nodes['renamed'] = self.controller.rename_columns(
            nodes['sorted'],
            mapping={'performance_score': 'score', 'emp_id': 'employee_id'}
        )
        self.save_node_data(nodes['renamed'], "07_renamed_columns")
        
        # NODE 8: String operation - uppercase names
        print("\nNODE 8: Uppercase Employee Names")
        nodes['upper'] = self.controller.str_to_upper(nodes['renamed'], 'name')
        self.save_node_data(nodes['upper'], "08_uppercase_names")
        
        # NODE 9: Math operation - 10% salary bonus
        print("\nNODE 9: Calculate 10% Salary Bonus")
        nodes['bonus'] = self.controller.apply_custom_expression(
            nodes['upper'],
            left_cols=['salary'],
            op='*',
            right_val=0.10,
            new_suffix='bonus'
        )
        self.save_node_data(nodes['bonus'], "09_with_bonus")
        
        # NODE 10: Horizontal sum - total compensation
        print("\nNODE 10: Calculate Total Compensation")
        nodes['total_comp'] = self.controller.horizontal_sum(
            nodes['bonus'],
            columns=['salary', 'salary_bonus'],
            new_col='total_compensation'
        )
        self.save_node_data(nodes['total_comp'], "10_total_compensation")
        
        # NODE 11: Group by department
        print("\nNODE 11: Group by Department Statistics")
        nodes['grouped'] = self.controller.group_by_agg(
            nodes['total_comp'],
            group_cols=['department'],
            aggs={
                'salary': ['sum', 'mean', 'count'],
                'total_compensation': ['sum', 'mean'],
                'score': ['mean']
            }
        )
        self.save_node_data(nodes['grouped'], "11_dept_statistics")
        
        # NODE 12: Filter outliers by salary
        print("\nNODE 12: Remove Salary Outliers (IQR method)")
        nodes['no_outliers'] = self.controller.filter_outliers_iqr(
            nodes['total_comp'],
            column='salary'
        )
        self.save_node_data(nodes['no_outliers'], "12_no_outliers")
        
        # NODE 13: Statistical analysis
        print("\nNODE 13: Statistical Analysis")
        nodes['stats'] = self.controller.col_stats_advanced(
            nodes['no_outliers'],
            columns=['salary', 'experience', 'score']
        )
        self.save_node_data(nodes['stats'], "13_statistics")
        
        # NODE 14: Linear regression model
        print("\nNODE 14: Linear Regression (Salary ~ Experience + Score)")
        nodes['lr_model'] = self.controller.linear_regression_node(
            nodes['no_outliers'],
            target='salary',
            features=['experience', 'score']
        )
        self.save_node_data(nodes['lr_model'], "14_lr_model")
        
        # NODE 15: Logistic prediction
        print("\nNODE 15: Logistic Prediction Scores")
        nodes['logistic'] = self.controller.logistic_regression_prediction(
            nodes['no_outliers'],
            features=['experience', 'score'],
            weights=[0.5, 0.3]
        )
        self.save_node_data(nodes['logistic'], "15_logistic_scores")
        
        print("\n" + "="*70)
        print(f"WORKFLOW COMPLETED: {len(nodes)} nodes executed")
        print("="*70 + "\n")
        
        return nodes
    
    def create_workflow_json(self, nodes):
        """Create workflow JSON representation"""
        workflow_json = {
            "workflow_id": "test_workflow_001",
            "name": "Complete DAG Test Workflow",
            "description": "Tests all major operations: upload, filter, join, math, string, aggregation, ML",
            "nodes": []
        }
        
        node_definitions = [
            {"id": "emp", "type": "upload_csv", "label": "Upload Employees", "parent": None},
            {"id": "dept", "type": "upload_csv", "label": "Upload Departments", "parent": None},
            {"id": "high_perf", "type": "filter", "label": "Filter High Performers", "parent": "emp"},
            {"id": "selected", "type": "select", "label": "Select Columns", "parent": "high_perf"},
            {"id": "joined", "type": "join", "label": "Join with Departments", "parent": "selected"},
            {"id": "sorted", "type": "sort", "label": "Sort by Salary", "parent": "joined"},
            {"id": "renamed", "type": "rename", "label": "Rename Columns", "parent": "sorted"},
            {"id": "upper", "type": "string_upper", "label": "Uppercase Names", "parent": "renamed"},
            {"id": "bonus", "type": "math_custom", "label": "Calculate Bonus", "parent": "upper"},
            {"id": "total_comp", "type": "math_sum", "label": "Total Compensation", "parent": "bonus"},
            {"id": "grouped", "type": "groupby", "label": "Department Stats", "parent": "total_comp"},
            {"id": "no_outliers", "type": "filter_outliers", "label": "Remove Outliers", "parent": "total_comp"},
            {"id": "stats", "type": "statistics", "label": "Statistical Analysis", "parent": "no_outliers"},
            {"id": "lr_model", "type": "linear_regression", "label": "LR Model", "parent": "no_outliers"},
            {"id": "logistic", "type": "logistic_prediction", "label": "Logistic Scores", "parent": "no_outliers"}
        ]
        
        for node_def in node_definitions:
            workflow_json["nodes"].append({
                "node_id": nodes.get(node_def["id"], "N/A"),
                "logical_id": node_def["id"],
                "type": node_def["type"],
                "label": node_def["label"],
                "parent": node_def["parent"]
            })
        
        # Save workflow JSON
        workflow_file = self.output_dir / "workflow_dag.json"
        with open(workflow_file, 'w') as f:
            json.dump(workflow_json, f, indent=2)
        print(f"✓ Workflow JSON saved: {workflow_file}")
        
        return workflow_json
    
    def generate_audit_report(self, nodes):
        """Generate comprehensive audit report"""
        report = {
            "test_name": "Complete Workflow DAG Test",
            "total_nodes": len(nodes),
            "nodes": {}
        }
        
        for label, node_id in nodes.items():
            try:
                df = self.controller.registry[node_id].collect()
                report["nodes"][label] = {
                    "node_id": node_id,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": df.columns,
                    "schema": {col: str(dtype) for col, dtype in df.schema.items()}
                }
            except Exception as e:
                report["nodes"][label] = {"error": str(e)}
        
        report_file = self.output_dir / "audit_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"✓ Audit report saved: {report_file}")
        
        return report
    
    def print_summary(self, nodes):
        """Print execution summary"""
        print("\n" + "="*70)
        print("EXECUTION SUMMARY")
        print("="*70)
        print(f"\nTotal Nodes Executed: {len(nodes)}")
        print(f"Output Directory: {self.output_dir}")
        print("\nNode List:")
        for i, (label, node_id) in enumerate(nodes.items(), 1):
            print(f"  {i:2d}. {label:20s} → {node_id}")
        print("\n" + "="*70 + "\n")

def main():
    test = WorkflowDAGTest()
    
    # Create test data
    test.create_test_data()
    
    # Execute workflow
    nodes = test.execute_workflow_dag()
    
    # Create workflow JSON
    workflow_json = test.create_workflow_json(nodes)
    
    # Generate audit report
    audit_report = test.generate_audit_report(nodes)
    
    # Print summary
    test.print_summary(nodes)
    
    print("✓ TEST COMPLETE")
    print(f"✓ Review files in: {test.output_dir}/")
    print("✓ All intermediate results saved as CSV files")
    print("✓ Workflow DAG saved as JSON")
    print("✓ Audit report generated\n")

if __name__ == "__main__":
    main()
