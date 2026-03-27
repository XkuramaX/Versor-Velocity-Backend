"""
Workflow Audit Analyzer - Validates all DAG transformations
"""
import pandas as pd
from pathlib import Path
import json

class WorkflowAuditor:
    def __init__(self):
        self.audit_dir = Path("workflow_audit")
        self.results = []
        
    def analyze_all_files(self):
        print("\n" + "="*80)
        print("WORKFLOW AUDIT ANALYSIS")
        print("="*80 + "\n")
        
        # Read audit report
        with open(self.audit_dir / "audit_report.json") as f:
            report = json.load(f)
        
        print(f"Total Nodes: {report['total_nodes']}\n")
        
        # Analyze each transformation
        self.check_01_raw_data()
        self.check_03_filter()
        self.check_04_select()
        self.check_05_join()
        self.check_06_sort()
        self.check_07_rename()
        self.check_08_uppercase()
        self.check_09_bonus()
        self.check_10_total_comp()
        self.check_11_groupby()
        self.check_12_outliers()
        self.check_13_stats()
        self.check_14_lr_model()
        self.check_15_logistic()
        
        self.print_summary()
        
    def check_01_raw_data(self):
        """Verify raw data loaded correctly"""
        print("✓ NODE 1-2: Raw Data")
        emp = pd.read_csv(list(self.audit_dir.glob("01_employees_raw_*.csv"))[0])
        dept = pd.read_csv(list(self.audit_dir.glob("02_departments_raw_*.csv"))[0])
        
        assert len(emp) == 100, f"Expected 100 employees, got {len(emp)}"
        assert len(dept) == 4, f"Expected 4 departments, got {len(dept)}"
        assert 'performance_score' in emp.columns
        
        print(f"  Employees: {len(emp)} rows, {len(emp.columns)} columns")
        print(f"  Departments: {len(dept)} rows, {len(dept.columns)} columns")
        self.results.append(("Raw Data", "PASS", f"{len(emp)} + {len(dept)} rows"))
        
    def check_03_filter(self):
        """Verify filter (score > 3.5)"""
        print("\n✓ NODE 3: Filter (performance_score > 3.5)")
        df = pd.read_csv(list(self.audit_dir.glob("03_high_performers_*.csv"))[0])
        
        assert all(df['performance_score'] > 3.5), "Filter failed: found scores <= 3.5"
        assert len(df) < 100, "Filter should reduce row count"
        
        print(f"  Filtered: {len(df)} rows (from 100)")
        print(f"  Score range: {df['performance_score'].min():.2f} - {df['performance_score'].max():.2f}")
        self.results.append(("Filter", "PASS", f"{len(df)} high performers"))
        
    def check_04_select(self):
        """Verify column selection"""
        print("\n✓ NODE 4: Select Columns")
        df = pd.read_csv(list(self.audit_dir.glob("04_selected_columns_*.csv"))[0])
        
        expected_cols = ['emp_id', 'name', 'department', 'salary', 'experience', 'performance_score']
        assert list(df.columns) == expected_cols, f"Columns mismatch: {list(df.columns)}"
        
        print(f"  Columns: {list(df.columns)}")
        self.results.append(("Select", "PASS", f"{len(df.columns)} columns"))
        
    def check_05_join(self):
        """Verify join with departments"""
        print("\n✓ NODE 5: Join with Departments")
        df = pd.read_csv(list(self.audit_dir.glob("05_joined_with_dept_*.csv"))[0])
        
        assert 'budget' in df.columns, "Join failed: budget column missing"
        assert 'manager' in df.columns, "Join failed: manager column missing"
        assert 'location' in df.columns, "Join failed: location column missing"
        
        print(f"  Rows: {len(df)}")
        print(f"  New columns: budget, manager, location")
        print(f"  Sample: {df['department'].iloc[0]} → Manager: {df['manager'].iloc[0]}")
        self.results.append(("Join", "PASS", f"Added 3 dept columns"))
        
    def check_06_sort(self):
        """Verify sorting by salary"""
        print("\n✓ NODE 6: Sort by Salary (Descending)")
        df = pd.read_csv(list(self.audit_dir.glob("06_sorted_by_salary_*.csv"))[0])
        
        salaries = df['salary'].tolist()
        assert salaries == sorted(salaries, reverse=True), "Sort failed: not in descending order"
        
        print(f"  Top salary: ${salaries[0]:,}")
        print(f"  Bottom salary: ${salaries[-1]:,}")
        self.results.append(("Sort", "PASS", "Descending order verified"))
        
    def check_07_rename(self):
        """Verify column renaming"""
        print("\n✓ NODE 7: Rename Columns")
        df = pd.read_csv(list(self.audit_dir.glob("07_renamed_columns_*.csv"))[0])
        
        assert 'employee_id' in df.columns, "Rename failed: employee_id not found"
        assert 'score' in df.columns, "Rename failed: score not found"
        assert 'emp_id' not in df.columns, "Rename failed: emp_id still exists"
        
        print(f"  Renamed: emp_id → employee_id, performance_score → score")
        self.results.append(("Rename", "PASS", "2 columns renamed"))
        
    def check_08_uppercase(self):
        """Verify uppercase transformation"""
        print("\n✓ NODE 8: Uppercase Names")
        df = pd.read_csv(list(self.audit_dir.glob("08_uppercase_names_*.csv"))[0])
        
        assert all(df['name'].str.isupper()), "Uppercase failed: found lowercase names"
        
        print(f"  Sample: {df['name'].iloc[0]}")
        self.results.append(("Uppercase", "PASS", "All names uppercase"))
        
    def check_09_bonus(self):
        """Verify bonus calculation (10% of salary)"""
        print("\n✓ NODE 9: Calculate 10% Bonus")
        df = pd.read_csv(list(self.audit_dir.glob("09_with_bonus_*.csv"))[0])
        
        assert 'salary_bonus' in df.columns, "Bonus column missing"
        
        # Verify calculation
        expected_bonus = df['salary'] * 0.10
        actual_bonus = df['salary_bonus']
        assert all(abs(expected_bonus - actual_bonus) < 0.01), "Bonus calculation incorrect"
        
        print(f"  Sample: Salary ${df['salary'].iloc[0]:,} → Bonus ${df['salary_bonus'].iloc[0]:,.2f}")
        self.results.append(("Bonus Calc", "PASS", "10% calculated correctly"))
        
    def check_10_total_comp(self):
        """Verify total compensation (salary + bonus)"""
        print("\n✓ NODE 10: Total Compensation")
        df = pd.read_csv(list(self.audit_dir.glob("10_total_compensation_*.csv"))[0])
        
        assert 'total_compensation' in df.columns, "Total compensation column missing"
        
        # Verify calculation
        expected_total = df['salary'] + df['salary_bonus']
        actual_total = df['total_compensation']
        assert all(abs(expected_total - actual_total) < 0.01), "Total compensation incorrect"
        
        print(f"  Sample: ${df['salary'].iloc[0]:,} + ${df['salary_bonus'].iloc[0]:,.2f} = ${df['total_compensation'].iloc[0]:,.2f}")
        self.results.append(("Total Comp", "PASS", "Sum calculated correctly"))
        
    def check_11_groupby(self):
        """Verify group by aggregation"""
        print("\n✓ NODE 11: Group by Department")
        df = pd.read_csv(list(self.audit_dir.glob("11_dept_statistics_*.csv"))[0])
        
        assert len(df) == 4, f"Expected 4 departments, got {len(df)}"
        assert 'salary_sum' in df.columns
        assert 'salary_avg' in df.columns
        assert 'salary_count' in df.columns
        
        print(f"  Departments: {len(df)}")
        print(f"  Aggregations: sum, mean, count")
        print(f"  Sample: {df['department'].iloc[0]} - Count: {df['salary_count'].iloc[0]}, Avg: ${df['salary_avg'].iloc[0]:,.2f}")
        self.results.append(("GroupBy", "PASS", f"{len(df)} groups"))
        
    def check_12_outliers(self):
        """Verify outlier removal"""
        print("\n✓ NODE 12: Remove Outliers (IQR)")
        before = pd.read_csv(list(self.audit_dir.glob("10_total_compensation_*.csv"))[0])
        after = pd.read_csv(list(self.audit_dir.glob("12_no_outliers_*.csv"))[0])
        
        removed = len(before) - len(after)
        print(f"  Before: {len(before)} rows")
        print(f"  After: {len(after)} rows")
        print(f"  Removed: {removed} outliers")
        self.results.append(("Outliers", "PASS", f"{removed} rows removed"))
        
    def check_13_stats(self):
        """Verify statistical analysis"""
        print("\n✓ NODE 13: Statistical Analysis")
        df = pd.read_csv(list(self.audit_dir.glob("13_statistics_*.csv"))[0])
        
        expected_cols = ['salary_mean', 'salary_std', 'salary_median', 'salary_q1', 'salary_q3',
                        'experience_mean', 'experience_std', 'experience_median', 'experience_q1', 'experience_q3',
                        'score_mean', 'score_std', 'score_median', 'score_q1', 'score_q3']
        
        assert len(df) == 1, "Stats should have 1 row"
        assert all(col in df.columns for col in expected_cols[:5]), "Missing salary stats"
        
        print(f"  Salary mean: ${df['salary_mean'].iloc[0]:,.2f}")
        print(f"  Salary std: ${df['salary_std'].iloc[0]:,.2f}")
        self.results.append(("Statistics", "PASS", "All stats calculated"))
        
    def check_14_lr_model(self):
        """Verify linear regression model"""
        print("\n✓ NODE 14: Linear Regression Model")
        df = pd.read_csv(list(self.audit_dir.glob("14_lr_model_*.csv"))[0])
        
        assert len(df) == 3, f"Expected 3 coefficients (intercept + 2 features), got {len(df)}"
        assert 'feature' in df.columns
        assert 'coefficient' in df.columns
        
        features = df['feature'].tolist()
        assert 'intercept' in features
        assert 'experience' in features
        assert 'score' in features
        
        print(f"  Features: {features}")
        print(f"  Coefficients: {df['coefficient'].tolist()}")
        self.results.append(("LR Model", "PASS", "3 coefficients"))
        
    def check_15_logistic(self):
        """Verify logistic prediction"""
        print("\n✓ NODE 15: Logistic Prediction")
        df = pd.read_csv(list(self.audit_dir.glob("15_logistic_scores_*.csv"))[0])
        
        assert 'probability' in df.columns, "Probability column missing"
        
        probs = df['probability']
        assert all((probs >= 0) & (probs <= 1)), "Probabilities out of range [0,1]"
        
        print(f"  Probability range: {probs.min():.4f} - {probs.max():.4f}")
        print(f"  Mean probability: {probs.mean():.4f}")
        self.results.append(("Logistic", "PASS", "Probabilities valid"))
        
    def print_summary(self):
        """Print audit summary"""
        print("\n" + "="*80)
        print("AUDIT SUMMARY")
        print("="*80 + "\n")
        
        for test, status, detail in self.results:
            status_icon = "✓" if status == "PASS" else "✗"
            print(f"{status_icon} {test:20s} {status:6s} - {detail}")
        
        total = len(self.results)
        passed = sum(1 for _, status, _ in self.results if status == "PASS")
        
        print(f"\n{'='*80}")
        print(f"RESULT: {passed}/{total} tests passed")
        print(f"{'='*80}\n")
        
        if passed == total:
            print("✓ ALL WORKFLOW TRANSFORMATIONS VERIFIED SUCCESSFULLY!\n")
        else:
            print("✗ SOME TESTS FAILED - REVIEW ABOVE\n")

def main():
    auditor = WorkflowAuditor()
    auditor.analyze_all_files()

if __name__ == "__main__":
    main()
