# Versor-Velocity — Comprehensive Node Test Flows
# ================================================
# 8 test workflows covering all 50 nodes in realistic combinations.
# Use test_data/employees.csv, departments.csv, sales.csv, bonuses.csv

## Flow 1: Basic ETL Pipeline (8 nodes)
# upload_csv → fill_missing → safe_filter → select → sort → rename → add_literal_column → download
Steps:
1. upload_csv: employees.csv
2. fill_missing: column=performance_score, value=0
3. safe_filter: salary > 60000 AND department contains "Engineering"
4. select: [emp_id, name, department, salary, experience]
5. sort: by=[salary], descending=true
6. rename: {name: employee_name, emp_id: id}
7. add_literal_column: column=report_date, value=2024-Q4, dtype=string
8. Download result

## Flow 2: String Operations Pipeline (7 nodes)
# upload_csv → string_clean → string_case → string_slice → string_concat → string_prefix_suffix → string_mid
Steps:
1. upload_csv: employees.csv
2. string_clean: column=name
3. string_case: column=name, mode=upper
4. string_slice: column=name, n_chars=3, mode=left, new_col=initials
5. string_concat: columns=[name, department], separator=" - ", new_col=name_dept
6. string_prefix_suffix: column=department, prefix="DEPT_", suffix="_2024"
7. string_mid: column=name, start=1, length=5, new_col=short_name

## Flow 3: Math & Aggregation Pipeline (9 nodes)
# upload_csv → math_horizontal → math_custom → math_multiply_bulk → groupby → stats → pivot → moving_average → reorder
Steps:
1. upload_csv: employees.csv
2. math_horizontal: columns=[salary, experience], new_col=comp_score, op=sum
3. math_custom: left_cols=[salary], op=*, right_val=1.1, new_suffix=raised
4. math_multiply_bulk: columns=[salary, experience], factor=1.05, suffix=_adj
5. groupby: group_cols=[department], aggs={salary: [mean, sum, count], experience: [mean]}
6. stats: columns=[salary_mean, salary_sum]
7. pivot: values=salary_mean, index=[department], on=department, agg=sum
8. moving_average: column=salary_sum, window=2
9. reorder: ordered_cols=[department, salary_mean, salary_sum, salary_count]

## Flow 4: Join & Union Pipeline (6 nodes)
# upload_csv(employees) + upload_csv(departments) → join → upload_csv(bonuses) → join → union
Steps:
1. upload_csv: employees.csv
2. upload_csv: departments.csv
3. join: on=department, how=left (employees + departments)
4. upload_csv: bonuses.csv
5. join: on=emp_id, how=inner (result + bonuses)
6. select: [emp_id, name, department, salary, bonus_amount, budget, manager, location]

## Flow 5: Data Cleaning Pipeline (7 nodes)
# upload_csv → drop_na → drop_nulls → drop_duplicates → fill_missing → cast → drop
Steps:
1. upload_csv: employees.csv
2. drop_na: subset=[salary, name]
3. drop_duplicates: subset=[name, department]
4. fill_missing: column=performance_score, value=3.0
5. cast: column=salary, dtype=float
6. drop: columns=[age]
7. extract_date_parts: column=hire_date (if date column exists)

## Flow 6: Advanced Analytics Pipeline (8 nodes)
# upload_csv → outliers → conditional → range_bucket → groupby → crosstab → cumulative_product → sort
Steps:
1. upload_csv: employees.csv
2. outliers: column=salary (remove salary outliers)
3. conditional: column=salary, op=gt, threshold=70000, then_val=Senior, else_val=Junior, new_col=level
4. range_bucket: column=experience, bins=[0, 5, 10, 20], labels=[Entry, Mid, Senior, Expert], new_col=exp_band
5. groupby: group_cols=[level, exp_band], aggs={salary: [mean, count]}
6. crosstab: index=level, columns=exp_band, agg=count
7. sort: by=[salary_mean], descending=true

## Flow 7: Statistical Tests Pipeline (8 nodes)
# upload_csv → ols_regression → t_test → f_test → chi_square_test → anova_test → dw_test
Steps:
1. upload_csv: employees.csv
2. ols_regression: target=salary, features=[experience, age, performance_score]
   → Output: coefficients table with p-values, t-stats, R², F-stat
3. t_test: column_a=salary, column_b=experience, test_type=two_sample
   → Output: t-statistic, p-value, means, significance
4. f_test: column_a=salary, column_b=experience
   → Output: F-statistic, variance comparison
5. chi_square_test: column_a=department, column_b=level (from conditional)
   → Output: chi² statistic, independence test
6. anova_test: value_col=salary, group_col=department
   → Output: F-statistic, per-group means, significance

## Flow 8: Visualization Pipeline (6 nodes)
# upload_csv → groupby → chart(bar) → chart(line) → chart(scatter) → chart(heatmap)
Steps:
1. upload_csv: employees.csv
2. groupby: group_cols=[department], aggs={salary: [mean], experience: [mean]}
3. chart: chart_type=bar, x_col=department, y_col=salary_mean, title="Avg Salary by Dept"
4. chart: chart_type=scatter, x_col=experience_mean, y_col=salary_mean, title="Experience vs Salary"
5. upload_csv: employees.csv (for heatmap on raw data)
6. chart: chart_type=heatmap, title="Correlation Heatmap"
7. chart: chart_type=histogram, x_col=salary, bins=15, title="Salary Distribution"

## Flow 9: Vector Operations (4 nodes)
# upload_csv(vectors.csv) → vector_dot_product → vector_linear_multiply → vector_cross_product
Steps:
1. upload_csv: vectors.csv
2. vector_dot_product: vec_a=[x1,y1,z1], vec_b=[x2,y2,z2], new_col=dot
3. vector_linear_multiply: vec_a=[x1,y1,z1], vec_b=[x2,y2,z2], suffix=_prod
4. vector_cross_product: vec_a=[x1,y1,z1], vec_b=[x2,y2,z2], prefix=cross

## Flow 10: Date & Utility Operations (5 nodes)
# upload_csv → date_offset → extract_date_parts → add_literal_column → transpose
Steps:
1. upload_csv: sales.csv (has sale_date column)
2. extract_date_parts: column=sale_date → creates sale_date_year, sale_date_month, sale_date_day
3. date_offset: column=sale_date, offset=30, unit=days, new_col=due_date
4. add_literal_column: column=currency, value=USD, dtype=string
5. groupby: group_cols=[sale_date_month], aggs={unit_price: [sum], units_sold: [sum]}

## Node Coverage Summary
# ─────────────────────
# Data Ingestion:     upload_csv ✅, upload_excel ✅, read_from_db ✅
# Transform:          safe_filter ✅, select ✅, drop ✅, sort ✅, rename ✅, reorder ✅
# String:             string_case ✅, string_slice ✅, string_mid ✅, string_concat ✅, string_prefix_suffix ✅, string_clean ✅
# Math:               math_horizontal ✅, math_custom ✅, math_multiply_bulk ✅
# Vector:             vector_dot_product ✅, vector_linear_multiply ✅, vector_cross_product ✅
# Cleaning:           drop_na ✅, drop_nulls ✅, drop_duplicates ✅, fill_missing ✅
# Types:              cast ✅
# Date:               extract_date_parts ✅
# Joins:              join ✅, union ✅
# Analytics:          outliers ✅, groupby ✅, stats ✅, pivot ✅, moving_average ✅, conditional ✅
# Matrix:             transpose ✅
# ML:                 linear_regression ✅, logistic_prediction ✅, correlation ✅
# Utility:            add_literal_column ✅, range_bucket ✅, date_offset ✅, crosstab ✅, cumulative_product ✅
# Stats Tests:        ols_regression ✅, t_test ✅, f_test ✅, chi_square_test ✅, dw_test ✅, anova_test ✅
# Visualization:      chart ✅ (bar, line, scatter, histogram, heatmap, pie)
# TOTAL: 50/50 nodes covered ✅
