# AI Workflow Generator — End-to-End Test Use Case

## Scenario: HR Compensation Review & Department Performance Report

A People Analytics team needs to produce two reports from three raw HR data files:
1. A **High Performers Compensation Report** — every employee with a performance score ≥ 3.5,
   enriched with their department location and manager, showing total compensation (salary + bonus).
2. A **Department Summary Report** — one row per department with headcount, average salary,
   total bonus spend, average performance score, and high-performer count.

---

## Input Files

Upload all three files in the "Input File(s)" drop zone.

### 1. `employees.csv`
Path: `backend/test_data/employees.csv`

Columns: `emp_id, name, department, salary, experience, age, performance_score`

100 rows. One row per employee. Contains salary and performance data.

### 2. `departments.csv`
Path: `backend/test_data/departments.csv`

Columns: `department, budget, manager, location`

4 rows. One row per department. Contains manager name and office location.

### 3. `bonuses.csv`
Path: `backend/test_data/bonuses.csv`

Columns: `emp_id, bonus_amount, bonus_type, review_quarter`

100 rows. One row per employee. Contains Q4-2024 bonus payments.

---

## Expected Output Files

Upload in the "Expected Output File" drop zone **one at a time** (run the generator twice,
once per output).

### Output A: `output_high_performers_compensation.csv`
Path: `backend/test_data/output_high_performers_compensation.csv`

Columns: `emp_id, employee_name, department, location, manager, salary, bonus_amount,
          total_compensation, performance_score, experience, performance_band`

19 rows. Only employees with performance_score >= 3.5 (after removing outliers from salary).
Sorted by total_compensation descending.

### Output B: `output_department_summary.csv`
Path: `backend/test_data/output_department_summary.csv`

Columns: `department, location, manager, budget, headcount, avg_salary, total_bonus_spend,
          avg_performance_score, high_performer_count`

4 rows. One per department. Aggregated stats.

---

## Prompt to Use in the AI Workflow Generator

Copy the text below exactly into the "Prompt or Python Code" field:

---

```
I have three input files:

1. employees.csv — columns: emp_id, name, department, salary, experience, age, performance_score
2. departments.csv — columns: department, budget, manager, location
3. bonuses.csv — columns: emp_id, bonus_amount, bonus_type, review_quarter

I need to produce TWO output reports:

--- OUTPUT 1: High Performers Compensation Report ---
Steps:
1. Join employees.csv with bonuses.csv on emp_id (inner join) to bring bonus_amount onto each employee row.
2. Join the result with departments.csv on department (left join) to bring in location and manager.
3. Remove salary outliers using the IQR method on the salary column.
4. Filter to keep only employees where performance_score >= 3.5.
5. Add a new column called total_compensation = salary + bonus_amount (use math horizontal sum on [salary, bonus_amount]).
6. Add a new column called performance_band: if performance_score > 3.5 then "High Performer" else "Standard" (conditional column).
7. Rename the column name to employee_name.
8. Select only these columns: emp_id, employee_name, department, location, manager, salary, bonus_amount, total_compensation, performance_score, experience, performance_band.
9. Sort by total_compensation descending.

--- OUTPUT 2: Department Summary Report ---
Steps:
1. Join employees.csv with bonuses.csv on emp_id (inner join).
2. Join the result with departments.csv on department (left join) to bring in location, manager, budget.
3. Group by department and aggregate:
   - salary: mean (rename to avg_salary), count (rename to headcount)
   - bonus_amount: sum (rename to total_bonus_spend)
   - performance_score: mean (rename to avg_performance_score)
4. Join the grouped result back with departments.csv on department to restore location, manager, budget columns.
5. Select columns: department, location, manager, budget, headcount, avg_salary, total_bonus_spend, avg_performance_score.
6. Sort by department ascending.
```

---

## What the AI Should Generate

### For Output 1 — Expected workflow nodes in order:
1. `upload_csv` — employees.csv
2. `upload_csv` — bonuses.csv
3. `join` — on emp_id, how: inner  (employees + bonuses)
4. `upload_csv` — departments.csv
5. `join` — on department, how: left  (above + departments)
6. `outliers` — column: salary
7. `safe_filter` — performance_score >= 3.5
8. `math_horizontal` — columns: [salary, bonus_amount], new_col: total_compensation, op: sum
9. `conditional` — column: performance_score, op: gt, threshold: 3.5, then_val: "High Performer", else_val: "Standard", new_col: performance_band
10. `rename` — mapping: {"name": "employee_name"}
11. `select` — columns: [emp_id, employee_name, department, location, manager, salary, bonus_amount, total_compensation, performance_score, experience, performance_band]
12. `sort` — by: [total_compensation], descending: true

### For Output 2 — Expected workflow nodes in order:
1. `upload_csv` — employees.csv
2. `upload_csv` — bonuses.csv
3. `join` — on emp_id, how: inner
4. `upload_csv` — departments.csv
5. `join` — on department, how: left
6. `groupby` — group_cols: [department], aggs: {salary: [mean, count], bonus_amount: [sum], performance_score: [mean]}
7. `upload_csv` — departments.csv (second instance for re-join)
8. `join` — on department, how: left  (to restore location/manager/budget)
9. `select` — columns: [department, location, manager, budget, headcount, avg_salary, total_bonus_spend, avg_performance_score]
10. `sort` — by: [department], descending: false

---

## Feasibility Assessment

All steps in this use case use ONLY nodes that exist in the Versor catalogue:
- `upload_csv` ✅
- `join` (inner + left) ✅
- `outliers` ✅
- `safe_filter` ✅
- `math_horizontal` ✅
- `conditional` ✅
- `rename` ✅
- `select` ✅
- `sort` ✅
- `groupby` ✅

Expected AI response: `feasibility: "full"` — no gaps, no tickets filed.

---

## Step-by-Step Test Instructions

### Prerequisites
- Backend running on http://localhost:8000
- Ollama running with llama3 model loaded
- Logged in to Versor UI at http://localhost:80

### Test Run 1 — Output 1 (High Performers Compensation)

1. Navigate to Dashboard → click **AI Generate ✨**
2. In "Input File(s)" drop zone, upload all three files:
   - `employees.csv`
   - `departments.csv`
   - `bonuses.csv`
3. In "Expected Output File" drop zone, upload:
   - `output_high_performers_compensation.csv`
4. Paste the full prompt from above into the text area
5. Click **Generate Workflow**
6. Wait 30–90 seconds for the LLM to respond

**Verify:**
- [ ] Green "Fully achievable" banner appears
- [ ] Reasoning paragraph mentions joins, filter, aggregation
- [ ] Canvas shows ~12 nodes connected in sequence with two branch paths merging at joins
- [ ] First nodes are upload_csv nodes labelled with filenames
- [ ] A `safe_filter` node appears with performance_score condition
- [ ] A `math_horizontal` node appears for total_compensation
- [ ] A `conditional` node appears for performance_band
- [ ] A `sort` node appears at the end
- [ ] No gap tickets are filed (gaps list is empty)
- [ ] "Save as Workflow" button is enabled

7. Rename the workflow to **"High Performers Compensation Q4-2024"**
8. Click **Save as Workflow**
9. Verify you are redirected to the Workflow Editor with all nodes visible

**In the Workflow Editor:**
10. Click the first `upload_csv` node → click Upload → upload `employees.csv`
11. Click the second `upload_csv` node → click Upload → upload `bonuses.csv`
12. Click the third `upload_csv` node → click Upload → upload `departments.csv`
13. Click **Run All**
14. Verify all nodes turn green (success)
15. Click the final `sort` node → click the eye icon to preview data
16. Verify the preview shows columns: emp_id, employee_name, department, location, manager,
    salary, bonus_amount, total_compensation, performance_score, experience, performance_band
17. Verify rows are sorted by total_compensation descending
18. Verify all rows have performance_score >= 3.5

### Test Run 2 — Output 2 (Department Summary)

1. Go back to Dashboard → click **AI Generate ✨**
2. Upload the same three input files
3. Upload `output_department_summary.csv` as the expected output
4. Paste the same full prompt
5. Click **Generate Workflow**

**Verify:**
- [ ] Green "Fully achievable" banner appears
- [ ] Canvas shows ~10 nodes
- [ ] A `groupby` node appears with department as group column
- [ ] A `sort` node appears at the end
- [ ] No gap tickets filed

6. Save as **"Department Summary Q4-2024"**
7. Run the workflow and verify the final output has 4 rows (one per department)

---

## Negative Test — Unsupported Feature (Ticket Filing)

To verify the gap detection and ticket system works, run a third generation with this prompt:

```
Using the employees.csv file, train a deep neural network to predict whether an employee
will leave the company (attrition prediction) based on salary, experience, age, and
performance_score. Use a 3-layer MLP with ReLU activations, dropout of 0.2, and train
for 50 epochs with Adam optimizer. Also generate a SHAP feature importance plot and
export the model as a .pkl file.
```

**Verify:**
- [ ] Red "Not achievable with current nodes" banner appears
- [ ] Reasoning explains neural networks are not supported
- [ ] At least 3 gap tickets are filed:
  - Neural Network / Deep Learning node
  - SHAP / Feature Importance Visualization
  - Model Export / Serialization
- [ ] No workflow canvas is shown (workflow is null)
- [ ] Each gap card shows title, category, description, and upvote button
- [ ] Clicking upvote on a gap card increments the vote count

**Verify tickets in admin panel:**
- [ ] Log in as admin (username: admin, password: Admin1234!)
- [ ] Dashboard header shows "Feature Tickets" button
- [ ] Click it → navigate to /admin/tickets
- [ ] The 3 tickets from the neural network request appear
- [ ] They are sorted by vote_count descending
- [ ] Category bar chart shows "Machine Learning" as top category
- [ ] Status dropdown on each ticket works (change to "in_progress")

---

## Partial Feasibility Test

Run a fourth generation with this prompt:

```
Using employees.csv:
1. Filter employees where salary > 80000
2. Group by department and calculate average salary and count
3. Create a bar chart showing average salary per department
4. Export the chart as a PNG file
```

**Verify:**
- [ ] Amber "Partially achievable" banner appears
- [ ] Feasible steps list shows: filter + groupby steps
- [ ] Gap tickets filed for: chart/visualization node, PNG export
- [ ] A partial workflow IS generated on the canvas (filter + groupby nodes)
- [ ] "Save as Workflow" button is still available for the partial workflow
- [ ] Amber warning banner inside save panel says "Partial workflow generated"

---

## Files Summary

| File | Type | Path |
|------|------|------|
| employees.csv | Input | backend/test_data/employees.csv |
| departments.csv | Input | backend/test_data/departments.csv |
| bonuses.csv | Input | backend/test_data/bonuses.csv |
| output_high_performers_compensation.csv | Expected Output | backend/test_data/output_high_performers_compensation.csv |
| output_department_summary.csv | Expected Output | backend/test_data/output_department_summary.csv |
