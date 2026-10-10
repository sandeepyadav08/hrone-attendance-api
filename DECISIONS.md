# DECISIONS.md

1. **Indexes.** `uniq_emp_code` keeps every emp_code unique, so two employees with the same code can't be created. `uniq_emp_code_date` allows only one attendance record per employee per date, so two punch-ins at the same time can't both be saved. `emp_code_punch_in` finds the employee's latest punch-in quickly at punch-out, so there is no full collection scan. The other indexes support list sorting/filters and the month ranges in analytics. I did not create an index on `late_minutes`. The date index already narrows the leaderboard to one month, and an extra index would only slow down writes.

2. **Punch-in race.** MongoDB enforces uniqueness itself through the unique index on (emp_code, date), which is created at startup. One insert succeeds. The other gets `DuplicateKeyError`, and I return 409. I don't check first and then insert, because both requests could pass the check.

3. **Ties.** Tied employees get the same rank (`$rank`), and the limit is applied on rank, not on rows. With limit=2 and three people tied at the top, all three come back with rank 1, sorted by emp_code.

4. **Headcount.** The pipeline starts from `employees`, not logs. `$lookup` only attaches logs and never removes the employee, so someone with zero punch-ins still counts as 1.

5. **100x data.** I would store a pre-calculated daily summary, so analytics don't scan crores of records every time. Dashboards would be faster and the database load lower.
