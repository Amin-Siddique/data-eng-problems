# Problem 032: Gap and Island Problem

**Difficulty:** Hard  
**Topics:** SQL, Window Functions, Pattern Recognition  
**Company Tags:** Google, Meta, Amazon, Uber

## Problem Statement

You have a table of user login sessions. Find continuous periods (islands) of daily activity for each user, and identify the gaps between them.

```sql
user_logins (
    user_id STRING,
    login_date DATE
)

-- Sample data:
-- user_id | login_date
-- --------|------------
-- alice   | 2024-01-01
-- alice   | 2024-01-02
-- alice   | 2024-01-03
-- alice   | 2024-01-07  -- gap: 4 days
-- alice   | 2024-01-08
-- bob     | 2024-01-01
-- bob     | 2024-01-05
```

**Expected output:**

```
user_id | streak_start | streak_end | streak_days
--------|--------------|------------|------------
alice   | 2024-01-01   | 2024-01-03 | 3
alice   | 2024-01-07   | 2024-01-08 | 2
bob     | 2024-01-01   | 2024-01-01 | 1
bob     | 2024-01-05   | 2024-01-05 | 1
```

## Setup

```sql
SELECT * FROM interview.sql.user_logins ORDER BY user_id, login_date;
```

## Constraints

- A streak is consecutive days with at least one login
- Single-day activity counts as a streak of 1
- Must handle multiple users correctly
- Order results by user_id, streak_start

## Hints

<details>
<summary>Hint 1</summary>
The classic approach uses the difference between row_number and the date itself to identify groups.
</details>

<details>
<summary>Hint 2</summary>
If you subtract a sequence number from each date, consecutive dates will have the same result.
</details>

<details>
<summary>Hint 3</summary>
DATE_SUB(login_date, ROW_NUMBER()) creates a "group identifier" - consecutive dates produce the same value.
</details>

## Solution

<details>
<summary>Click to reveal solution</summary>

```sql
-- Classic Gap and Island solution using ROW_NUMBER trick

WITH numbered AS (
    -- Assign row numbers within each user, ordered by date
    SELECT 
        user_id,
        login_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as rn
    FROM user_logins
),

grouped AS (
    -- The magic: subtract row number from date
    -- Consecutive dates will have the same "group_id"
    SELECT 
        user_id,
        login_date,
        DATE_SUB(login_date, rn) as group_id
    FROM numbered
)

SELECT 
    user_id,
    MIN(login_date) as streak_start,
    MAX(login_date) as streak_end,
    DATEDIFF(MAX(login_date), MIN(login_date)) + 1 as streak_days
FROM grouped
GROUP BY user_id, group_id
ORDER BY user_id, streak_start;
```

**How the ROW_NUMBER trick works:**

```
login_date | rn | date - rn (group_id)
-----------|----|-----------------------
2024-01-01 | 1  | 2023-12-31  ← same group
2024-01-02 | 2  | 2023-12-31  ← same group
2024-01-03 | 3  | 2023-12-31  ← same group
2024-01-07 | 4  | 2024-01-03  ← new group (gap detected!)
2024-01-08 | 5  | 2024-01-03  ← same group
```

**Alternative solution using LAG:**

```sql
WITH with_gap AS (
    SELECT 
        user_id,
        login_date,
        CASE 
            WHEN DATEDIFF(login_date, LAG(login_date) OVER (
                PARTITION BY user_id ORDER BY login_date
            )) > 1 THEN 1 
            ELSE 0 
        END as is_new_streak
    FROM user_logins
),

with_streak_id AS (
    SELECT 
        user_id,
        login_date,
        SUM(is_new_streak) OVER (
            PARTITION BY user_id ORDER BY login_date
        ) as streak_id
    FROM with_gap
)

SELECT 
    user_id,
    MIN(login_date) as streak_start,
    MAX(login_date) as streak_end,
    COUNT(*) as streak_days
FROM with_streak_id
GROUP BY user_id, streak_id
ORDER BY user_id, streak_start;
```

</details>

## Follow-up Questions

1. **Find the longest streak per user** - Add a ranking step
2. **Find gaps longer than N days** - Filter on DATEDIFF between streak_end and next streak_start
3. **Handle multiple logins per day** - Add DISTINCT or pre-aggregate

## What Interviewers Look For

1. **Pattern recognition:** This is a classic SQL pattern - have you seen it before?
2. **Window function mastery:** ROW_NUMBER, LAG, running SUM
3. **Clear explanation:** Can you explain WHY the date-minus-row-number trick works?
4. **Edge cases:** Single-day streaks, first/last rows, ties
