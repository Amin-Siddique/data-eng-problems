# Problem 033: Sessionization

**Difficulty:** Medium  
**Topics:** SQL, Window Functions, Analytics  
**Company Tags:** Amplitude, Mixpanel, Google Analytics, Airbnb

## Problem Statement

You have a stream of user clickstream events. Group them into sessions where a session ends after 30 minutes of inactivity.

```sql
clickstream (
    user_id STRING,
    event_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP
)
```

**Expected output:**

```
user_id | session_id | session_start       | session_end         | event_count | duration_minutes
--------|------------|---------------------|---------------------|-------------|------------------
alice   | 1          | 2024-01-01 10:00:00 | 2024-01-01 10:15:00 | 5           | 15
alice   | 2          | 2024-01-01 14:30:00 | 2024-01-01 14:45:00 | 3           | 15
bob     | 1          | 2024-01-01 09:00:00 | 2024-01-01 09:05:00 | 2           | 5
```

## Setup

```sql
SELECT * FROM interview.sql.clickstream 
WHERE user_id = 'user_001'
ORDER BY event_timestamp
LIMIT 20;
```

## Constraints

- Session timeout: 30 minutes of inactivity
- Session IDs should be sequential per user (1, 2, 3...)
- Include session duration in minutes
- Single-event sessions are valid

## Hints

<details>
<summary>Hint 1</summary>
Use LAG to find the time since the previous event.
</details>

<details>
<summary>Hint 2</summary>
If the gap is > 30 minutes, mark it as a new session.
</details>

<details>
<summary>Hint 3</summary>
Use a running SUM of the "is_new_session" flag to generate session IDs.
</details>

## Solution

<details>
<summary>Click to reveal solution</summary>

```sql
WITH with_prev AS (
    -- Step 1: Get previous event timestamp
    SELECT 
        user_id,
        event_id,
        event_type,
        event_timestamp,
        LAG(event_timestamp) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) as prev_timestamp
    FROM clickstream
),

with_session_flag AS (
    -- Step 2: Mark new sessions (gap > 30 minutes or first event)
    SELECT 
        *,
        CASE 
            WHEN prev_timestamp IS NULL THEN 1
            WHEN TIMESTAMPDIFF(MINUTE, prev_timestamp, event_timestamp) > 30 THEN 1
            ELSE 0
        END as is_new_session
    FROM with_prev
),

with_session_id AS (
    -- Step 3: Generate session IDs using running sum
    SELECT 
        *,
        SUM(is_new_session) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) as session_id
    FROM with_session_flag
)

-- Step 4: Aggregate by session
SELECT 
    user_id,
    session_id,
    MIN(event_timestamp) as session_start,
    MAX(event_timestamp) as session_end,
    COUNT(*) as event_count,
    TIMESTAMPDIFF(MINUTE, MIN(event_timestamp), MAX(event_timestamp)) as duration_minutes
FROM with_session_id
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

**How it works:**

1. **LAG** gives us the previous event's timestamp
2. **CASE** creates a binary flag: 1 = new session, 0 = same session
3. **Running SUM** of the flag becomes the session ID
4. **GROUP BY** aggregates events into sessions

**Example walkthrough:**

```
event_timestamp     | prev_timestamp      | gap_minutes | is_new_session | session_id
--------------------|---------------------|-------------|----------------|------------
10:00:00           | NULL                | -           | 1              | 1
10:05:00           | 10:00:00            | 5           | 0              | 1
10:15:00           | 10:05:00            | 10          | 0              | 1
14:30:00           | 10:15:00            | 255         | 1              | 2  ← new session!
14:35:00           | 14:30:00            | 5           | 0              | 2
```

</details>

## Follow-up Questions

1. **What if timeout varies by user type?** - Join with user table for custom timeouts
2. **Calculate bounce rate** - Sessions with event_count = 1
3. **Find sessions with specific event sequences** - Add LEAD/LAG for pattern matching

## What Interviewers Look For

1. **Standard pattern:** Sessionization is a canonical analytics problem
2. **Window functions:** LAG, running SUM
3. **Edge cases:** First event, single-event sessions
4. **Follow-ups:** Can you adapt to different requirements?
