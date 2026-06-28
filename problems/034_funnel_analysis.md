# Problem 034: Funnel Analysis

**Difficulty:** Medium  
**Topics:** SQL, Product Analytics, Window Functions  
**Company Tags:** Airbnb, Uber, DoorDash, Any Product Company

## Problem Statement

Build a conversion funnel for an e-commerce checkout flow. Users go through these steps:

1. `view_product` - User views a product page
2. `add_to_cart` - User adds item to cart
3. `begin_checkout` - User starts checkout
4. `purchase` - User completes purchase

Calculate the conversion rate at each step and identify where users drop off.

```sql
events (
    user_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    product_id STRING
)
```

**Expected output:**

```
step | event_type     | users | conversion_rate | drop_off_rate
-----|----------------|-------|-----------------|---------------
1    | view_product   | 10000 | 100.0%          | 0.0%
2    | add_to_cart    | 4500  | 45.0%           | 55.0%
3    | begin_checkout | 2000  | 44.4%           | 55.6%
4    | purchase       | 800   | 40.0%           | 60.0%
```

## Setup

```sql
SELECT event_type, COUNT(DISTINCT user_id) as users
FROM interview.funnel.events
GROUP BY event_type;
```

## Constraints

- Users must complete steps in order (can't purchase without adding to cart)
- A user counts for a step if they ever completed it
- Calculate conversion relative to previous step AND to first step

## Hints

<details>
<summary>Hint 1</summary>
First, find which users completed each step (ever).
</details>

<details>
<summary>Hint 2</summary>
You need to ensure ordering - a purchase only counts if add_to_cart happened before it.
</details>

<details>
<summary>Hint 3</summary>
Use MIN(event_timestamp) per user per event_type to get first occurrence of each event.
</details>

## Solution

<details>
<summary>Click to reveal solution</summary>

```sql
-- Step 1: Get first occurrence of each event per user
WITH user_events AS (
    SELECT 
        user_id,
        event_type,
        MIN(event_timestamp) as first_occurrence
    FROM events
    GROUP BY user_id, event_type
),

-- Step 2: Pivot to get one row per user with timestamp of each step
user_funnel AS (
    SELECT 
        user_id,
        MAX(CASE WHEN event_type = 'view_product' THEN first_occurrence END) as view_ts,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN first_occurrence END) as cart_ts,
        MAX(CASE WHEN event_type = 'begin_checkout' THEN first_occurrence END) as checkout_ts,
        MAX(CASE WHEN event_type = 'purchase' THEN first_occurrence END) as purchase_ts
    FROM user_events
    GROUP BY user_id
),

-- Step 3: Filter for valid sequences (must be in order)
valid_funnel AS (
    SELECT 
        user_id,
        view_ts,
        CASE WHEN cart_ts > view_ts THEN cart_ts END as cart_ts,
        CASE WHEN checkout_ts > cart_ts AND cart_ts > view_ts THEN checkout_ts END as checkout_ts,
        CASE WHEN purchase_ts > checkout_ts AND checkout_ts > cart_ts THEN purchase_ts END as purchase_ts
    FROM user_funnel
    WHERE view_ts IS NOT NULL
),

-- Step 4: Count users at each step
step_counts AS (
    SELECT 
        COUNT(view_ts) as step_1_view,
        COUNT(cart_ts) as step_2_cart,
        COUNT(checkout_ts) as step_3_checkout,
        COUNT(purchase_ts) as step_4_purchase
    FROM valid_funnel
)

-- Step 5: Format output
SELECT 
    step,
    event_type,
    users,
    ROUND(100.0 * users / FIRST_VALUE(users) OVER (ORDER BY step), 1) as overall_conversion,
    ROUND(100.0 * users / LAG(users, 1, users) OVER (ORDER BY step), 1) as step_conversion
FROM (
    SELECT 1 as step, 'view_product' as event_type, step_1_view as users FROM step_counts
    UNION ALL
    SELECT 2, 'add_to_cart', step_2_cart FROM step_counts
    UNION ALL
    SELECT 3, 'begin_checkout', step_3_checkout FROM step_counts
    UNION ALL
    SELECT 4, 'purchase', step_4_purchase FROM step_counts
) funnel
ORDER BY step;
```

**Alternative - More dynamic approach:**

```sql
WITH step_order AS (
    SELECT 
        'view_product' as event_type, 1 as step_num UNION ALL
    SELECT 'add_to_cart', 2 UNION ALL
    SELECT 'begin_checkout', 3 UNION ALL
    SELECT 'purchase', 4
),

user_max_step AS (
    SELECT 
        e.user_id,
        MAX(s.step_num) as max_step_completed
    FROM events e
    JOIN step_order s ON e.event_type = s.event_type
    GROUP BY e.user_id
)

SELECT 
    s.step_num as step,
    s.event_type,
    COUNT(CASE WHEN u.max_step_completed >= s.step_num THEN 1 END) as users
FROM step_order s
CROSS JOIN user_max_step u
GROUP BY s.step_num, s.event_type
ORDER BY s.step_num;
```

</details>

## Follow-up Questions

1. **Time-bounded funnel:** Only count if all steps happen within 24 hours
2. **Per-product funnel:** Break down by product category
3. **Cohort analysis:** How does the funnel change over time?

## What Interviewers Look For

1. **Understanding of funnels:** Events must be sequential
2. **Handling order:** Timestamp comparison for valid sequences
3. **Two types of conversion:** Overall (vs step 1) and step-to-step
4. **Clean SQL:** Can be done multiple ways - clarity matters
