# Data Engineering Problems

Practice real Spark, SQL, and pipeline interview problems. Open-source interview prep for data engineers.

**[Problem Bank](#problems)** | **[Run Locally](#quick-start)** | **[Contributing](#contributing)**

## Why?

| Platform | SQL | Spark | Pipelines | Real Compute |
|----------|-----|-------|-----------|--------------|
| LeetCode | Basic | No | No | No |
| DataLemur | Yes | No | No | No |
| StrataScratch | Yes | No | No | No |
| **This Repo** | **Yes** | **Yes** | **Yes** | **Yes** |

Most interview prep platforms only test SQL. But data engineering interviews ask about:
- Spark optimization (skew, shuffle, broadcast)
- Incremental pipelines (watermarks, idempotency)
- Dimensional modeling (SCD Type 2, star schema)
- Data quality (validation, testing)
- Performance tuning (partitioning, Z-order, clustering)

This repo lets you practice all of that with real Spark execution.

## Features

- **50+ Problems** covering Spark, SQL, dbt, and pipeline design
- **Real Execution** - Run with [lakehouse-local](https://github.com/Amin-Siddique/lakehouse-local) (optional)
- **Company Tags** - Know which companies ask which types of problems
- **Difficulty Levels** - Easy, Medium, Hard, Expert
- **Detailed Solutions** - Multiple approaches with trade-off analysis

## Problem Categories

| Category | Count | Topics |
|----------|-------|--------|
| **Spark Optimization** | 15 | Skew, shuffle, broadcast, caching, partitioning |
| **SQL Advanced** | 12 | Window functions, CTEs, recursive queries |
| **Incremental Pipelines** | 8 | Watermarks, CDC, merge, idempotency |
| **Dimensional Modeling** | 8 | SCD Type 1/2/3, star schema, fact tables |
| **Data Quality** | 5 | Validation, testing, anomaly detection |
| **Performance Tuning** | 7 | Delta Lake, Z-order, clustering, vacuum |

## Quick Start

**Option 1: Just read the problems** (no setup needed)
- Browse the [problems/](problems/) folder
- Each problem has setup, hints, and detailed solutions

**Option 2: Practice with real Spark** (requires Docker)
```bash
# Clone the execution environment
git clone https://github.com/Amin-Siddique/lakehouse-local.git
cd lakehouse-local
docker compose up -d

# Open http://localhost:8888 and try the problems!
```

## Sample Problem

### Fix the Skewed Join

**Difficulty:** Medium | **Company Tags:** Meta, Netflix, Uber

You have two tables with a heavily skewed join key. The query times out.

```sql
SELECT c.name, COUNT(*) as orders
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY c.name;
```

**Your task:** Rewrite to handle 100:1 skew efficiently.

<details>
<summary>Show Solution</summary>

```sql
-- Option 1: Broadcast hint (if small table fits in memory)
SELECT /*+ BROADCAST(c) */ c.name, COUNT(*) as orders
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY c.name;

-- Option 2: Salting (for extreme skew)
-- See full solution in problems/001_fix_skewed_join.md
```
</details>

## Problems

### Spark Optimization

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 001 | [Fix the Skewed Join](problems/001_fix_skewed_join.md) | Medium | Meta, Netflix |
| 002 | [Optimize Broadcast Join](problems/002_broadcast_join.md) | Easy | Amazon, Google |
| 003 | [Reduce Shuffle Size](problems/003_reduce_shuffle.md) | Medium | Uber, Lyft |
| 004 | [Handle Data Skew with Salting](problems/004_salting.md) | Hard | Meta, Airbnb |
| 005 | [Optimize Window Functions](problems/005_window_optimization.md) | Medium | Netflix, Spotify |

### Incremental Pipelines

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 011 | [Incremental Load with Watermarks](problems/002_incremental_load.md) | Medium | Airbnb, Stripe |
| 012 | [CDC with Delta Lake](problems/012_cdc_delta.md) | Medium | Databricks |
| 013 | [Idempotent Pipelines](problems/013_idempotent.md) | Hard | Netflix, Uber |

### Dimensional Modeling

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 016 | [SCD Type 2 Implementation](problems/003_scd_type_2.md) | Hard | Amazon, Walmart |
| 017 | [SCD Type 1 vs Type 2](problems/017_scd_comparison.md) | Medium | Target, Costco |
| 018 | [Build a Star Schema](problems/018_star_schema.md) | Medium | Any retail |

### Delta Lake / Performance

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 026 | [Optimize Small Files](problems/026_small_files.md) | Easy | Databricks |
| 027 | [Z-Order Strategy](problems/027_zorder.md) | Medium | Databricks |
| 028 | [Liquid Clustering](problems/028_liquid_clustering.md) | Medium | Databricks |

### SQL Advanced

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 032 | [Gap and Island](problems/032_gap_and_island.md) | Hard | Google, Meta |
| 033 | [Sessionization](problems/033_sessionization.md) | Medium | Amplitude, Mixpanel |
| 034 | [Funnel Analysis](problems/034_funnel_analysis.md) | Medium | Any product company |

## Tech Stack

- **Problems:** Markdown with SQL/PySpark solutions
- **Execution:** [lakehouse-local](https://github.com/Amin-Siddique/lakehouse-local) (Spark 3.5 + Delta Lake 3.0)
- **UI:** Static HTML (can be hosted anywhere)

## Contributing

Contributions welcome! 

### Adding a Problem

1. Create `problems/XXX_problem_name.md` using the template below
2. Submit a PR

### Problem Template

```markdown
# Problem XXX: Title

**Difficulty:** Easy/Medium/Hard/Expert
**Topics:** Topic1, Topic2
**Company Tags:** Company1, Company2

## Problem Statement
[Clear description]

## Setup
[SQL to see the data]

## Constraints
[Rules and limits]

## Hints
[Progressive hints in collapsible sections]

## Solution
[Full solution with explanation]
```

## Roadmap

- [x] Core problems (Spark, SQL, Pipelines)
- [x] Local Spark execution via lakehouse-local
- [ ] 50+ problems across all categories
- [ ] Web UI for browser-based practice
- [ ] Leaderboard and progress tracking
- [ ] Video explanations

## License

MIT License - see [LICENSE](LICENSE).

---

Built by [Amin Siddique](https://github.com/Amin-Siddique) | Star this repo if it helps you!
