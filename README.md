# DataEngineer.io

The LeetCode for Data Engineering. Practice real Spark, SQL, and pipeline problems with actual compute.

**[Live Demo](https://dataengineer.io)** | **[Problem Bank](#problems)** | **[Contributing](#contributing)**

![DataEngineer.io Screenshot](docs/screenshot.png)

## Why?

| Platform | SQL | Spark | Pipelines | Real Compute |
|----------|-----|-------|-----------|--------------|
| LeetCode | Basic | No | No | No |
| DataLemur | Yes | No | No | No |
| StrataScratch | Yes | No | No | No |
| **DataEngineer.io** | **Yes** | **Yes** | **Yes** | **Yes** |

Most interview prep platforms only test SQL. But data engineering interviews ask about:
- Spark optimization (skew, shuffle, broadcast)
- Incremental pipelines (watermarks, idempotency)
- Dimensional modeling (SCD Type 2, star schema)
- Data quality (validation, testing)
- Performance tuning (partitioning, Z-order, clustering)

This platform lets you practice all of that with real Spark execution.

## Features

- **50+ Problems** covering Spark, SQL, dbt, and pipeline design
- **Real Execution** - Your code runs on actual Spark, not a simulator
- **Instant Feedback** - See if your solution is correct and how fast it runs
- **Company Tags** - Know which companies ask which types of problems
- **Difficulty Levels** - Easy, Medium, Hard, Expert
- **Progress Tracking** - Track your solved problems and streaks
- **Discussion** - Learn from community solutions

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

### Option 1: Use the hosted version
Visit [dataengineer.io](https://dataengineer.io) - no setup required.

### Option 2: Run locally

```bash
# Clone the repo
git clone https://github.com/Amin-Siddique/dataengineer-io.git
cd dataengineer-io

# Start the platform (requires Docker)
docker compose up -d

# Open http://localhost:3000
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
-- See full solution in the platform
```
</details>

## Tech Stack

- **Frontend:** HTML/CSS/JS (vanilla, no framework bloat)
- **Backend:** Python FastAPI
- **Execution Engine:** Apache Spark 3.5 + Delta Lake 3.0
- **Infrastructure:** Docker, runs on [Lakehouse Local](https://github.com/Amin-Siddique/lakehouse-local)

## Problems

### Spark Optimization

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 001 | [Fix the Skewed Join](problems/001_fix_skewed_join.md) | Medium | Meta, Netflix |
| 002 | [Optimize Broadcast Join](problems/002_broadcast_join.md) | Easy | Amazon, Google |
| 003 | [Reduce Shuffle Size](problems/003_reduce_shuffle.md) | Medium | Uber, Lyft |
| 004 | [Handle Data Skew with Salting](problems/004_salting.md) | Hard | Meta, Airbnb |
| 005 | [Optimize Window Functions](problems/005_window_optimization.md) | Medium | Netflix, Spotify |
| 006 | [Cache vs Persist Strategy](problems/006_cache_persist.md) | Medium | LinkedIn, Twitter |
| 007 | [Partition Pruning](problems/007_partition_pruning.md) | Easy | Databricks, Snowflake |
| 008 | [Adaptive Query Execution](problems/008_aqe.md) | Medium | Databricks |
| 009 | [Optimize Aggregations](problems/009_aggregations.md) | Medium | Google, Meta |
| 010 | [Memory Tuning](problems/010_memory_tuning.md) | Hard | Netflix, Uber |

### Incremental Pipelines

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 011 | [Incremental Load with Watermarks](problems/011_incremental_watermarks.md) | Medium | Airbnb, Stripe |
| 012 | [CDC with Delta Lake](problems/012_cdc_delta.md) | Medium | Databricks |
| 013 | [Idempotent Pipelines](problems/013_idempotent.md) | Hard | Netflix, Uber |
| 014 | [Late Arriving Data](problems/014_late_data.md) | Hard | Lyft, DoorDash |
| 015 | [Exactly Once Processing](problems/015_exactly_once.md) | Expert | Confluent, Uber |

### Dimensional Modeling

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 016 | [SCD Type 2 Implementation](problems/016_scd_type_2.md) | Hard | Amazon, Walmart |
| 017 | [SCD Type 1 vs Type 2](problems/017_scd_comparison.md) | Medium | Target, Costco |
| 018 | [Build a Star Schema](problems/018_star_schema.md) | Medium | Any retail |
| 019 | [Fact Table Design](problems/019_fact_tables.md) | Medium | Netflix, Spotify |
| 020 | [Bridge Tables](problems/020_bridge_tables.md) | Hard | Amazon |

### Data Quality

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 021 | [Data Validation Pipeline](problems/021_validation.md) | Medium | Airbnb |
| 022 | [Anomaly Detection](problems/022_anomaly.md) | Hard | Stripe, Square |
| 023 | [Schema Drift Handling](problems/023_schema_drift.md) | Medium | Databricks |
| 024 | [Duplicate Detection](problems/024_duplicates.md) | Easy | Any |
| 025 | [Data Reconciliation](problems/025_reconciliation.md) | Hard | Banks, Fintech |

### Delta Lake / Performance

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 026 | [Optimize Small Files](problems/026_small_files.md) | Easy | Databricks |
| 027 | [Z-Order Strategy](problems/027_zorder.md) | Medium | Databricks |
| 028 | [Liquid Clustering](problems/028_liquid_clustering.md) | Medium | Databricks |
| 029 | [Vacuum Strategy](problems/029_vacuum.md) | Easy | Any Delta user |
| 030 | [Time Travel Queries](problems/030_time_travel.md) | Easy | Databricks |

### SQL Advanced

| # | Problem | Difficulty | Companies |
|---|---------|------------|-----------|
| 031 | [Running Totals](problems/031_running_totals.md) | Easy | Any |
| 032 | [Gap and Island](problems/032_gap_island.md) | Hard | Google, Meta |
| 033 | [Sessionization](problems/033_sessionization.md) | Medium | Amplitude, Mixpanel |
| 034 | [Funnel Analysis](problems/034_funnel.md) | Medium | Any product company |
| 035 | [Retention Cohorts](problems/035_retention.md) | Medium | Any product company |

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md).

### Adding a Problem

1. Create `problems/XXX_problem_name.md` using the template
2. Add test data in `data/XXX/`
3. Add expected output in `solutions/XXX/`
4. Submit a PR

### Problem Template

```markdown
# Problem XXX: Title

**Difficulty:** Easy/Medium/Hard/Expert
**Topics:** Topic1, Topic2
**Company Tags:** Company1, Company2

## Problem Statement
[Clear description of what to solve]

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

- [x] Core platform with 30 problems
- [x] Real Spark execution
- [ ] User accounts and progress tracking
- [ ] Leaderboard
- [ ] Discussion forum
- [ ] Company-specific problem sets
- [ ] Mock interview mode
- [ ] Video explanations

## License

MIT License - see [LICENSE](LICENSE).

---

Built by [Amin Siddique](https://github.com/Amin-Siddique) | Star this repo if it helps you land your dream job!
