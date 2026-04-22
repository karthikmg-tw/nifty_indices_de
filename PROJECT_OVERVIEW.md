# Nifty Indices Data Engineering Project — Full Overview

---

## What Are We Building?

A production-style Data Engineering pipeline that takes 14 raw Nifty Indian stock market index CSV files and transforms them — through three structured layers (Bronze, Silver, Gold) — into clean, analytics-ready Delta tables on Databricks.

The end result is a set of Gold tables that directly answer four business questions:
1. How do market trends evolve over time?
2. What are the volatility patterns across indices?
3. How do sectors perform relative to each other?
4. What historical patterns support forecasting? (DE deliverable: feature tables — no ML)

---

## Dataset

**14 CSV files** in `dataset/`, each representing a daily time-series of an Indian equity index from approximately 2000 onwards:

| Type | Indices | Columns Available |
|------|---------|-------------------|
| Volatility | INDIAVIX | Date, Open, High, Low, Close, Previous, Change, %Change |
| Size / Broad (no volume) | NIFTY 50, NIFTY NEXT 50, NIFTY MICROCAP 250, NIFTY MIDCAP 150, NIFTY SMALLCAP 250 | Date, Open, High, Low, Close, P/E, P/B, Div Yield % |
| Sector / Broad (with volume) | NIFTY 100, NIFTY 500, NIFTY AUTO, NIFTY BANK, NIFTY FMCG, NIFTY IT, NIFTY METAL, NIFTY PHARMA | Date, Open, High, Low, Close, Volume, Turnover, P/E, P/B, Div Yield |

---

## Architecture: Medallion Pattern

We follow a Medallion (Bronze → Silver → Gold) architecture, running on **Databricks Delta Live Tables (DLT)**. This is the same pattern used in the reference project (`data-sentinels`) which processed the Olist Brazilian e-commerce dataset.

```
Raw CSVs (Databricks Volume)
        ↓
    [ BRONZE ]       14 tables, one per CSV — minimal transformation, DQ rules, ingestion timestamp
        ↓
    [ SILVER ]       Cleaned, enriched, unified — one foundation table + 3 use-case tables
        ↓
    [ GOLD ]         KPI aggregations, executive summaries — directly answers business questions
```

### Why DLT?
- Declarative pipeline: define tables with `@dlt.table`, Databricks handles dependencies and execution order
- Built-in data quality: `@dlt.expect_or_drop` enforces rules at each layer
- Delta format: ACID transactions, time-travel, efficient upserts

---

## Databricks Infrastructure

```
Catalog:   nifty_de
Schemas:   nifty_de.bronze    nifty_de.silver    nifty_de.gold
Volume:    /Volumes/nifty_de/raw/ingestion_layer/    ← CSVs uploaded here manually
```

---

## Layer-by-Layer Breakdown

---

### BRONZE LAYER

**Purpose:** Ingest each raw CSV as-is into a Delta table. Minimal transformation — just type casting, column renaming to avoid special characters, a data quality gate, and an `ingestion_ts` timestamp.

**How it works:**
- A single configuration file (`utilities/ingestion_config.json`) defines all 14 files: their paths, declared schemas, and DQ rules.
- `transformations/bronze/data_ingestion.py` reads this config and generates 14 DLT table definitions in a loop.
- Each table gets two DQ rules: `Date IS NOT NULL` and `Close > 0`. Rows failing these are dropped.

**Key design decisions:**
- We use explicit positional schemas (not header inference) so that `Div Yield %` (Group B) and `Div Yield` (Group C) both land as `DivYield` — no header mismatch.
- Dash `"-"` placeholder values in early MICROCAP/SMALLCAP rows become `null` automatically when Spark tries to cast them to DOUBLE. No special handling needed.
- `%Change` in the VIX file is declared as `PctChange` to avoid the percent symbol in schema strings.

**Output:** 14 bronze tables — `raw_indiavix`, `raw_nifty50`, `raw_niftynext50`, ..., `raw_niftypharma`

---

### SILVER LAYER

Silver has two shared foundation tables (dependencies for all use-case tables) and three use-case-specific tables.

---

#### Shared Foundation

**`vix_daily_silver`** — Clean VIX-only table
- VIX is the India Volatility Index. It has a completely different column structure from equity indices (no P/E, P/B, Volume), so it is kept separate and joined into the volatility use case later.
- Transformations: date casting, snake_case column names, positive close filter.

**`index_daily_silver`** — The core unified equity table
- Unions all 13 equity indices into a single table with an `index_name` discriminator column.
- Each file is normalised via `normalize_index_frame()` before unioning: date casting, column renaming, adding `index_category` (broad / size / sector), adding null `volume`/`turnover` columns for the 5 files that don't have them.
- Deduplication: `dedupe_by_date()` handles the known duplicate row in NIFTY SMALLCAP 250 at inception (2005-04-01).
- Adds `is_zero_ohlc` flag for inception rows where open=high=low=0 (some indices start with zero prices). These rows are warned at the DQ layer but NOT dropped — they are filtered by downstream silver tables using `is_zero_ohlc = false`.
- This table is the single source of truth for UC1, UC2, and UC3.

---

#### UC1 — Market Trends: `index_trends_silver`

**Answers:** How do market trends evolve over time?

**Source:** `index_daily_silver` (zero-OHLC rows excluded)

**What it adds:**
- `daily_return_pct` = (close - prev_close) / prev_close × 100
- `prev_close` via lag window, **partitioned by `index_name`** — critical to avoid return calculations bleeding across different index series
- Calendar parts: `trade_year`, `trade_month`, `trade_quarter`, `trade_week`, `day_of_week`, `is_month_end`, `is_quarter_end`

This enriched daily table feeds all trend Gold tables.

---

#### UC2 — Volatility: `index_volatility_silver`

**Answers:** What are the volatility patterns across indices?

**Source:** `index_daily_silver` LEFT JOIN `vix_daily_silver` on `trade_date`

**What it adds:**
- `high_low_spread_pct` = (high - low) / prev_close × 100 — measures daily price range
- `true_range` = max(high−low, |high−prev_close|, |low−prev_close|) — a better intraday volatility measure that accounts for overnight gaps
- `rolling_vol_21d` and `rolling_vol_63d` — rolling standard deviation of `daily_return_pct` (21-day ≈ 1 month, 63-day ≈ 1 quarter), partitioned by `index_name`
- `drawdown_pct` = (close − 52-week high) / 52-week high × 100 — how far below recent peak
- `vix_close` and `vix_regime` — the VIX value for that date and a 4-bucket regime: low (<15), moderate (15–25), elevated (25–40), extreme (>40)

Note: VIX data ends in July 2021. For dates after that, `vix_close` and `vix_regime` are null — this is handled with a LEFT join (not inner).

---

#### UC3 — Sector Rotation: `sector_monthly_returns_silver`

**Answers:** How do different sectors perform relative to each other?

**Source:** `index_daily_silver` filtered to `index_category IN ('sector', 'broad')`

**What it adds (monthly grain):**
- `month_open` = first trading day's close in that month
- `month_close` = last trading day's close in that month
- `monthly_return_pct` = (month_close − month_open) / month_open × 100
- `nifty50_monthly_return` — same calculation for NIFTY 50, joined back as the benchmark
- `relative_strength` = sector monthly return − NIFTY 50 monthly return (positive = outperformed)
- `sector_rank_in_month` — rank among all sector indices for that month (1 = best performer)

---

### GOLD LAYER

Gold tables are the final, analytics-ready outputs. Each use case has 2–3 Gold tables and one executive summary table (a single-row snapshot of the latest KPIs).

---

#### UC1 Gold — Market Trend KPIs (`05_gold_trends_kpis.py`)

| Table | Grain | Purpose |
|-------|-------|---------|
| `gold_index_monthly_trends` | (index_name, month) | Monthly returns with MoM %, YoY %, rolling 3M/6M returns, avg P/E, P/B per index |
| `gold_index_yearly_trends` | (index_name, year) | Annual returns with YoY %, max/min monthly return, avg valuation metrics |
| `gold_trends_exec_summary` | 1 row | Best/worst YTD index, NIFTY 50 latest P/E and P/B, data freshness timestamp |

**Key patterns:** Window functions for MoM (lag over previous month), YoY (lag over 12 months), rolling sums — all partitioned by `index_name`. Single-row exec summary uses a dummy-key broadcast join (same pattern as the reference project's `build_gold_revenue_exec_summary`).

---

#### UC2 Gold — Volatility KPIs (`06_gold_volatility_kpis.py`)

| Table | Grain | Purpose |
|-------|-------|---------|
| `gold_index_rolling_volatility` | (index_name, date) | Full daily fact — rolling vol, drawdown, VIX context per index per day |
| `gold_volatility_monthly_summary` | (index_name, month) | Monthly aggregation: avg spread, monthly return stdev, max drawdown, avg VIX, dominant regime, `instability_flag` |
| `gold_volatility_exec_summary` | 1 row | Most/least volatile index right now, current VIX regime, highest-ever VIX date |

The `instability_flag` in the monthly summary is `true` when that month's return standard deviation exceeds the historical 90th percentile — a simple market stress indicator.

---

#### UC3 Gold — Sector Rotation KPIs (`07_gold_sector_rotation_kpis.py`)

| Table | Grain | Purpose |
|-------|-------|---------|
| `gold_sector_monthly_performance` | (index_name, month) | Returns, relative strength, rank, rolling RS 3M/6M, momentum signal quadrant |
| `gold_sector_leaders_laggards` | (month, position) | Top 3 leaders and bottom 3 laggards per month — ready for charting |
| `gold_sector_exec_summary` | 1 row | Current leading/lagging sector, best/worst sector YTD |

The **momentum signal** classifies each sector into one of four quadrants based on current monthly return and rolling 3-month relative strength:
- **Leading** — outperforming now and over the past 3 months
- **Deteriorating** — strong history but underperforming this month
- **Recovering** — recently lagged but bouncing back
- **Lagging** — underperforming now and over 3 months

---

#### Q4 Gold — Forecast Feature Tables (`08_gold_forecast_features.py`)

This is the DE deliverable for Question 4. Actual ML model training is out of scope. The DE pipeline delivers two Gold tables that a data scientist can load directly into a forecasting notebook:

| Table | Grain | Purpose |
|-------|-------|---------|
| `gold_forecast_features_daily` | (index_name, date) | Complete ML feature matrix: lag returns (1d/5d/21d), rolling means (5d/21d), rolling volatility, cyclical calendar encodings (sin/cos), VIX, P/E, P/B |
| `gold_return_distribution_stats` | (index_name, year) | Annual return distributions: mean, stddev, skewness, kurtosis, percentiles (5th/25th/75th/95th), Sharpe proxy |

**Cyclical encodings:** `month_sin = sin(2π × month/12)` and `month_cos = cos(2π × month/12)` ensure that December and January are close in feature space — important for ML models that need to understand seasonal continuity.

---

## Code Organisation

### Pure Functions (Utilities)
The heavy transformation logic lives in standalone Python modules under `utilities/`. Each function takes a Spark DataFrame and returns a Spark DataFrame — no DLT dependencies. This makes them independently testable with a local SparkSession.

| Module | Responsible For |
|--------|----------------|
| `index_common.py` | Shared: normalization, deduplication, daily returns, calendar enrichment |
| `trends_logic.py` | UC1: monthly/yearly aggregations, MoM/YoY/rolling calculations |
| `volatility_logic.py` | UC2: spread, true range, rolling volatility, drawdown, VIX regime |
| `sector_rotation_logic.py` | UC3: monthly returns, benchmark join, ranking, momentum signals |
| `forecast_features_logic.py` | Q4: lag features, rolling means, cyclical encodings, distribution stats |

### DLT Files (Transformations)
DLT files are thin wrappers: they call `dlt.read()` to get input tables, call the pure utility functions, and return the result for DLT to write. No business logic lives in DLT files — it all lives in utilities.

### Tests
~35 pytest unit tests across 5 test files. All tests use synthetic DataFrames (no real data files needed) and a local SparkSession. Critical tests verify that window functions are **always partitioned by `index_name`** — a global window would compute returns across indices (e.g., using NIFTY 50's close as NIFTY BANK's previous close), which is silent and wrong.

---

## Critical Implementation Rule

**Every window function must use `.partitionBy("index_name")`.**

The 13 equity indices are all in one table (`index_daily_silver`). Lag functions, rolling averages, and rolling standard deviations MUST be partitioned by `index_name` — otherwise Spark will use one index's data to compute another's values. This is the single most important correctness invariant in the entire pipeline. Tests specifically verify this.

---

## Known Data Quirks

| Quirk | Files | Handling |
|-------|-------|---------|
| `"-"` placeholder instead of null | NIFTY MICROCAP 250, SMALLCAP 250 | Explicit DOUBLE schema at bronze → becomes null automatically |
| Duplicate date at inception | NIFTY SMALLCAP 250 (2005-04-01) | `dedupe_by_date()` keeps the row with the higher close |
| Zero OHLC rows at inception | Multiple sector indices | `is_zero_ohlc` flag added; silver filters these out; DQ warns but doesn't drop (for audit trail) |
| Different Volume availability | Group B has no Volume | `normalize_index_frame()` adds null Volume/Turnover before the union |
| VIX data ends 2021-07 | INDIAVIX.csv | LEFT join on trade_date; null VIX values after that date are expected and correct |

---

## Implementation Order

1. `utilities/ingestion_config.json` — declare all 14 files
2. `transformations/bronze/data_ingestion.py` — config-driven ingestion
3. `utilities/index_common.py` — shared foundation functions
4. `silver/shared/` — VIX table + unified index table
5. UC1: `trends_logic.py` → silver trends → gold trends KPIs
6. UC2: `volatility_logic.py` → silver volatility → gold volatility KPIs
7. UC3: `sector_rotation_logic.py` → silver sector → gold sector KPIs
8. Q4: `forecast_features_logic.py` → gold forecast features
9. `tests/conftest.py` + all `test_*.py` files

---

## How to Run on Databricks

1. **Upload CSVs** to `/Volumes/nifty_de/raw/ingestion_layer/` in your Databricks workspace
2. **Create the catalog and schemas**: run `CREATE CATALOG nifty_de`, `CREATE SCHEMA nifty_de.bronze`, etc.
3. **Create a DLT Pipeline** pointing to the `transformations/` directory
4. **Start the pipeline** — DLT resolves dependencies automatically and runs Bronze → Silver → Gold in order
5. **Run tests locally**: `pip install pyspark==3.5.3 pytest && pytest` from the `nifty_indices_de/` directory
