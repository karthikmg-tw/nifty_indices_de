# Nifty Indices Data Engineering Pipeline

A production-grade medallion data pipeline built on **Databricks Delta Live Tables** that ingests 14 raw Nifty Indian stock market index CSV files and transforms them into clean, analytics-ready Delta tables answering four key business questions: market trends, volatility patterns, sector rotation signals, and ML forecast features.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Pipeline Stages](#pipeline-stages)
  - [Bronze — Raw Ingestion](#bronze--raw-ingestion)
  - [Silver — Cleaned & Enriched](#silver--cleaned--enriched)
  - [Gold — KPIs & Analytics](#gold--kpis--analytics)
- [Output Tables](#output-tables)
- [Project Structure](#project-structure)
- [Local Setup & Testing](#local-setup--testing)
- [Databricks Deployment](#databricks-deployment)
- [Design Principles](#design-principles)
- [Data Quirks & Handling](#data-quirks--handling)
- [Known Limitations](#known-limitations)

---

## Overview

This pipeline ingests historical OHLC (Open/High/Low/Close) data for 14 NSE (National Stock Exchange of India) indices — spanning broad market, size, and sector indices — along with India VIX volatility data. It answers four business questions:

| Use Case | Question | Key Output |
|---|---|---|
| **UC1 – Market Trends** | How do market trends evolve over time? | Monthly/yearly returns, YoY growth, valuation metrics |
| **UC2 – Volatility Patterns** | What are volatility patterns across indices? | True range, rolling vol, drawdowns, VIX regimes |
| **UC3 – Sector Rotation** | How do sectors perform relative to the benchmark? | Relative strength, momentum signals, leaders/laggards |
| **Q4 – Forecast Features** | What historical patterns support forecasting? | Lag features, rolling stats, cyclical encodings, return distributions |

---

## Architecture

```
Raw CSVs (14 files)
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE  (nifty_de.bronze)                                      │
│  Config-driven ingestion — explicit schemas, DQ drop rules       │
│  14 raw_* tables (one per index)                                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER  (nifty_de.silver)                                       │
│                                                                  │
│  Shared Foundation                                               │
│  ├── vix_daily_silver         (VIX — separate schema)           │
│  └── index_daily_silver       (13 equity indices unified)        │
│                                                                  │
│  Use-Case-Specific                                               │
│  ├── index_trends_silver      (UC1 — returns + calendar)        │
│  ├── index_volatility_silver  (UC2 — volatility + VIX join)     │
│  └── sector_monthly_returns_silver  (UC3 — monthly aggregation) │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD  (nifty_de.gold)                                           │
│                                                                  │
│  UC1 – Trends                UC2 – Volatility                   │
│  ├── monthly_trends          ├── rolling_volatility              │
│  ├── yearly_trends           ├── volatility_monthly_summary      │
│  └── trends_exec_summary     └── volatility_exec_summary         │
│                                                                  │
│  UC3 – Sector Rotation       Q4 – Forecast Features             │
│  ├── sector_monthly_perf     ├── forecast_features_daily         │
│  ├── sector_leaders_laggards └── return_distribution_stats       │
│  └── sector_exec_summary                                         │
└─────────────────────────────────────────────────────────────────┘
```

**Key design separation:** All business logic lives in `utilities/` as pure PySpark functions (DataFrame-in / DataFrame-out). The `transformations/` layer contains thin DLT wrappers that read from the catalog and delegate to utilities. This makes every transformation independently unit-testable without Databricks or real data.

---

## Tech Stack

| Component | Technology |
|---|---|
| Pipeline orchestration | Databricks Delta Live Tables (DLT) |
| Storage format | Delta Lake (ACID, time-travel) |
| Compute | Apache Spark 3.5.3 (PySpark) |
| Catalog & governance | Unity Catalog (`nifty_de` catalog) |
| Local testing | pytest 8.3.2 + local SparkSession |
| Language | Python 3.x |

---

## Data Sources

14 CSV files uploaded manually to a Databricks Volume at `/Volumes/nifty_de/raw/ingestion_layer/`.

### Index Groupings

**Group A — Broad Market Indices** (Date, Open, High, Low, Close, Volume, Turnover, P/E, P/B, Div Yield)

| File | Start Year | Notes |
|---|---|---|
| NIFTY 100.csv | 2000 | Top 100 companies by free-float market cap |
| NIFTY 500.csv | 2000 | Top 500 companies |

**Group B — Size Indices** (Date, Open, High, Low, Close, P/E, P/B, Div Yield — *no Volume/Turnover*)

| File | Start Year | Notes |
|---|---|---|
| NIFTY 50.csv | 2000 | Benchmark index — 50 largest companies |
| NIFTY NEXT 50.csv | 2005 | Ranks 51–100 |
| NIFTY MIDCAP 150.csv | 2005 | Mid-cap segment |
| NIFTY SMALLCAP 250.csv | 2005 | Small-cap segment; has duplicate inception row |
| NIFTY MICROCAP 250.csv | 2009 | Micro-cap; contains "-" placeholders for null values |

**Group C — Sector Indices** (Date, Open, High, Low, Close, Volume, Turnover, P/E, P/B, Div Yield)

| File | Sector | Start Year | Notes |
|---|---|---|---|
| NIFTY AUTO.csv | Automotive | 2000 | |
| NIFTY BANK.csv | Banking | 2000 | Some zero-OHLC inception rows |
| NIFTY FMCG.csv | Consumer Goods | 2008 | |
| NIFTY IT.csv | Technology | 2008 | |
| NIFTY METAL.csv | Metals & Mining | 2008 | |
| NIFTY PHARMA.csv | Pharmaceuticals | 2008 | |

**Volatility Index**

| File | Start Year | End Year | Notes |
|---|---|---|---|
| INDIAVIX.csv | 2009 | 2021 | Separate schema (Previous, Change, %Change columns) |

---

## Pipeline Stages

### Bronze — Raw Ingestion

**Files:** `transformations/bronze/data_ingestion.py`, `utilities/ingestion_config.json`

The bronze layer is entirely config-driven. `ingestion_config.json` defines all 14 files with their volume paths, explicit column schemas, and data quality rules. A loop over this config dynamically generates all 14 DLT table definitions at pipeline startup — no copy-paste repetition.

**Per-table transformations:**
- Read CSV from Volume with an explicit schema (prevents header inference issues and normalises column names like `Div Yield %` → `DivYield`)
- Add `ingestion_ts` audit timestamp
- Apply `@dlt.expect_all_or_drop` rules: `Date IS NOT NULL` AND `Close > 0`
- Raise error if the resulting table is empty (catches upload failures early)

**Output:** 14 tables in `nifty_de.bronze` — `raw_nifty50`, `raw_niftynext50`, `raw_nifty100`, ..., `raw_indiavix`

---

### Silver — Cleaned & Enriched

#### Shared Foundation Tables

**`vix_daily_silver`** — `transformations/silver/shared/00_silver_vix.py`

Reads `raw_indiavix`. Casts the date string to `DATE`, renames all columns to snake_case, and applies DQ rules (valid date, positive close, VIX range 8–100). This table is a dimension used by UC2 volatility and Q4 forecast features via a LEFT join on `trade_date`.

**`index_daily_silver`** — `transformations/silver/shared/01_silver_index_daily.py`

The single source of truth for all three use cases. Reads all 13 equity bronze tables and unifies them through `normalize_index_frame()` (in `utilities/index_common.py`):

1. Cast `Date` string → `trade_date DATE`
2. Rename PE/PB/DivYield columns to snake_case
3. Add `index_name` (e.g., `"NIFTY 50"`) and `index_category` (`"broad"`, `"size"`, `"sector"`) discriminators
4. Add `is_zero_ohlc` flag — `True` when OHLC = 0 (identifies inception stub rows)
5. Group B files: add null `Volume` and `Turnover` columns to match Group A/C schema
6. Union all 13 via `reduce()`
7. Deduplicate `(index_name, trade_date)` — keep row with higher `close` (handles the known NIFTY SMALLCAP 250 duplicate at 2005-04-01)
8. DQ: drop rows with null date, non-positive close, or null index_name; warn on OHLC sanity violations

#### Use-Case Silver Tables

**`index_trends_silver`** — `transformations/silver/trends/02_silver_trends.py`

Source: `index_daily_silver` (zero-OHLC rows excluded). Enriches each trading day with:
- `prev_close` and `daily_return_pct` via a lag window **partitioned by `index_name`**
- Calendar dimensions: `trade_year`, `trade_month`, `trade_quarter`, `trade_week`, `day_of_week`, `is_month_end`, `is_quarter_end`

DQ: return sanity bounds −30% to +30% (drops extreme outliers).

**`index_volatility_silver`** — `transformations/silver/volatility/03_silver_volatility.py`

Source: `index_daily_silver` LEFT JOIN `vix_daily_silver` on `trade_date`. Computes per-trading-day volatility metrics:

| Metric | Formula |
|---|---|
| `high_low_spread_pct` | `(high - low) / prev_close × 100` |
| `true_range` | `max(high−low, │high−prev_close│, │low−prev_close│)` — accounts for overnight gaps |
| `rolling_vol_21d` | Rolling 21-day stddev of `daily_return_pct` |
| `rolling_vol_63d` | Rolling 63-day stddev of `daily_return_pct` |
| `rolling_52wk_high` | Max `close` over trailing 252 trading days |
| `drawdown_pct` | `(close − rolling_52wk_high) / rolling_52wk_high × 100` (always ≤ 0) |
| `vix_regime` | `"low"` (<15), `"moderate"` (15–25), `"elevated"` (25–40), `"extreme"` (>40) |

All window functions partition by `index_name`.

**`sector_monthly_returns_silver`** — `transformations/silver/sector_rotation/04_silver_sector_rotation.py`

Source: `index_daily_silver` filtered to `sector` and `broad` categories. Aggregates to a monthly grain:
- `month_open` = close of first trading day in month; `month_close` = close of last trading day
- `monthly_return_pct` = `(month_close − month_open) / month_open × 100`
- Joins NIFTY 50 as benchmark to compute `relative_strength` = sector return − benchmark return
- `sector_rank_in_month` = dense rank of monthly returns (1 = best performer that month)

---

### Gold — KPIs & Analytics

#### UC1 — Market Trends (`transformations/gold/05_gold_trends_kpis.py`)

| Table | Grain | Key Metrics |
|---|---|---|
| `gold_index_monthly_trends` | (index_name, month) | `monthly_return_pct`, MoM/YoY return, rolling 3M/6M returns, avg P/E, P/B, Div Yield |
| `gold_index_yearly_trends` | (index_name, year) | `annual_return_pct`, YoY, best/worst monthly return in year, avg valuation |
| `gold_trends_exec_summary` | 1 row | Best/worst YTD index + return, NIFTY 50 current P/E & P/B, `data_through_date` |

#### UC2 — Volatility Patterns (`transformations/gold/06_gold_volatility_kpis.py`)

| Table | Grain | Key Metrics |
|---|---|---|
| `gold_index_rolling_volatility` | (index_name, date) | Full daily fact: `rolling_vol_21d/63d`, `drawdown_pct`, `true_range`, `vix_close`, `vix_regime` |
| `gold_volatility_monthly_summary` | (index_name, month) | Avg true range, monthly vol stddev, max drawdown in month, avg VIX, dominant VIX regime, `instability_flag` |
| `gold_volatility_exec_summary` | 1 row | Most/least volatile index (21d), current VIX regime, highest-ever VIX + date, max NIFTY 50 drawdown ever |

`instability_flag` = `True` when a month's return stddev exceeds the 90th percentile for that index — a market stress signal.

#### UC3 — Sector Rotation (`transformations/gold/07_gold_sector_rotation_kpis.py`)

| Table | Grain | Key Metrics |
|---|---|---|
| `gold_sector_monthly_performance` | (index_name, month) | `monthly_return_pct`, `relative_strength`, rolling RS 3M/6M, `momentum_signal`, `sector_rank_in_month` |
| `gold_sector_leaders_laggards` | (month, position) | Top 3 leaders + bottom 3 laggards per month, labelled `leader_1`…`laggard_3` — chart-ready |
| `gold_sector_exec_summary` | 1 row | Current leading/lagging sector, best/worst sector YTD |

**Momentum signal quadrant classification:**

| Condition | Signal |
|---|---|
| rolling RS 3M > 0 AND monthly return > 0 | `leading` |
| rolling RS 3M > 0 AND monthly return ≤ 0 | `deteriorating` |
| rolling RS 3M ≤ 0 AND monthly return > 0 | `recovering` |
| rolling RS 3M ≤ 0 AND monthly return ≤ 0 | `lagging` |

#### Q4 — Forecast Features (`transformations/gold/08_gold_forecast_features.py`)

| Table | Grain | Key Features |
|---|---|---|
| `gold_forecast_features_daily` | (index_name, date) | `lag_return_1d/5d/21d`, `rolling_mean_5d/21d`, `rolling_vol_21d/63d`, `high_low_spread_pct`, `month_sin/cos`, `dow_sin/dow_cos`, `vix_close`, `pe`, `pb`, `div_yield` |
| `gold_return_distribution_stats` | (index_name, year) | Mean/stddev/skewness/kurtosis, percentiles (5th/25th/75th/95th), `pct_positive_days`, Sharpe proxy |

Cyclical encodings (`sin`/`cos`) are used for month (1–12) and day-of-week (1–5) so that December and January are adjacent in feature space — standard practice for time-series models.

---

## Output Tables

```
nifty_de.bronze
├── raw_nifty50
├── raw_niftynext50
├── raw_niftymidcap150
├── raw_niftysmallcap250
├── raw_niftymicrocap250
├── raw_nifty100
├── raw_nifty500
├── raw_niftyauto
├── raw_niftybank
├── raw_niftyfmcg
├── raw_niftyit
├── raw_niftymetal
├── raw_niftypharma
└── raw_indiavix

nifty_de.silver
├── vix_daily_silver
├── index_daily_silver
├── index_trends_silver
├── index_volatility_silver
└── sector_monthly_returns_silver

nifty_de.gold
├── gold_index_monthly_trends
├── gold_index_yearly_trends
├── gold_trends_exec_summary
├── gold_index_rolling_volatility
├── gold_volatility_monthly_summary
├── gold_volatility_exec_summary
├── gold_sector_monthly_performance
├── gold_sector_leaders_laggards
├── gold_sector_exec_summary
├── gold_forecast_features_daily
└── gold_return_distribution_stats
```

---

## Project Structure

```
de-course-final-project/
├── dataset/                                    # Raw CSV files (14 indices)
│   ├── NIFTY 50.csv
│   ├── NIFTY 100.csv
│   ├── NIFTY 500.csv
│   ├── NIFTY NEXT 50.csv
│   ├── NIFTY MIDCAP 150.csv
│   ├── NIFTY SMALLCAP 250.csv
│   ├── NIFTY MICROCAP 250.csv
│   ├── NIFTY AUTO.csv
│   ├── NIFTY BANK.csv
│   ├── NIFTY FMCG.csv
│   ├── NIFTY IT.csv
│   ├── NIFTY METAL.csv
│   ├── NIFTY PHARMA.csv
│   └── INDIAVIX.csv
│
└── nifty_indices_de/                           # Main package
    ├── transformations/                        # DLT pipeline definitions
    │   ├── bronze/
    │   │   └── data_ingestion.py               # Config-driven ingestion (14 tables)
    │   ├── silver/
    │   │   ├── shared/
    │   │   │   ├── 00_silver_vix.py            # VIX foundation table
    │   │   │   └── 01_silver_index_daily.py    # Unified 13-index table
    │   │   ├── trends/
    │   │   │   └── 02_silver_trends.py         # UC1: returns + calendar dims
    │   │   ├── volatility/
    │   │   │   └── 03_silver_volatility.py     # UC2: volatility metrics + VIX join
    │   │   └── sector_rotation/
    │   │       └── 04_silver_sector_rotation.py # UC3: monthly aggregation + benchmark
    │   └── gold/
    │       ├── 05_gold_trends_kpis.py          # UC1: monthly/yearly trend KPIs
    │       ├── 06_gold_volatility_kpis.py      # UC2: volatility KPIs
    │       ├── 07_gold_sector_rotation_kpis.py # UC3: sector rotation KPIs
    │       └── 08_gold_forecast_features.py    # Q4: ML feature tables
    │
    ├── utilities/                              # Pure PySpark business logic
    │   ├── ingestion_config.json               # 14-file manifest (paths, schemas, DQ rules)
    │   ├── index_common.py                     # normalize, dedupe, returns, calendar dims
    │   ├── trends_logic.py                     # Monthly/yearly aggregation logic
    │   ├── volatility_logic.py                 # Spread, true range, rolling vol, regimes
    │   ├── sector_rotation_logic.py            # Monthly returns, ranking, momentum signals
    │   └── forecast_features_logic.py          # Lag features, rolling means, seasonality
    │
    ├── tests/                                  # Unit tests (~35 tests, no Databricks required)
    │   ├── conftest.py                         # Session-scoped SparkSession fixture
    │   ├── test_index_common.py
    │   ├── test_trends_logic.py
    │   ├── test_volatility_logic.py
    │   ├── test_sector_rotation_logic.py
    │   └── test_forecast_features_logic.py
    │
    ├── pytest.ini
    └── requirements-dev.txt                    # pyspark==3.5.3, pytest==8.3.2
```

---

## Local Setup & Testing

All business logic in `utilities/` is testable locally without Databricks or the actual CSV files. Tests use a local `SparkSession` and synthetic DataFrames.

```bash
# Install dependencies
cd nifty_indices_de
pip install -r requirements-dev.txt

# Run all tests
pytest

# Run a specific test module
pytest tests/test_volatility_logic.py -v
```

Test coverage spans:
- `normalize_index_frame()` — schema normalisation, category tagging, zero-OHLC flagging
- `dedupe_by_date()` — duplicate detection, keep-higher-close logic
- `compute_daily_returns()` — lag window correctness, cross-index isolation
- All rolling metric computations
- Momentum signal quadrant classification
- Cyclical calendar encodings
- Distribution statistics

---

## Databricks Deployment

### Prerequisites

1. A Databricks workspace with Unity Catalog enabled
2. A cluster running Databricks Runtime 13.x or later (includes Spark 3.5)

### Step 1 — Create Catalog and Schemas

```sql
CREATE CATALOG IF NOT EXISTS nifty_de;
CREATE SCHEMA IF NOT EXISTS nifty_de.bronze;
CREATE SCHEMA IF NOT EXISTS nifty_de.silver;
CREATE SCHEMA IF NOT EXISTS nifty_de.gold;
```

### Step 2 — Create Volume and Upload CSVs

```sql
CREATE VOLUME IF NOT EXISTS nifty_de.raw.ingestion_layer;
```

Upload all 14 CSV files from `dataset/` to:
```
/Volumes/nifty_de/raw/ingestion_layer/
```

### Step 3 — Configure DLT Pipeline

Create a new Delta Live Tables pipeline in the Databricks UI with:

| Setting | Value |
|---|---|
| Source code | Point to `nifty_indices_de/transformations/` directory |
| Target catalog | `nifty_de` |
| Pipeline mode | Triggered (or Continuous for streaming) |
| Cluster policy | Use default or your org's standard |

### Step 4 — Run

Trigger the pipeline. DLT resolves all table dependencies automatically and executes in this order:

```
Bronze (14 tables, parallel)
  → Silver shared (VIX + index_daily, parallel)
    → Silver UC-specific (trends, volatility, sector, parallel)
      → Gold (10 tables across 4 use cases, parallel within each UC)
```

Total run time depends on cluster size and data volume. On a single-node cluster, expect 10–20 minutes for the full pipeline.

---

## Design Principles

### 1. Pure Function / DLT Wrapper Separation

Every transformation is split into two layers:
- **`utilities/`** — pure Python functions: `(DataFrame, ...) → DataFrame`. No DLT imports, no Spark context required at test time.
- **`transformations/`** — DLT wrapper: reads from catalog via `dlt.read()`, calls the utility function, returns the result for DLT to write.

This makes every unit testable with a local SparkSession and synthetic data.

### 2. Mandatory Window Partitioning

All 13 equity indices share a single silver table (`index_daily_silver`). Every window function — lag, rolling stddev, rolling max — **must** partition by `index_name`. Without this, Spark silently computes metrics across index boundaries, producing nonsensical returns (e.g., a lag from NIFTY BANK's last row into NIFTY IT's first row). Tests explicitly verify this invariant.

### 3. LEFT Join on VIX

VIX data ends in July 2021. Post-2021 rows in the volatility and forecast tables must retain `null` VIX values — not be dropped. All VIX joins use LEFT (not INNER) join semantics.

### 4. Cyclical Temporal Encodings

Month (1–12) and day-of-week (1–5) use sin/cos encoding for the forecast feature table:

```python
month_sin = sin(2π × trade_month / 12)
month_cos = cos(2π × trade_month / 12)
```

This ensures temporal continuity: December and January are close in feature space, as they should be for seasonal models.

### 5. Config-Driven Bronze Ingestion

All 14 files are described in a single JSON config (`ingestion_config.json`). Adding a new index requires only a new entry in the config — no code changes. Explicit schemas prevent header inference bugs and normalise inconsistent column names across file groups.

### 6. Exec Summary Single-Row Pattern

Each use case produces a one-row executive summary table (`*_exec_summary`). In DLT, aggregating to a single row requires care to avoid graph initialisation issues. The pattern used here is a dummy-key broadcast join after aggregation, which is safe during DLT graph init and produces a guaranteed single row.

---

## Data Quirks & Handling

| Quirk | Affected Index | Handling |
|---|---|---|
| `"-"` placeholder for nulls | NIFTY MICROCAP 250, NIFTY SMALLCAP 250 | Declared as `DOUBLE` in explicit bronze schema — Spark converts `"-"` to `null` automatically |
| Duplicate inception row (2005-04-01) | NIFTY SMALLCAP 250 | Deduplicated in `index_daily_silver` — keep row with higher `close` |
| Zero-OHLC inception rows (Open=High=Low=0) | NIFTY BANK, NIFTY IT, others | `is_zero_ohlc` flag set in `index_daily_silver`; filtered out in UC-specific silver tables, preserved in shared table for audit |
| Group B has no Volume/Turnover | NIFTY 50, NEXT 50, MIDCAP, SMALLCAP, MICROCAP | Null `volume` and `turnover` columns added before union with Group A/C |
| VIX data ends July 2021 | INDIAVIX | All joins to VIX use LEFT join; null VIX values post-2021 are expected and documented |
| Inception rows with same-day data | Several sector indices | Handled by deduplication logic in `index_daily_silver` |

---

## Known Limitations

- **VIX ends 2021:** No India VIX data is available after July 2021. The `vix_close` and `vix_regime` columns in volatility and forecast tables will be `null` for post-2021 dates.
- **Group B valuation metrics:** P/E, P/B, and Div Yield are available for size indices but Volume and Turnover are not — liquidity-based analysis is limited to Group A/C.
- **NIFTY MICROCAP 250 starts 2009:** Any cross-index comparison requiring data before 2009 will have incomplete coverage for this index.
- **Monthly grain in UC3:** Sector rotation analysis is at monthly granularity. Intra-month rotation signals are not captured.
- **Static CSVs:** The pipeline is designed for batch ingestion of static historical files. Real-time or incremental updates would require adjustments to the bronze ingestion config and DLT pipeline mode.
