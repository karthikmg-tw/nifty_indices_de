# data-sentinels

## Overview
This project implements an end-to-end Data Engineering pipeline using **PySpark** combined with the power of **Databricks** leveraging **Delta Live Tables**.

## Dataset Description
E-commerce is an ever-growing field often leveraging data analytics to great effect. With this as the primary motivation we have identified the [OList Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) as our source. It contains 9 files covering customers, orders, order items, payments, reviews, products, sellers, geolocation, and category translations.

## Team
- Ravi Teja Boddupally
- Jagadeesh Damarla
- Souvik Karmakar

## Questions answered
As a team we identified three opportunity areas for Olist:
- **Use Case 1** - Revenue & Growth Analytics (Finance pillar)
- **Use Case 2** - Logistics & Delivery Efficiency (Operations pillar)
- **Use Case 3** - Customer Lifetime Value & Segmentation (Marketing pillar)

## Architecture
The pipeline strictly follows the **Medallion Architecture** (Bronze, Silver, Gold), leveraging DLT's declarative Directed Acyclic Graph (DAG) for automated dependency management. Code is logically modularised into separate files based on their respective layers to maintain a clean workspace.

Catalog: `data_sentinals` · Schemas: `bronze`, `silver`, `gold`.

---

## Bronze Layer (Raw Ingestion)
Driven by `utilities/ingestion_config.json` + `transformations/bronze/data_ingestion.py`. Every file creates a Materialized View with `@dlt.expect_all_or_drop` rules from the config, and an added `ingestion_ts` column.

* **`raw_geolocation`** - Geographical coordinates and zip codes across Brazil.
* **`raw_customers`** - Unique customer IDs and their geographic location metadata.
* **`raw_olist_sellers`** - Seller IDs, zip codes, and location details.
* **`raw_orders`** - Core order metadata including order status and purchase timestamps.
* **`raw_order_items`** - Line-item details for each order, including individual item price and freight value.
* **`raw_order_payments`** - Payment methods, installments, and total transaction values per order.
* **`raw_order_reviews`** - Customer reviews, scores, and review timestamps for completed orders.
* **`raw_product_names_translation`** - Maps Portuguese product category names to English.
* **`raw_products`** - Product catalog detailing category and physical dimensions.

---

## Opportunity 1 - Revenue & Growth Analytics (Use Case 1)

Transforms raw order and payment data into a finance-ready Gold layer representing **monthly financial health**. Showcases complex Spark joins, 1-to-many payment handling, and rolling-revenue window functions. Data-quality checks guarantee **no negative payments** and **no "ghost" orders** reach the Gold layer.

### Silver tables (UC1)
* **`payments_clean_silver`** - Cleansed `raw_order_payments`; negative values dropped via DLT expectations, `payment_type` normalised.
* **`payments_order_agg_silver`** - Order-grain payment rollup. Collapses the 1-to-many payment relationship into `total_payment_value`, `n_payments`, `installments_max`, `primary_payment_type`, and `payment_type_mix`.
* **`product_catalog_silver`** - `raw_products` enriched with English category names (`product_category_en`, coalesced to `'unknown'` when missing).
* **`order_items_revenue_silver`** - Line-item revenue enriched with category; exposes `item_total_revenue = price + freight_value`.
* **`orders_revenue_silver`** - Order-grain revenue fact; **inner-joins** orders with items so ghost payments cannot reach Gold. Includes `revenue_total`, `payment_reconciliation_delta`, `order_purchase_month`, `order_purchase_year`, and an `is_revenue_recognised` flag (excludes `canceled` / `unavailable`).

### Gold tables (UC1)
* **`gold_revenue_monthly`** - Monthly trend with MoM %, YoY %, rolling 3-month and 6-month revenue. MoM / YoY are computed via month-based self-joins (`add_months`) so gaps in the timeline do not silently produce NULLs; rolling windows use `Window.rowsBetween`.
* **`gold_revenue_by_category_month`** - Monthly revenue by product category with share-of-month and rank.
* **`gold_revenue_by_state_month`** - Monthly revenue by customer state with AOV, share-of-month and rank.
* **`gold_revenue_by_payment_type_month`** - Monthly revenue split by primary payment type; showcases the 1-to-many handling done in silver.
* **`gold_revenue_exec_summary`** - Single-row KPI snapshot: GMV, AOV, last-12m vs prior-12m revenue, YoY growth %, top category, top state, top payment type.

---

## Opportunity 2 - Logistics & Delivery Efficiency (Use Case 2)

Analyses the "last mile" by computing the variance between estimated and actual delivery dates. The Silver layer cleanses geolocation, handles null delivery timestamps, and adds delivery-bucket flags. The Gold layer identifies regional and corridor bottlenecks.

### Silver tables (UC2)
* **`geo_zip_prefix_silver`**, **`customer_geo_silver`**, **`seller_geo_silver`** - dimensional geo enrichment.
* **`delivery_order_silver`** - Order-grain delivery fact: typed timestamps, `delivery_delta_days`, `is_late`, `delivery_status_bucket`.
* **`order_seller_rollup_silver`**, **`delivery_order_item_silver`** - Order x seller grain for corridor analysis.

### Gold tables (UC2)
* **`gold_delivery_state_month`**, **`gold_delivery_seller_state_month`**, **`gold_delivery_corridor_month`**, **`gold_delivery_hotspots`**, **`gold_delivery_exec_summary`**.

---

## Opportunity 3 - Customer Lifetime Value & Segmentation (Use Case 3)

Leverages the last 12 months of active purchasing behaviour; ranks customers by percentiles on monetary spend and order volume and maps them onto a 3x3 segmentation matrix.

### Silver tables (UC3)
* **`stg_orders_agg`** - Item-level financial data rolled up to order grain.
* **`stg_last_12months_orders`** - Delivered orders in the rolling 12-month window (anchor-date cross-join pattern).
* **`stg_cust_percentile`** - Monetary spend percentile per unique customer with `value_segment`.
* **`stg_cust_vol_agg`** - Order volume per unique customer with `volume_segment`.

### Gold tables (UC3)
* **`customer_segmentation`** - Final personas (`VIP`, `Core Customer`, `Casual Buyer`).

---

## Tests

Unit tests live in `Olist_DE_Practise_Jobs/tests/` and run with a local `SparkSession` (no Databricks dependency). They exercise the pure PySpark helpers in `utilities/revenue_logic.py`.

### One-time setup

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

Java 11 or 17 must be available on `PATH` for local Spark. If you run Java 17, also export the module-open flags it needs:

```bash
export SPARK_LOCAL_IP=127.0.0.1
export JAVA_TOOL_OPTIONS="\
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
```

### Running

```bash
pytest -v
```

Expected: **17 passed**.

## Performance benchmarks

`Olist_DE_Practise_Jobs/Exploration/performance_benchmarks.ipynb` measures the UC1 gold build across different dataset sizes (full / 0.5 / 0.1 sample) and compares the effect of `broadcast()` hinting on the category dim and `cache()` on the orders-revenue fact.

## Exploration notebooks

* `Exploration/revenue_exploration.ipynb` - UC1 strategy notes, silver/gold validation SQL, business-question answers, dashboard guidance.
* `Exploration/JD-Exploration-2.ipynb` - UC3 strategy + validation.
* `Exploration/RaviTeja-Exploration.ipynb` - Personal catalog and manual bronze loads used during development.

## Key Technologies
* **Databricks / Delta Lake** - Scalable data storage and distributed processing.
* **Delta Live Tables (DLT)** - Declarative pipeline orchestration and DAG generation.
* **PySpark** - DataFrame transformations, window functions, and conditional logic.
* **pytest** - Local unit testing of pure PySpark helpers.
