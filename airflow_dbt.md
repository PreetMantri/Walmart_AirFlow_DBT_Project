# Airflow + dbt Orchestration

Describes the Airflow DAG and the dbt models used for the Silver and Gold layers, which run downstream of the Bronze ingestion covered in [`setup.md`](setup.md).

## 1. Overview

High-level steps:

1. Trigger the Databricks Bronze ingestion job from Airflow.
2. Clean previous dbt build artifacts.
3. Run dbt source freshness checks.
4. Build and test the Silver layer.
5. Build the Gold layer using Silver as input.

Orchestrated by the Airflow DAG `orchestrate`.

## 2. Airflow Orchestration Flow

Runs on a daily schedule, in order:

1. **`ingest_cdc`** — Triggers the Databricks Bronze ingestion job (see `setup.md` §6) and waits for it to complete. This job originally ran on its own hourly Databricks schedule; it was added here to demonstrate triggering/monitoring a Databricks job from within an Airflow DAG alongside the dbt steps. Fails the DAG on job failure.
2. **`clean_target`** — Removes the previous dbt `target` and `logs` folders for a clean run.
3. **`source_freshness`** — Runs `dbt source freshness` to validate source data freshness before transformation.
4. **Silver layer:**
   - `silver_technical` → `dbt run --select silver_t`
   - `silver_technical_tests` → `dbt test --select silver_t`
   - `silver_business` → `dbt run --select silver_b`
   - `silver_business_tests` → `dbt test --select silver_b`
5. **Gold layer:**
   - `gold_ephemeral` → `dbt run --select gold/ephemeral`
   - `gold_dimensions` → `dbt snapshot`
   - `gold_facts` → `dbt run --select gold/fact`

## 3. Silver Layer

### 3.1 Silver Technical Models (`silver_t`)

Ingest source data into a structured, cleaned form. Materialized as tables in the `silver_t` schema, validated with dbt tests.

| Model | Description |
|---|---|
| `customers_t` | Incremental model for customer data; adds `processed_at`; refreshes via `updated_timestamp` |
| `orders_t` | Incremental model for order data; adds `processed_at`; refreshes only on newer updates |
| `order_items_t` | Technical staging model for order-item records |
| `products_t` | Technical staging model for product data |
| `employees_t` | Technical staging model for employee data |
| `stores_t` | Technical staging model for store data |

### 3.2 Silver Business Model (`obt_b`)

Built from the technical Silver tables, consolidated into a single **One Big Table (OBT)** model:

- `obt_b` joins `orders_t`, `customers_t`, `order_items_t`, `products_t`, `employees_t`, `stores_t`

Produces a consolidated business view of orders and related entities, suitable for downstream Gold models.

## 4. Gold Layer

Consumes the Silver business model (`obt_b`) to build presentation-ready analytics datasets.

### 4.1 Ephemeral Gold Models (`gold/ephemeral`)

Lightweight intermediate transformations preparing data for downstream Gold logic. Select relevant columns from `obt_b` and add a processing timestamp.

- `eph_orders`, `eph_customers`, `eph_employees`, `eph_products`, `eph_stores`

### 4.2 Gold Fact Model (`gold/fact/fact_orders.sql`)

Fact-style dataset for orders, built from the Silver business layer. Key fields:

- `order_id`, `order_item_id`, `product_id`, `store_id`, `employee_id`, `customer_id`
- `total_amount`, `quantity`, `unit_price`, `line_amount`

## 5. Deployment & Execution

The Airflow stack runs via Docker Compose and mounts the dbt project (`walmart_project`) into the Airflow containers.

**Recommended practices:**

- Store Databricks connection details in Airflow connections or environment variables — never hardcode credentials.
- Keep dbt tests enabled so data quality gates the Silver layer before Gold models build.
- Check the Airflow `logs` directory when troubleshooting task failures.

## 6. End-to-End Summary

1. Databricks Bronze ingestion is triggered from Airflow (`ingest_cdc`).
2. Airflow waits for ingestion to succeed.
3. dbt source freshness is checked.
4. Technical Silver models are built and tested.
5. Business Silver model (`obt_b`) is built.
6. Gold ephemeral models are created.
7. Gold snapshots/dimensions are processed.
8. Gold fact models are built.

Result: a structured path from raw ingested data to curated analytical models ready for reporting and downstream consumption.
