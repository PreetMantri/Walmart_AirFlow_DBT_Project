# Setup: Ghost (PostgreSQL) → Databricks Bronze

This document covers standing up the Ghost PostgreSQL database, loading source CSVs, and ingesting into the Databricks Bronze layer.

## 1. Connect to the Ghost Database

Use the connection string stored in the `connection` file and copy it into the `conn_string` variable inside `walmart_dataset/load_data.py` before running the loader.

To run the SQL manually instead, use `psql` with the same connection string:

```bash
psql "<your_connection_string>" -f walmart_dataset/ddl/walmart_schema.sql
```

## 2. Create the Database Schema

Run the SQL schema file to create the tables:

- `customers`
- `stores`
- `products`
- `employees`
- `orders`
- `order_items`

The schema file defines the table structure and columns for each entity.

## 3. Load the CSV Data into Ghost

`walmart_dataset/load_data.py` reads CSV files from `walmart_dataset/data/` and imports them into PostgreSQL using the `COPY` command.

**Notes:**
- The script expects CSV files to be available in the folder it's pointed to (`data_dir`).
- The loader maps to `raw.*` schema names (`raw.customers`, `raw.stores`, etc.). If using the schema as written in `walmart_schema.sql`, ensure target tables exist under that same schema name, or update the loader's table names accordingly.

**Run:**

```bash
python walmart_dataset/load_data.py
```

## 4. Databricks Connection to Ghost

Configure a Databricks connection to the Ghost PostgreSQL instance using:

- a JDBC connection string or Databricks SQL warehouse connection
- Ghost hostname, database name, port, username, password
- credentials stored via Databricks secrets (not hardcoded)

```
jdbc:postgresql://<ghost-host>:5432/<database-name>?sslmode=require
```

## 5. Bronze Layer in Databricks

Organize Bronze under the Walmart catalog:

```
catalog: walmart
schema:  bronze
```

Staging tables, one per source table:

- `bronze.stg_customers`
- `bronze.stg_stores`
- `bronze.stg_products`
- `bronze.stg_employees`
- `bronze.stg_orders`
- `bronze.stg_order_items`

**Example ingestion pattern:**

```sql
CREATE OR REPLACE TABLE walmart.bronze.stg_customers AS
SELECT *
FROM jdbc('<ghost_jdbc_connection>', 'public.customers');
```

## 6. Bronze Ingestion Job

> **Design note:** this job originally ran on a standalone hourly Databricks schedule. It's now triggered as the first step of the Airflow DAG (see [`airflow_dbt.md`](airflow_dbt.md)) — the ingestion logic below is unchanged, only the trigger moved from Databricks' own scheduler to Airflow.

**Pipeline logic:**

1. Read from Ghost via the Databricks JDBC connection.
2. Load the latest available records into Bronze staging tables.
3. Append (or merge, depending on update strategy) into the Walmart Bronze schema.

**Example notebook logic:**

```python
for table in [
    'customers',
    'stores',
    'products',
    'employees',
    'orders',
    'order_items'
]:
    df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"public.{table}").load()
    df.write.mode("append").saveAsTable(f"walmart.bronze.stg_{table}")
```

> Note: this append pattern loads the latest full/incremental extract each run; it is not row-level CDC (no update/delete capture or merge-based upsert). "CDC" in the Airflow task name refers to the job's role as the change-ingestion trigger point in the DAG, not to log-based CDC semantics. If true CDC (updates/deletes) is needed, switch to a merge/upsert pattern keyed on primary key + a watermark column.

## 7. Verify the Load

**Ghost (Postgres):**

```sql
SELECT COUNT(*) FROM raw.customers;
SELECT COUNT(*) FROM raw.orders;
SELECT COUNT(*) FROM raw.order_items;
```

**Databricks Bronze:**

```sql
SELECT COUNT(*) FROM walmart.bronze.stg_customers;
SELECT COUNT(*) FROM walmart.bronze.stg_orders;
SELECT COUNT(*) FROM walmart.bronze.stg_order_items;
```

## Summary

1. Use the connection string from the `connection` file.
2. Create the schema using `walmart_schema.sql`.
3. Update the loader script with the correct connection string and data path.
4. Run the Python loader to import CSVs into Ghost.
5. Connect Databricks to Ghost.
6. Ingest the six source tables into Walmart Bronze via staging tables.
7. Trigger the ingestion job — either on its original hourly schedule or via the Airflow DAG.
