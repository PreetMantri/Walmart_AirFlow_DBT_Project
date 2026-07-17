# Walmart Airflow + dbt Data Pipeline

End-to-end data pipeline that ingests Walmart-style operational data into a Postgres (Ghost) database, lands it in Databricks Bronze, transforms it through dbt Silver/Gold layers, and orchestrates the whole flow with Airflow.

## Architecture

```
CSV files → Ghost (PostgreSQL) → Databricks Bronze → dbt Silver → dbt Gold
                                        ▲
                                        │
                                    Airflow DAG (orchestrate)
```

- **Source**: CSV files loaded into a PostgreSQL instance ("Ghost")
- **Ingestion**: Databricks job reads from Ghost via JDBC into Bronze staging tables
- **Transformation**: dbt builds Silver (technical + business/OBT) and Gold (ephemeral, dimensions, facts) layers
- **Orchestration**: Airflow DAG triggers the Databricks ingestion job and runs the full dbt build/test sequence

## Ingestion history / design note

Bronze ingestion originally ran as a standalone **hourly scheduled Databricks job** (independent of Airflow). After the Airflow + dbt orchestration layer was added, that same ingestion job was wired into the DAG as its first task (`ingest_cdc`) — this was done to demonstrate triggering and monitoring a Databricks job directly from Airflow, alongside the dbt build steps. The ingestion logic itself (append-based load from Ghost) is unchanged; only the trigger mechanism moved from a Databricks-native schedule to an Airflow-orchestrated call.

## Repository Structure

```
.
├── README.md                          # this file
├── docs/
│   ├── setup.md                       # Postgres (Ghost) + Databricks Bronze setup
│   └── airflow_dbt.md                 # Airflow DAG + dbt Silver/Gold layers
├── walmart_dataset/
│   ├── ddl/
│   │   └── walmart_schema.sql         # table creation script
│   ├── load_data.py                   # CSV -> Postgres loader
│   └── data/                          # source CSV files
├── walmart_project/                   # dbt project (Silver + Gold models)
└── connection                         # Ghost PostgreSQL connection string
```

## Quick Start

1. Follow [`docs/setup.md`](setup.md) to stand up Ghost (Postgres) and load source data, then ingest into Databricks Bronze.
2. Follow [`docs/airflow_dbt.md`](airflow_dbt.md) to run the Airflow-orchestrated dbt Silver/Gold build.

## Prerequisites

- Python 3.10+, `psycopg2-binary`
- Access to Ghost PostgreSQL instance
- Databricks Workspace + cluster/SQL warehouse
- Docker Compose (for Airflow)
