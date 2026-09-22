# Zomato Data Engineering & AI Pipeline

An end-to-end batch data platform built on a Zomato-style food delivery dataset — from raw CSV files to AI-powered analytics.

**Pipeline flow:** Raw Data → Amazon S3 → Snowflake → dbt → Airflow → AI Layer (OpenAI)

## Overview

This project simulates a real-world food delivery analytics stack. Data is ingested into an S3 data lake, loaded into Snowflake via a storage integration, and transformed through a medallion architecture using dbt (Bronze → Silver → Gold). Apache Airflow orchestrates the entire pipeline as a single daily DAG. On top of the warehouse, an AI layer built with OpenAI adds LLM-based review enrichment, retrieval-augmented chat over reviews, and natural-language querying of the warehouse. Streamlit powers the interactive apps.

## Dataset

The dataset are CSV files and placed under `data/` in local(excluded from the repo due to size, ~2.3 GB).

## What's Built

| Layer | Platform | Description |
|---|---|---|
| Source | `data/` (local) | 4 dimension tables (restaurants, users, food, menu) + 3 fact tables: 10M orders, ~23M order items, 300K free-text reviews |
| Bronze | Snowflake `ZOMATO.RAW` | Loaded from S3 via `COPY INTO`, using a keyless storage integration |
| Silver | Snowflake `ZOMATO.STAGING` | dbt staging views — cleaned, typed, renamed |
| Gold | Snowflake `ZOMATO.MARTS` | Dimensions, incremental fact tables, business marts, and an SCD2 snapshot |
| AI | Snowflake `ZOMATO.AI` | LLM-enriched reviews (sentiment/topic), RAG chat, text-to-SQL |
| Orchestration | Airflow (Docker) | Single daily DAG: load → transform → enrich → AI mart |

## Tech Stack

Python · Pandas · Amazon S3 · Snowflake · dbt (dbt-snowflake) · Apache Airflow 3 (Docker) · OpenAI (gpt-4o-mini, text-embedding-3-small) · Streamlit

## Repository Structure

```
├── airflow/                  # Airflow 3 on Docker
│   ├── Dockerfile            #   Snowflake + OpenAI providers, dbt in its own venv
│   ├── docker-compose.yaml   #   postgres + api-server + scheduler; creds via env vars
│   ├── example.env           #   template for SNOWFLAKE_* / OPENAI_API_KEY
│   └── dags/zomato_batch.py  #   pipeline DAG (4 tasks)
├── zomato/                   # dbt project
│   ├── models/staging/       #   staging views (Silver) + sources + tests
│   ├── models/marts/         #   dims, incremental facts, business marts (Gold)
│   └── macros/               #   custom schema-name macro
├── ai/                       # AI layer
│   ├── enrich_reviews.py     #   LLM enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   ├── rag_chat.py           #   RAG — chat with reviews (Streamlit)
│   ├── text_to_sql.py        #   text-to-SQL — chat with the warehouse (Streamlit)
│   └── example.env           #   template for AI credentials
├── snowflake/                # Snowflake setup SQL (run in order via Snowsight)
│   ├── 01_setup.sql          #   warehouse, database, schemas, role
│   ├── 02_storage_integration.sql  # keyless S3 ↔ Snowflake link
│   ├── 03_stage_and_formats.sql    # external stage + CSV file format
│   ├── 04_raw_tables.sql     #   RAW (Bronze) table DDL
│   └── 05_copy_into.sql      #   COPY INTO RAW from the stage
├── aws/iam/                  # IAM policies for the S3 ↔ Snowflake handshake
└── docs/architecture.png     # architecture diagram
```

> `data/`, logs, and dbt `target/` artifacts are intentionally excluded from version control — see the Dataset section above.

## How It Works

### 1. Ingestion — Data lands in S3
Source CSVs are uploaded to `s3://<BUCKET>/raw/<table>/`, one folder per table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2. S3 → Snowflake — keyless integration
Snowflake reads directly from the bucket without stored access keys, using a storage integration paired with an IAM role. Configuration lives in `snowflake/02_storage_integration.sql` (Snowflake side) and `aws/iam/` (AWS side):

| File | Purpose |
|---|---|
| `s3-read-policy.json` | Read-only IAM policy scoped to the bucket |
| `snowflake-role-trust-policy-initial.json` | Placeholder trust policy used at role creation |
| `snowflake-role-trust-policy-final.json` | Final trust policy using Snowflake's IAM user ARN + external ID from `DESC INTEGRATION` |

**Setup order matters:** create the IAM policy and role → create the Snowflake storage integration pointing at the role ARN → run `DESC INTEGRATION` to retrieve `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` → update the role's trust policy with those values.

> Two gotchas worth knowing: the trust policy's Principal must be Snowflake's IAM user ARN (not `:root`), and re-running `CREATE OR REPLACE` on the integration regenerates the external ID and silently breaks the trust relationship.

### 3. Load — `COPY INTO`
Table DDL in `snowflake/04_raw_tables.sql` mirrors each CSV's column order. `snowflake/05_copy_into.sql` then loads each file from the external stage into `ZOMATO.RAW` — 10M orders, ~23M order items, 300K reviews.

### 4. Transform — dbt (medallion architecture)
- **Staging (Silver):** one view per source table — normalizing messy fields (e.g. stripping `₹` symbols, converting `--` to null), lowercasing emails, deriving flags like `is_delivered`.
- **Dimensions (Gold):** `dim_restaurants`, `dim_customer` (with age segmentation), `dim_food`, and a generated `dim_date` calendar table.
- **Facts (Gold, incremental):** `fct_orders` and `fact_order_items` use `materialized='incremental'` with a `MERGE` strategy, so reruns only process new rows rather than rebuilding 10M+ records from scratch.
- **Marts (Gold):** business-question-oriented tables — daily city revenue (GMV, AOV, cancellation rate), restaurant performance, delivery SLA percentiles (p50/p90 by city and hour), and review insights.
- **Tests:** `unique`, `not_null`, `relationships`, `accepted_values`, plus a custom reconciliation test. `dbt build` runs models and tests together in dependency order.

### 5. Orchestrate — Airflow
A single daily DAG, `zomato_batch`, runs the full pipeline as one graph:

```
reload_raw  →  dbt_build_core  →  enrich_reviews  →  dbt_build_ai
(COPY from S3)   (dbt build+test)   (OpenAI enrichment)  (AI mart)
```

Credentials are never hardcoded — `docker-compose` injects `SNOWFLAKE_*` environment variables (consumed by dbt's `profiles.yml` via `env_var()`) plus an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection for the load task.

### 6. AI Layer
- **LLM enrichment** (`ai/enrich_reviews.py`): uses the LLM as a transformation step — reads free-text reviews, asks `gpt-4o-mini` for structured JSON (sentiment + topic), and writes results to `ZOMATO.AI.REVIEW_ENRICHED`, which dbt then models into `mart_review_insights`. Idempotent and sample-capped via `SAMPLE_N` to avoid reprocessing costs.
- **RAG chat** (`ai/rag_chat.py`): embeds reviews, retrieves the most relevant ones for a given question, and generates a grounded answer with cited sources.
- **Text-to-SQL** (`ai/text_to_sql.py`): the LLM is given the warehouse's mart schemas, generates Snowflake SQL from a natural-language question, and a `SELECT`-only guard validates the query before it runs under a restricted role.

## Getting Started

```bash
# 1. Snowflake setup — run snowflake/01 through 05 in Snowsight
#    (creates warehouse, database, schemas, role, and the S3 storage integration)
#    See aws/iam/ for the corresponding AWS-side configuration.

# 2. dbt
cd zomato
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai

# 3. Airflow
cd airflow
cp example.env .env          # fill in SNOWFLAKE_*, OPENAI_API_KEY, SAMPLE_N
docker compose build && docker compose up -d
# visit http://localhost:8080 → unpause `zomato_batch` → trigger a run

# 4. AI apps
export OPENAI_API_KEY=sk-...
python ai/enrich_reviews.py
streamlit run ai/rag_chat.py      # chat with reviews
streamlit run ai/text_to_sql.py   # chat with the warehouse
```

## Key Design Decisions

- **Keyless S3 access:** avoided long-lived AWS credentials entirely by using a Snowflake storage integration + IAM role trust relationship.
- **Incremental fact tables:** `MERGE`-based incremental models mean the pipeline scales to daily reruns without reprocessing tens of millions of historical rows.
- **Idempotent AI enrichment:** LLM calls are sample-capped and skip already-enriched rows, keeping OpenAI costs predictable on reruns.
- **Single orchestrated DAG:** the whole pipeline — ingestion, transformation, enrichment, and AI marts — runs as one dependency-ordered Airflow graph rather than disconnected scripts.
