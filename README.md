# Retail Lakehouse — End-to-End Data Platform

An end-to-end retail analytics platform built on the Databricks Lakehouse, implementing the **medallion architecture** (Bronze → Silver → Gold) with **Lakeflow Spark Declarative Pipelines (SDP)**, streaming CDC ingestion, SCD Type 2 history tracking, an AI/BI semantic layer, dashboards, and a Genie conversational analytics agent.

## Architecture

```
External Sources
├── Blob storage (CSV)      ──Auto Loader──▶  retail_q.blob_bronze.transactions
├── Salesforce (CDC events) ──pipeline: salesforce_ingestion──▶  retail_q.salesforce_bronze.{account, opportunity}
└── PostgreSQL (CDC events) ──pipeline: postgres_ingestion──▶   retail_q.postgres_bronze.{product_catalog, inventory}
        │
        │  Delta Change Data Feed (CDF)
        ▼
Lakeflow Spark Declarative Pipeline — Retail_Transformation_Pipeline
│  bronze_to_silver/  ── single-layer streaming tables via Auto CDC
│      account.py, product_catalog.py, inventory.py, opportunity.py, transactions.py
│  silver_to_gold/    ── gold fact
│      fact_sales.py
        ▼
Gold layer — retail_q.retail_gold.{fact_sales, fact_inventory, dim_product, dim_customer, dim_calendar}
        ▼
Semantic layer — retail_q.retail_semantic.retail_metric (AI/BI metric view)
        ▼
Consumption
├── AI/BI Dashboard — "Retail Sales Analysis" (6 pages)
└── Genie space — "Retail Sales and Inventory Analytics" (natural-language Q&A)

Orchestration: Retail_Master_Job (Lakeflow Job, daily 06:00 IST) runs the flow
end-to-end: ingestion pipelines → transformation → calendar → gold views →
semantic layer → dashboard refresh. Every resource — pipelines, job, and
dashboard — is created and managed by the Declarative Automation Bundle
(databricks.yml) and deployed from this repo via GitHub Actions CI/CD
(push to main → dev, version tag → prod).
```

## Repository Structure

```
retail-lakehouse-project/            ← GitHub repo root — DAB source of truth
├── README.md                        ← this file — full project guide
├── databricks.yml                   ← Declarative Automation Bundle (dev + prod targets)
├── resources/                       ← DAB resource definitions
│   ├── salesforce_ingestion.pipeline.yml
│   ├── postgres_ingestion.pipeline.yml
│   ├── retail_transformation_pipeline.pipeline.yml
│   ├── retail_master_job.job.yml
│   └── retail_dashboard.dashboard.yml
├── .github/workflows/deploy.yml     ← CI/CD: PR → validate, main → dev, tag → prod
├── bronze_to_silver/                ← SDP source: bronze → silver (details: bronze_to_silver/README.md)
│   ├── README.md
│   ├── account.py                   ← SCD Type 2 (history)
│   ├── product_catalog.py           ← SCD Type 2 (history)
│   ├── inventory.py                 ← SCD Type 1 (latest state)
│   ├── opportunity.py               ← SCD Type 1 (latest state)
│   └── transactions.py              ← append-only streaming table
├── silver_to_gold/                  ← details: silver_to_gold/README.md
│   ├── README.md
│   └── fact_sales.py                ← gold fact (transactions × opportunity)
├── Retail_Project/                  ← supporting notebooks (details: Retail_Project/README.md)
│   ├── README.md
│   ├── 01_blob_to_bronze            ← Auto Loader CSV ingestion to bronze
│   ├── 02_Gold_views                ← gold dimension / fact views
│   ├── 03_calendar                  ← dim_calendar generator (widget-driven date range)
│   ├── 04_Metric_view               ← semantic metric view
│   └── Test                         ← bronze change simulation (MERGE INTO)
└── analytics/
    └── Retail Sales Analysis.lvdash.json   ← AI/BI dashboard definition (Genie space is a workspace asset — see §5)
```

> This repository is the single source of truth for the whole platform. The Declarative Automation Bundle (DAB) deploys every resource — three pipelines, the master job, and the dashboard — directly from this repo into the workspace (dev and prod targets), so nothing is created or maintained by hand in the UI.

## 1. Bronze Layer — Ingestion

Three bronze schemas in the `retail_q` catalog, one per source system. All bronze tables are Delta tables with **Change Data Feed (CDF) enabled** so downstream consumers read change events as a stream.

| Schema | Table | Source | Ingestion |
|---|---|---|---|
| `retail_q.blob_bronze` | `transactions` | CSV files at `/Volumes/retail_q/volumes/blob_source/transaction_source/` | Auto Loader (`cloudFiles`), triggered `availableNow`; schema evolution `addNewColumns`; `_ingested_at` / `_source_file` metadata |
| `retail_q.salesforce_bronze` | `account` | Salesforce (CDC) | Managed ingestion pipeline `salesforce_ingestion` (streaming table, CDF) |
| `retail_q.salesforce_bronze` | `opportunity` | Salesforce (CDC) | Managed ingestion pipeline `salesforce_ingestion` (streaming table, CDF) |
| `retail_q.postgres_bronze` | `product_catalog` | PostgreSQL (CDC) | Managed ingestion pipeline `postgres_ingestion` (streaming table, CDF) |
| `retail_q.postgres_bronze` | `inventory` | PostgreSQL (CDC) | Managed ingestion pipeline `postgres_ingestion` (streaming table, CDF) |

Source changes (inserts / updates / deletes) arrive as CDF events and are simulated in this environment with `MERGE INTO` statements (see `Retail_Project/Test`).

## 2. Silver Layer — Single-Layer Streaming Tables (Auto CDC)

Implemented in the Lakeflow Spark Declarative Pipeline **`Retail_Transformation_Pipeline`** (created and managed by the bundle; serverless compute, Photon enabled, channel `CURRENT`, triggered mode).

### Design pattern

Each silver dataset is a **single streaming table** built with a 3-step pattern:

1. **`@dp.temporary_view()`** — streams the bronze table's **Change Data Feed** (`readChangeFeed=true`) and applies **all business transformations in-flight** (standardization, computed columns, casts, defaults, `processed_at` timestamp).
2. **`dp.create_streaming_table()`** — the silver table itself, liquid-clustered on its primary key, with data-quality **expectations attached at ingestion** (fail / drop / warn levels).
3. **`dp.create_auto_cdc_flow()`** — merges the transformed change events into the table by key: **SCD Type 2** (full history with `__START_AT` / `__END_AT`) where history is required, **SCD Type 1** (latest state, in-place overwrite) otherwise. Deletes are propagated via `apply_as_deletes` on `_change_type = 'delete'`.

### Dataset inventory

| Silver table | Pattern | SCD | Keys | Sequenced by | Clustered on |
|---|---|---|---|---|---|
| `retail_silver.account` | Auto CDC | **Type 2** | `id` | `SystemModstamp` | `id` |
| `retail_silver.product_catalog` | Auto CDC | **Type 2** | `product_id` | `updated_at` | `product_id` |
| `retail_silver.inventory` | Auto CDC | Type 1 | `inventory_id` | `last_stock_update` | `inventory_id` |
| `retail_silver.opportunity` | Auto CDC | Type 1 | `id` | `last_modified_date` | `id` |
| `retail_silver.transactions` | Streaming read from bronze (append-only) | — | `transaction_id` | — | — |

> History tracking is intentionally enabled **only** for `account` and `product_catalog`; `inventory` and `opportunity` keep just the latest state per key.

### Streaming & idempotency guarantees

* **First run = full load**: each Auto CDC flow's first update reads the bronze table's CDF initial snapshot (all rows as inserts).
* **Subsequent runs = incremental**: only new change events (inserts / updates / deletes) since the last checkpoint are processed. Re-running with no source changes is a no-op — no duplicates, no reprocessing.
* **Never batch**: every silver ingestion path is a streaming read. Gold aggregations are the only batch recomputes.

### Business transformations

* **account** — column standardization to snake_case, `customer_name` upper-cased & trimmed, null `industry` → `UNKNOWN`, `SystemModstamp` retained for sequencing.
* **product_catalog** — `initcap` names/categories, null `subcategory` / `brand` → `Uncategorized`, `unit_price` rounded & cast to `DECIMAL(10,2)`, derived `product_segment` (`PREMIUM` > 50000 / `MID_RANGE` > 10000 / `BUDGET`).
* **inventory** — derived `inventory_status` (`LOW_STOCK` when `stock_quantity < reorder_level`, else `HEALTHY`).
* **opportunity** — `opportunity_name` upper-cased, `stage_name` trimmed, `amount` rounded & cast `DECIMAL(18,2)`, derived `deal_size` (`ENTERPRISE` > 100000 / `MID_MARKET` > 25000 / `SMALL`), `probability` rounded, null `type` / `lead_source` → `UNKNOWN`.
* **transactions** — types cast (`quantity` → INT, prices → `DECIMAL(18,2)`), timestamp parsed from `dd-MMM-yyyy hh.mm.ss a`, derived `gross_amount` = `quantity × selling_price − discount_amount`.
* Every table carries a `processed_at` ingestion timestamp.

### Data quality expectations

| Table | Fail (pipeline stops) | Drop (row discarded) | Warn (logged) |
|---|---|---|---|
| `account` | — | `id IS NOT NULL` | `customer_name` not null; `annual_revenue ≥ 0`; `number_of_employees ≥ 0` |
| `product_catalog` | `product_id IS NOT NULL` | valid `product_name`, `category`, `unit_price ≥ 0` | `launch_date` not null; `supplier_name` not null |
| `inventory` | — | `inventory_id IS NOT NULL` | `stock_quantity > 0`; `product_id` not null; `store_id` not null |
| `opportunity` | — | `id IS NOT NULL` | `opportunity_name`, `account_id`, `close_date` not null; `stage_name` in valid set; `amount ≥ 0`; `probability` 0–100 |
| `transactions` | — | `transaction_id IS NOT NULL` | quantity/prices positive, timestamps & payment mode not null |

## 3. Gold Layer

| Object | Type | Definition |
|---|---|---|
| `retail_gold.fact_sales` | Materialized view (pipeline) | Sales facts — `transactions` LEFT JOIN `opportunity` on `upper(trim(opportunity_name))` |
| `retail_gold.fact_inventory` | View | Silver `inventory` (current stock, status, warehouse) |
| `retail_gold.dim_product` | View | Active products — `WHERE __END_AT IS NULL` on silver `product_catalog` |
| `retail_gold.dim_customer` | View | Active, non-deleted accounts — `WHERE is_deleted = false AND __END_AT IS NULL` on silver `account` |
| `retail_gold.dim_calendar` | Table (notebook `03_calendar`) | Calendar dimension: year / quarter / month / week / day attributes, weekend & month-boundary flags; date range driven by `start_date` / `end_date` widgets |

> SCD2 dimensions are consumed with the standard active-row filter `__END_AT IS NULL` (see `Retail_Project/02_Gold_views`).

## 4. Semantic Layer — AI/BI Metric View

`retail_q.retail_semantic.retail_metric` (notebook `04_Metric_view`) — governed metric view joining `fact_sales`, `dim_product`, `dim_customer`, `dim_calendar`.

* **22 dimensions** — Transaction Date/Month, Payment Mode, Sales Channel, Stage Name, Store ID, Product Name / Category / Subcategory / Brand / Segment, Supplier, Customer Name / Type, Billing geography, Industry, and full calendar hierarchy.
* **12 measures** — Transaction Count, Total Quantity, Total Revenue, Total Discount, Net Revenue, Avg Order Value, Avg Discount per Transaction, Discount Rate, Total Opportunity Amount, Won Revenue, Unique Customers, Unique Products.

Reference data: payment modes (Card, Cash, Crypto, Net Banking, UPI), sales channels (Online, Store), product segments (BUDGET, MID_RANGE, PREMIUM), customer types (Channel, Direct).

## 5. Consumption Layer

* **AI/BI Dashboard — "Retail Sales Analysis"** (6 pages): Executive Summary, Product Performance, Sales Pipeline Analysis, Customer & Geographic Analysis, Time & Calendar Analysis, Payment & Channel Analysis. All queries use `MEASURE()` syntax against the metric view.
* **Genie space — "Retail Sales and Inventory Analytics"**: conversational natural-language analytics grounded on the project's tables. Capabilities: sales performance (revenue, discounts, volume by product / customer / period / channel), inventory levels & reorder needs, customer segmentation (type, region, industry, revenue), product performance trends over calendar dimensions.

## 6. Orchestration — Retail_Master_Job

A Lakeflow Job (**`Retail_Master_Job`**, created and managed by the bundle, **daily 06:00 IST**, unpaused) runs the entire platform end-to-end:

| # | Task | Type | Purpose |
|---|---|---|---|
| 1 | `Ingest_Salesforce` | pipeline | runs `salesforce_ingestion` (bronze refresh) |
| 2 | `Ingest_Postgres` | pipeline | runs `postgres_ingestion` (bronze refresh) |
| 3 | `Ingest_Blob` | notebook | runs `01_blob_to_bronze` (Auto Loader, available-now) |
| 4 | `Transform_Silver_Gold` | pipeline | runs `Retail_Transformation_Pipeline` (silver + `fact_sales`) |
| 5 | `Calendar_Dimension` | notebook | runs `03_calendar` (rebuilds `dim_calendar`) |
| 6 | `Gold_Views` | notebook | runs `02_Gold_views` (dimension / fact views) |
| 7 | `Semantic_Layer` | notebook | runs `04_Metric_view` (metric view) |
| 8 | `Refresh_Dashboard` | dashboard task | refreshes the "Retail Sales Analysis" dashboard from gold data |

The job is fully idempotent — pipelines process incrementally (no-op when nothing changed) and the notebooks use `CREATE OR REPLACE` / checkpointed writes, so the daily run is safe to repeat.

## 7. CI/CD — Declarative Automation Bundle (DAB)

`databricks.yml` defines two targets and **creates and owns every resource** — pipelines, job, and dashboard. Nothing is adopted by ID or created in the UI:

* **dev** (default, development mode) — resources auto-prefixed `[dev <user>]`, schedules paused, bundle root `~/.bundle/retail_lakehouse_project/dev`.
* **prod** (production mode) — canonical resource names, daily schedule live, bundle root `~/.bundle/retail_lakehouse_project/prod`.

GitHub Actions (`.github/workflows/deploy.yml`):

| Trigger | Action |
|---|---|
| Pull request | `bundle validate` only (no deploy) |
| Push to `main` | validate + **deploy to dev** |
| Tag `v*` | validate + **deploy to prod** |

**Verified status (2026-11-06):** end-to-end run of `Retail_Master_Job` completed green — all 8 tasks succeeded; bronze/silver/gold verified (1,000 transactions through to `fact_sales`; metric view returning 1,000 transactions / $58.8M revenue; dashboard refreshed from gold data).

## How to Run

**Prerequisites** — catalog `retail_q` with schemas `blob_bronze`, `salesforce_bronze`, `postgres_bronze`, `retail_silver`, `retail_gold`, `retail_semantic`; volume `retail_q.volumes.blob_source` containing the transaction CSVs.

**One-command path:** run `Retail_Master_Job` — it orchestrates the full flow end-to-end (§6) and runs daily at 06:00 IST on schedule.

Manual, step-by-step:

1. **Bronze**: run `01_blob_to_bronze` (Auto Loader ingest); ensure the Salesforce / Postgres bronze streaming tables are loaded with CDF enabled.
2. **Silver + fact**: trigger an update of the `Retail_Transformation_Pipeline` pipeline — first run performs the full load, later runs are incremental.
3. **Gold**: run `02_Gold_views` (dimension / fact views) and `03_calendar` (calendar dimension, set the `start_date` / `end_date` widgets).
4. **Semantic**: run `04_Metric_view` to create the metric view.
5. **Consumption**: open the "Retail Sales Analysis" dashboard and the "Retail Sales and Inventory Analytics" Genie space.

### Testing incremental behavior

Run the MERGE statements in `Retail_Project/Test` against the bronze tables (new records + updates + deletes), then trigger another pipeline update. Expected results: SCD1 tables update in place, SCD2 tables (`account`, `product_catalog`) open new history versions while closing old rows (`__END_AT` set), and gold / semantic layers reflect the change.

### Querying SCD Type 2 history

```sql
-- Current state of a dimension
SELECT * FROM retail_q.retail_silver.account WHERE __END_AT IS NULL;

-- Point-in-time view (what did we know at time T?)
SELECT * FROM retail_q.retail_silver.account
WHERE __START_AT <= :t AND (__END_AT > :t OR __END_AT IS NULL);
```

## Operational Notes

* **Dataset type changes** (e.g., materialized view → streaming table) cannot be applied in place — the existing table must be dropped manually first; a full refresh will not fix it.
* **Deletes propagate**: bronze delete events close SCD2 rows / remove SCD1 rows via `apply_as_deletes`.
* **Schema evolution**: silver table schemas are inferred from the transformed CDF stream; bronze `addNewColumns` schema evolution flows through automatically.
* **Rebuilding a silver table** seeds it from the bronze current state — SCD2 history versions accrued before the rebuild are not replayed (CDF initial snapshot semantics).
* **Pipeline-deployed sources**: the job and pipelines execute the bundle-deployed copies at `~/.bundle/retail_lakehouse_project/<target>/files/`, not the workspace originals — always change code in the repo and redeploy.
* **A table can only be owned by one pipeline**: tables keep the `pipelines.pipelineId` property even after their pipeline is deleted, which blocks a new pipeline from rebuilding them. Drop the stale tables first (silver/gold rebuild from bronze automatically).
* **Notebook task hygiene**: job tasks execute every cell, so keep task notebooks free of scratch/exploration cells. Also note that `%sql ${widget}` interpolation resolves to an empty string in job runs — read widget values in Python instead (see `03_calendar`).
* **Dev/prod share one data layer**: both targets point at the same `retail_q` schemas — the transformation pipeline that owns the silver/gold tables is the prod one; keep the (paused) dev job from running pipeline tasks against them.
