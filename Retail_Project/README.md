# Retail Project — Supporting Notebooks

Operational notebooks that run alongside the Lakeflow Spark Declarative Pipeline `retail_transformations_Final` (see the root `README.md` for the pipeline itself). They cover bronze ingestion, gold views, the calendar dimension, the semantic layer, and source-change simulation.

## Notebooks

| Notebook | Purpose | What it does |
|---|---|---|
| `01_blob_to_bronze` | Bronze ingestion | Auto Loader (`cloudFiles`) streams transaction CSVs from `/Volumes/retail_q/volumes/blob_source/transaction_source/` into `retail_q.blob_bronze.transactions`. Triggered `availableNow` (processes new files then stops), schema evolution `addNewColumns`, adds `_ingested_at` / `_source_file` metadata. Checkpointed — safe to re-run, only new files are ingested. |
| `02_Gold_views` | Gold views | Creates `dim_product`, `dim_customer`, `fact_inventory` as views over the silver layer. SCD2 dimensions use the active-row filter (`WHERE __END_AT IS NULL`; `dim_customer` also excludes deleted accounts). Idempotent `CREATE OR REPLACE VIEW`. |
| `03_calendar` | Calendar dimension | Generates `retail_gold.dim_calendar` for a widget-driven `start_date` / `end_date` range: year / quarter / month / week / day columns plus weekend and month-boundary flags. Re-run when extending the date range. |
| `04_Metric_view` | Semantic layer | Creates the AI/BI metric view `retail_q.retail_semantic.retail_metric` joining `fact_sales`, `dim_product`, `dim_customer`, `dim_calendar` — 22 dimensions and 12 measures (revenue, discounts, AOV, opportunity amounts, distinct counts) powering the dashboard and Genie space. |
| `Test` | Change simulation | `MERGE INTO` statements against the bronze tables to simulate source CDC — new records, updates, and deletes. Run, then trigger a pipeline update to watch incremental processing and SCD2 history in action. |

## Run order

```
01_blob_to_bronze  ──▶  retail_transformations_Final pipeline update  ──▶  02_Gold_views  ──▶  03_calendar  ──▶  04_Metric_view
                                                  (silver + fact_sales)
Test — run any time against bronze, followed by another pipeline update
```

Prerequisites: catalog `retail_q` with schemas `blob_bronze`, `salesforce_bronze`, `postgres_bronze`, `retail_silver`, `retail_gold`, `retail_semantic`, and volume `retail_q.volumes` with the source/checkpoint directories. Notebooks run on a SQL warehouse.

## Consumption assets (exported to `analytics/`)

* **Dashboard** — "Retail Sales Analysis" (6 pages: Executive Summary, Product Performance, Sales Pipeline, Customer & Geographic, Time & Calendar, Payment & Channel) querying the metric view with `MEASURE()` syntax. Definition: `analytics/Retail Sales Analysis.lvdash.json` — import into a Databricks workspace via the dashboard import UI.
* **Genie space** — "Retail Sales and Inventory Analytics": natural-language Q&A over the retail tables (sales, inventory, customers, product trends). This is a workspace asset and is not file-exported to this repo — to recreate it in another workspace, create a Genie space pointed at the `retail_q` tables listed above.
