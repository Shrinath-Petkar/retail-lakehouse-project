# Silver → Gold Layer

Source files for the gold layer of the Lakeflow Spark Declarative Pipeline **`retail_transformations_Final`**. This folder builds the gold **fact table** by joining clean silver streaming tables; the remaining gold objects (dimension views, calendar dimension) are managed by notebooks in `Retail_Project/` (see below).

## Files

### `fact_sales.py` → `retail_q.retail_gold.fact_sales`

Materialized view (batch recompute, refreshed with every pipeline update) joining silver transactions with opportunities.

* **Sources**: `retail_q.retail_silver.transactions` (1,100 streaming facts) LEFT JOIN `retail_q.retail_silver.opportunity` (102 current opportunities)
* **Join key**: `upper(trim(opportunity_name))` on both sides — case/whitespace-insensitive business-name match
* **Join type**: `LEFT` — every transaction is kept even when no matching opportunity exists

Output columns:

| Column | Source | Notes |
|---|---|---|
| `transaction_id`, `opportunity_name`, `product_id`, `store_id` | transactions | identifiers |
| `quantity`, `selling_price`, `discount_amount` | transactions | measures |
| `transaction_timestamp`, `transaction_date` | transactions | timestamp + cast to DATE |
| `payment_mode`, `sales_channel` | transactions | dimensions |
| `opp_name`, `stage_name`, `owner_id`, `amount` | opportunity | pipeline attributes |
| `customer_id` | opportunity `account_id` | joins to `dim_customer` |

**Why a materialized view**: gold aggregations/facts over streaming sources are the correct MV use case — they recompute from the current silver state, so SCD1 updates and new transactions are always reflected, without maintaining streaming state for a bounded dataset.

## Other gold objects (managed outside this folder)

| Object | Type | Defined in |
|---|---|---|
| `retail_gold.dim_product` | View | `Retail_Project/02_Gold_views` — active products from silver `product_catalog` (`WHERE __END_AT IS NULL`) |
| `retail_gold.dim_customer` | View | `Retail_Project/02_Gold_views` — active, non-deleted accounts from silver `account` (`WHERE is_deleted = false AND __END_AT IS NULL`) |
| `retail_gold.fact_inventory` | View | `Retail_Project/02_Gold_views` — silver `inventory` current state |
| `retail_gold.dim_calendar` | Table | `Retail_Project/03_calendar` — widget-driven `start_date` / `end_date` range |

> Downstream: `fact_sales` + the dimension views feed the semantic metric view `retail_q.retail_semantic.retail_metric` (`Retail_Project/04_Metric_view`), the **Retail Sales Analysis** dashboard, and the **Retail Sales and Inventory Analytics** Genie space. See the root `README.md` for the full picture.

## Refresh behavior

* `fact_sales` refreshes with every pipeline update — it recomputes from the current silver state, picking up new transactions and SCD1 opportunity updates automatically.
* Silver streaming tables only process new change events; the MV is the only batch recompute in the pipeline (by design).

## Extending this layer

To add another gold fact/aggregate:

1. Add `<entity>.py` here reading silver tables with `spark.read.table` (batch reads — MV semantics are correct for gold).
2. Join on business keys; prefer `LEFT` joins to keep all facts.
3. Keep row-level detail in facts; push aggregation to the semantic layer (metric view).
4. Run a dry run, then a pipeline update. Note: adding a new dataset is safe; **changing an existing dataset's type** (MV ↔ streaming table) requires dropping the table manually first.

This folder is included in the pipeline via the `silver_to_gold/fact_sales.py` library entry — new `.py` files must be added to the pipeline libraries to run. Markdown files are ignored by the pipeline parser.
