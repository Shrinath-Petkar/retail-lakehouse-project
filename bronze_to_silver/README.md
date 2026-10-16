# Bronze → Silver Layer

Source files for the silver layer of the Lakeflow Spark Declarative Pipeline **`retail_transformations_Final`**. Every dataset here reads a bronze Delta table's **Change Data Feed (CDF)** as a stream, applies all business transformations in-flight, and lands a single clean silver streaming table via **Auto CDC** — no batch reads, no intermediate raw copies.

## Files

| File | Silver table | CDC pattern | SCD | Keys | Sequenced by | Clustered on |
|---|---|---|---|---|---|---|
| `account.py` | `retail_silver.account` | Auto CDC | **Type 2** (history) | `id` | `SystemModstamp` | `id` |
| `product_catalog.py` | `retail_silver.product_catalog` | Auto CDC | **Type 2** (history) | `product_id` | `updated_at` | `product_id` |
| `inventory.py` | `retail_silver.inventory` | Auto CDC | Type 1 (latest) | `inventory_id` | `last_stock_update` | `inventory_id` |
| `opportunity.py` | `retail_silver.opportunity` | Auto CDC | Type 1 (latest) | `id` | `last_modified_date` | `id` |
| `transactions.py` | `retail_silver.transactions` | Streaming read (append-only) | — | `transaction_id` | — | — |

> History tracking is enabled **only** for `account` and `product_catalog` (SCD Type 2 with `__START_AT` / `__END_AT`). `inventory` and `opportunity` keep only the latest state per key (SCD Type 1, in-place overwrite).

## Design pattern (every CDC file)

```python
@dp.temporary_view()
def <entity>_cdf_source():
    # 1. Stream the bronze table's change data feed + apply ALL transformations
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table("retail_q.<bronze_schema>.<entity>")
        .select("_change_type", <transformed columns>)
    )

dp.create_streaming_table(
    name="retail_q.retail_silver.<entity>",   # 2. The silver table itself
    cluster_by=["<key>"],
    expect_all_or_fail={...},   # pipeline-stopping rules
    expect_all_or_drop={...},   # row-dropping rules
    expect_all={...},           # warn-only rules
)

dp.create_auto_cdc_flow(
    target="retail_q.retail_silver.<entity>",
    source="<entity>_cdf_source",
    keys=["<key>"],
    sequence_by="<sequence column>",
    stored_as_scd_type=1,                       # or 2 for history
    apply_as_deletes=F.expr("_change_type = 'delete'"),  # propagate deletes
    except_column_list=["_change_type"],
)
```

Why this shape:

* **Transform before CDC** — standardization, casts and computed columns are applied per change event, so the silver table stores only clean, business-ready data (Databricks best practice: clean data first).
* **Streaming + idempotent** — first pipeline update reads the bronze CDF initial snapshot (full load); later updates process only new change events. Re-running with no source changes is a no-op.
* **Delete propagation** — bronze delete events are honored via `apply_as_deletes` (SCD1 removes the row, SCD2 closes its validity window).
* **Key clustering** — liquid clustering on the primary key supports fast entity lookups.

## Business transformations

* **account** (`account.py`) — Salesforce columns aliased to snake_case, `customer_name` upper-cased & trimmed, null `industry` → `UNKNOWN`, `SystemModstamp` retained as a column for sequencing, `processed_at` ingestion timestamp.
* **product_catalog** (`product_catalog.py`) — `initcap` names / category / supplier, null `subcategory` / `brand` → `Uncategorized`, `unit_price` rounded & cast `DECIMAL(10,2)`, derived `product_segment` (PREMIUM > 50000 / MID_RANGE > 10000 / BUDGET).
* **inventory** (`inventory.py`) — derived `inventory_status`: `LOW_STOCK` when `stock_quantity < reorder_level`, else `HEALTHY`.
* **opportunity** (`opportunity.py`) — `opportunity_name` upper-cased, `stage_name` trimmed, `amount` rounded & cast `DECIMAL(18,2)`, derived `deal_size` (ENTERPRISE > 100000 / MID_MARKET > 25000 / SMALL), `probability` rounded, null `type` / `lead_source` → `UNKNOWN`.
* **transactions** (`transactions.py`) — `quantity` cast INT, prices cast `DECIMAL(18,2)`, `transaction_timestamp` parsed from `dd-MMM-yyyy hh.mm.ss a`, derived `gross_amount = quantity × selling_price − discount_amount`. Reads the Auto Loader bronze table with `spark.readStream` (append-only, no CDF needed).

## Data quality expectations

| Table | Fail (pipeline stops) | Drop (row discarded) | Warn (logged only) |
|---|---|---|---|
| `account` | — | `id IS NOT NULL` | `customer_name` not null · `annual_revenue ≥ 0` · `number_of_employees ≥ 0` |
| `product_catalog` | `product_id IS NOT NULL` | valid `product_name`, `category`, `unit_price ≥ 0` | `launch_date` not null · `supplier_name` not null |
| `inventory` | — | `inventory_id IS NOT NULL` | `stock_quantity > 0` · `product_id` not null · `store_id` not null |
| `opportunity` | — | `id IS NOT NULL` | `opportunity_name`, `account_id`, `close_date` not null · `stage_name` in (Prospecting, Closed Won, Closed Lost) · `amount ≥ 0` · `probability` 0–100 |
| `transactions` | — | `transaction_id IS NOT NULL` | `quantity > 0` · prices ≥ 0 · timestamps & payment mode not null |

Rules are attached to the streaming table (`create_streaming_table`), so bad data never lands in silver.

## SCD Type 2 — querying history

```sql
-- Current state
SELECT * FROM retail_q.retail_silver.account WHERE __END_AT IS NULL;

-- Point-in-time (what did we know at time T?)
SELECT * FROM retail_q.retail_silver.account
WHERE __START_AT <= :t AND (__END_AT > :t OR __END_AT IS NULL);
```

Each bronze update to a tracked key closes the old row (`__END_AT` stamped) and inserts a new active version.

## Extending this layer

To add a new silver dataset from a new bronze table:

1. Create `<entity>.py` in this folder using the design pattern above.
2. Choose SCD type: **2** if consumers need history, **1** for latest-state only.
3. Pick a business key and a monotonically increasing sequence column (source `updated_at` / `LastModifiedDate` style timestamps).
4. Add expectations — fail/drop for critical identity & integrity, warn for informational checks.
5. Run a dry run to validate, then trigger a pipeline update (first run = full load from the bronze CDF snapshot).

## Operational notes

* **Dataset type changes** (e.g., MV → streaming table) cannot be applied in place — drop the existing table manually first; a full refresh will not fix it.
* **Rebuilding a table** seeds from the bronze current state — SCD2 history accrued before the rebuild is not replayed.
* These files are included in the pipeline via the `bronze_to_silver/**` library glob. Markdown files in this folder are ignored by the pipeline parser.
