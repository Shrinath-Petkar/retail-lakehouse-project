from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Table locations come from the pipeline configuration set by databricks.yml per target.
CATALOG = spark.conf.get("retail.catalog", "retail_q")
SILVER = spark.conf.get("retail.silver_schema", "retail_silver")


# Single-layer silver streaming table — Auto CDC with SCD Type 1 (latest state).
# All silver transformations are applied to the bronze CDF events BEFORE the
# CDC merge, so one streaming table holds clean, transformed current-state
# data — no separate raw copy, no batch MV recompute, fully incremental
# and idempotent.

@dp.temporary_view()
def inventory_cdf_source():
    """CDF streaming read from bronze — silver transformations applied in-flight, before Auto CDC."""
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.postgres_bronze.inventory")
        .select(
            "_change_type",
            F.col("inventory_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity"),
            F.col("reorder_level"),
            F.when(
                F.col("stock_quantity") < F.col("reorder_level"),
                "LOW_STOCK"
            ).otherwise("HEALTHY").alias("inventory_status"),
            F.col("warehouse_location"),
            F.col("last_stock_update"),
            F.current_timestamp().alias("processed_at"),
        )
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER}.inventory",
    comment="Silver inventory — SCD Type 1 streaming table with computed inventory status and data quality checks",
    cluster_by=["inventory_id"],
    expect_all_or_drop={
        "non-null inventory_id": "inventory_id IS NOT NULL",
    },
    expect_all={
        "valid stock_quantity": "stock_quantity > 0",
        "non-null product_id": "product_id IS NOT NULL",
        "non-null store_id": "store_id IS NOT NULL",
    },
)


dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER}.inventory",
    source="inventory_cdf_source",
    keys=["inventory_id"],
    sequence_by="last_stock_update",
    stored_as_scd_type=1,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
)