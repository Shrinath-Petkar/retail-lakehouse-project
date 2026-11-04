from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Single-layer silver streaming table — Auto CDC with SCD Type 2 (full history).
# All silver transformations are applied to the bronze CDF events BEFORE the
# CDC merge, so one streaming table holds clean, transformed data with change
# history — no separate raw copy, no batch MV recompute, fully incremental
# and idempotent. Active rows: __END_AT IS NULL.

@dp.temporary_view()
def product_catalog_cdf_source():
    """CDF streaming read from bronze — silver transformations applied in-flight, before Auto CDC."""
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .table("retail_q.postgres_bronze.product_catalog")
        .select(
            "_change_type",
            F.col("product_id"),
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
            F.initcap(F.trim(F.col("category"))).alias("category"),
            F.when(F.trim(F.col("subcategory")).isNull() | (F.trim(F.col("subcategory")) == ""), F.lit("Uncategorized"))
             .otherwise(F.initcap(F.trim(F.col("subcategory")))).alias("subcategory"),
            F.when(F.trim(F.col("brand")).isNull() | (F.trim(F.col("brand")) == ""), F.lit("Uncategorized"))
             .otherwise(F.initcap(F.trim(F.col("brand")))).alias("brand"),
            F.round(F.col("unit_price"), 2).cast("decimal(10, 2)").alias("unit_price"),
            F.initcap(F.trim(F.col("supplier_name"))).alias("supplier_name"),
            F.col("launch_date"),
            F.when(F.col("unit_price") > 50000, "PREMIUM")
                .when(F.col("unit_price") > 10000, "MID_RANGE")
                .otherwise("BUDGET")
                .alias("product_segment"),
            F.col("updated_at"),
            F.current_timestamp().alias("processed_at"),
        )
    )


dp.create_streaming_table(
    name="retail_q.retail_silver.product_catalog",
    comment="Silver product catalog — SCD Type 2 streaming table with standardized fields, computed columns and data quality checks from postgres bronze source",
    cluster_by=["product_id"],
    expect_all_or_fail={
        "valid_product_id": "product_id IS NOT NULL",
    },
    expect_all_or_drop={
        "valid_product_name": "product_name IS NOT NULL AND product_name != ''",
        "valid_category": "category IS NOT NULL AND category != ''",
        "valid_unit_price": "unit_price IS NOT NULL AND unit_price >= 0",
    },
    expect_all={
        "valid_launch_date": "launch_date IS NOT NULL",
        "valid_supplier": "supplier_name IS NOT NULL AND supplier_name != ''",
    },
)


dp.create_auto_cdc_flow(
    target="retail_q.retail_silver.product_catalog",
    source="product_catalog_cdf_source",
    keys=["product_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
)
