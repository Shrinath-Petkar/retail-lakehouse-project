from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Single-layer silver streaming table — Auto CDC with SCD Type 2 (full history).
# All silver transformations are applied to the bronze CDF events BEFORE the
# CDC merge, so one streaming table holds clean, transformed data with change
# history — no separate raw copy, no batch MV recompute, fully incremental
# and idempotent. Active rows: __END_AT IS NULL.

@dp.temporary_view()
def account_cdf_source():
    """CDF streaming read from bronze — silver transformations applied in-flight, before Auto CDC."""
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .table("retail_q.salesforce_bronze.account")
        .select(
            "_change_type",
            F.col("Id").alias("id"),
            F.col("IsDeleted").alias("is_deleted"),
            F.upper(F.trim(F.col("Name"))).alias("customer_name"),
            F.col("Type").alias("type"),
            F.col("ParentId").alias("parent_id"),
            F.col("BillingStreet").alias("billing_street"),
            F.col("BillingCity").alias("billing_city"),
            F.col("BillingState").alias("billing_state"),
            F.col("BillingPostalCode").alias("billing_postal_code"),
            F.col("BillingCountry").alias("billing_country"),
            F.col("ShippingStreet").alias("shipping_street"),
            F.col("ShippingCity").alias("shipping_city"),
            F.col("ShippingState").alias("shipping_state"),
            F.col("ShippingPostalCode").alias("shipping_postal_code"),
            F.col("ShippingCountry").alias("shipping_country"),
            F.col("Phone").alias("phone"),
            F.col("Website").alias("website"),
            F.coalesce(F.col("Industry"), F.lit("UNKNOWN")).alias("industry"),
            F.col("AnnualRevenue").alias("annual_revenue"),
            F.col("NumberOfEmployees").alias("number_of_employees"),
            F.col("Description").alias("description"),
            F.col("SystemModstamp"),
            F.current_timestamp().alias("processed_at"),
        )
    )


dp.create_streaming_table(
    name="retail_q.retail_silver.account",
    comment="Silver account data — SCD Type 2 streaming table with standardized columns and data quality checks from Salesforce bronze source",
    cluster_by=["id"],
    expect_all_or_drop={
        "non-null account_id": "id IS NOT NULL",
    },
    expect_all={
        "non-null customer_name": "customer_name IS NOT NULL",
        "valid annual_revenue": "annual_revenue IS NULL OR annual_revenue >= 0",
        "valid number_of_employees": "number_of_employees IS NULL OR number_of_employees >= 0",
    },
)


dp.create_auto_cdc_flow(
    target="retail_q.retail_silver.account",
    source="account_cdf_source",
    keys=["id"],
    sequence_by="SystemModstamp",
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
)