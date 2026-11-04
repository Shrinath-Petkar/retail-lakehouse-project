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
def opportunity_cdf_source():
    """CDF streaming read from bronze — silver transformations applied in-flight, before Auto CDC."""
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.salesforce_bronze.opportunity")
        .select(
            "_change_type",
            F.col("Id").alias("id"),
            F.col("IsDeleted").alias("is_deleted"),
            F.col("AccountId").alias("account_id"),
            F.upper(F.trim(F.col("Name"))).alias("opportunity_name"),
            F.col("Description").alias("description"),
            F.trim(F.col("StageName")).alias("stage_name"),
            F.round(F.col("Amount"), 2).cast("decimal(18, 2)").alias("amount"),
            F.when(F.col("Amount") > 100000, F.lit("ENTERPRISE"))
                .when(F.col("Amount") > 25000, F.lit("MID_MARKET"))
                .otherwise(F.lit("SMALL"))
                .alias("deal_size"),
            F.round(F.col("Probability"), 2).alias("probability"),
            F.col("CloseDate").alias("close_date"),
            F.coalesce(F.trim(F.col("Type")), F.lit("UNKNOWN")).alias("type"),
            F.coalesce(F.trim(F.col("LeadSource")), F.lit("UNKNOWN")).alias("lead_source"),
            F.col("IsClosed").alias("is_closed"),
            F.col("IsWon").alias("is_won"),
            F.col("ForecastCategoryName").alias("forecast_category"),
            F.col("HasOpportunityLineItem").alias("has_line_item"),
            F.col("OwnerId").alias("owner_id"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("FiscalQuarter").alias("fiscal_quarter"),
            F.col("FiscalYear").alias("fiscal_year"),
            F.current_timestamp().alias("processed_at"),
        )
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER}.opportunity",
    comment="Silver opportunity — SCD Type 1 streaming table with standardized columns and data quality checks",
    cluster_by=["id"],
    expect_all_or_drop={
        "non-null opportunity_id": "id IS NOT NULL",
    },
    expect_all={
        "non-null opportunity_name": "opportunity_name IS NOT NULL",
        "non-null account_id": "account_id IS NOT NULL",
        "valid stage": "stage_name IN ('Prospecting','Closed Won','Closed Lost')",
        "non-null close_date": "close_date IS NOT NULL",
        "valid amount": "amount IS NULL OR amount >= 0",
        "valid probability": "probability IS NULL OR (probability >= 0 AND probability <= 100)",
    },
)


dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER}.opportunity",
    source="opportunity_cdf_source",
    keys=["id"],
    sequence_by="last_modified_date",
    stored_as_scd_type=1,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
)