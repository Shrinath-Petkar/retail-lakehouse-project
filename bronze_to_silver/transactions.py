from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Table locations come from the pipeline configuration set by databricks.yml per target.
CATALOG = spark.conf.get("retail.catalog", "retail_q")
SILVER = spark.conf.get("retail.silver_schema", "retail_silver")
BLOB_BRONZE = spark.conf.get("retail.blob_bronze_schema", "blob_bronze")


@dp.table(
    name=f"{CATALOG}.{SILVER}.transactions",
    comment="Transaction data from blob bronze with schema-converted columns, standardized fields, and data quality checks",
)
@dp.expect_or_drop("non-null transaction_id", "transaction_id IS NOT NULL")
@dp.expect("non-null opportunity_name", "opportunity_name IS NOT NULL")
@dp.expect("non-null store_id", "store_id IS NOT NULL")
@dp.expect("valid quantity", "quantity > 0")
@dp.expect("valid selling_price", "selling_price >= 0")
@dp.expect("valid discount_amount", "discount_amount >= 0")
@dp.expect("non-null transaction_timestamp", "transaction_timestamp IS NOT NULL")
@dp.expect("non-null payment_mode", "payment_mode IS NOT NULL")
def transactions_clean():
    # Read from bronze streaming table (Auto Loader ingestion)
    source_df = spark.readStream.table(f"{CATALOG}.{BLOB_BRONZE}.transactions")

    # Select business-relevant columns with schema conversion and standard transformations
    # Dropped columns: _rescued_data (100% null), _ingested_at (metadata), _source_file (metadata)
    # Schema conversions: quantity string -> int, selling_price/discount_amount string -> decimal(18,2)
    # Timestamp format: dd-MMM-yyyy hh.mm.ss a (e.g. "17-Mar-2026 05.09.51 PM")
    return source_df.select(
        F.col("transaction_id").alias("transaction_id"),
        F.upper(F.trim(F.col("opportunity_name"))).alias("opportunity_name"),
        F.trim(F.col("product_id")).alias("product_id"),
        F.trim(F.col("store_id")).alias("store_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.round(F.col("selling_price").cast("decimal(18, 2)"), 2).alias("selling_price"),
        F.round(F.col("discount_amount").cast("decimal(18, 2)"), 2).alias("discount_amount"),
        F.to_timestamp(F.col("transaction_timestamp"), "dd-MMM-yyyy hh.mm.ss a").alias("transaction_timestamp"),
        F.trim(F.col("payment_mode")).alias("payment_mode"),
        F.trim(F.col("sales_channel")).alias("sales_channel"),
        F.round((F.col("quantity") * F.col("selling_price")) - F.col("discount_amount"), 2).alias("gross_amount"),
        F.current_timestamp().alias("processed_at"),
    )