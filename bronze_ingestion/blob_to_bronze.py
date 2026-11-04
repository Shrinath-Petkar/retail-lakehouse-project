from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Table locations come from the pipeline configuration set by databricks.yml per target.
CATALOG = spark.conf.get("retail.catalog", "retail_q")
BLOB_BRONZE = spark.conf.get("retail.blob_bronze_schema", "blob_bronze")

# Blob source (CSV files arriving in the UC volume)
SOURCE_PATH = "/Volumes/retail_q/volumes/blob_source/transaction_source/"
SCHEMA_LOCATION = "/Volumes/retail_q/volumes/blob_source/_schemas/transactions"


dp.create_streaming_table(
    name=f"{CATALOG}.{BLOB_BRONZE}.transactions",
    comment="Bronze transactions - Auto Loader CSV ingestion from the blob source volume with schema evolution and traceability metadata",
)


@dp.append_flow(
    name="ingest_transactions",
    target=f"{CATALOG}.{BLOB_BRONZE}.transactions",
)
def ingest_transactions():
    """Incrementally read CSV files from the blob source volume via Auto Loader."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
