from pyspark.sql.functions import upper, trim, col
from pyspark import pipelines as dp


# Table locations come from the pipeline configuration set by databricks.yml per target.
CATALOG = spark.conf.get("retail.catalog", "retail_q")
SILVER = spark.conf.get("retail.silver_schema", "retail_silver")
GOLD = spark.conf.get("retail.gold_schema", "retail_gold")


@dp.table(name=f"{CATALOG}.{GOLD}.fact_sales")
def fact_sales():
    transactions_df = spark.read.table(f"{CATALOG}.{SILVER}.transactions")
    opportunity_df = spark.read.table(f"{CATALOG}.{SILVER}.opportunity")

    joined_df = transactions_df.alias("t").join(
        opportunity_df.alias("o"),
        upper(trim(transactions_df.opportunity_name)) == upper(trim(opportunity_df["opportunity_name"])),
        how="left"
    )

    # Select important columns (customize as needed)
    selected_df = joined_df.select(
        "t.transaction_id",
        "t.opportunity_name",
        "t.product_id",
        "t.store_id",
        "t.quantity",
        "t.selling_price",
        "t.discount_amount",
        "t.transaction_timestamp",
        col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        "t.payment_mode",
        "t.sales_channel",
        col("o.opportunity_name").alias("opp_name"),
        "o.stage_name",
        "o.owner_id",
        "o.amount",
        col("o.account_id").alias("customer_id")
    )

    return selected_df