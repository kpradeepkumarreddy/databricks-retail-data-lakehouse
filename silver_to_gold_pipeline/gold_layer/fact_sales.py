from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="retail_q.gold.fact_sales",
    comment="Fact table combining transaction and opportunity data"
)
def fact_sales():
    # Read silver layer tables in batch mode
    transactions = spark.read.table("retail_q.silver.transactions")
    opportunity = spark.read.table("retail_q.silver.opportunity")
    
    # Join transactions with opportunity on opportunity_name = name
    fact_sales_df = transactions.alias("t").join(
        opportunity.alias("o"),
        F.col("t.opportunity_name") == F.col("o.name"),
        "left"
    ).select(
        F.col("t.transaction_id"),
        F.col("t.opportunity_name"),
        F.col("t.product_id"),
        F.col("t.store_id"),
        F.col("t.quantity"),
        F.col("t.selling_price"),
        F.col("t.discount_amount"),
        F.col("t.transaction_timestamp"),
        F.col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        F.col("t.payment_mode"),
        F.col("t.sales_channel"),
        F.col("o.name"),
        F.col("o.stage_name"),
        F.col("o.owner_id"),
        F.col("o.amount"),
        F.col("o.account_id").alias("customer_id")
    )
    
    return fact_sales_df
