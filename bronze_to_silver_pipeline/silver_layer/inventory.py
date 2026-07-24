from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.inventory",
    comment="Standardized inventory data with DQ rules applied"
)
@dp.expect_or_drop("no null store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("no null product_id", "product_id IS NOT NULL")
def inventory_silver():
    df = spark.readStream.table("retail_q.postgres_bronze.inventory")
    # Example standardizations: lowercase warehouse_location, cast stock_quantity to integer
    standardized_df = (
        df.withColumn("warehouse_location", F.lower(F.col("warehouse_location")))
          .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
          .withColumn(
              "inventory_status",
              F.when(F.col("stock_quantity") < F.col("reorder_level"), "LOW_STOCK").otherwise("HEALTHY")
          )
    )
    return standardized_df