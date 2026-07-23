from pyspark import pipelines as dp
from pyspark.sql.functions import col, lower, trim, when

# Define a view to read the streaming table and apply generic standardization transformations
@dp.temporary_view()
def standardized_product_catalog():
    df = spark.readStream.table("retail_q.postgres_bronze.product_catalog")
    # Generic standardization: trim and lowercase string columns, handle nulls
    return (
        df.withColumn("product_name", trim(lower(col("product_name"))))
          .withColumn("category", trim(lower(col("category"))))
          .withColumn("subcategory", trim(lower(col("subcategory"))))
          .withColumn("brand", trim(lower(col("brand"))))
          .withColumn("supplier_name", trim(lower(col("supplier_name"))))
          .withColumn(
              "product_segment",
              when(col("unit_price") > 50000, "PREMIUM")
              .when(col("unit_price") > 10000, "MID_RANGE")
              .otherwise("BUDGET")
          )
          .filter(col("is_active") == True)
    )

# Write the output to the target streaming table with core data quality rules
@dp.table(
    name="retail_q.retail_silver.product_catalog",
    comment="Standardized and quality-checked product catalog"
)
@dp.expect("valid product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("non-null product_name", "product_name IS NOT NULL")
@dp.expect_or_drop("positive unit_price", "unit_price > 0")
@dp.expect_or_drop("category not invalid", "NOT(category LIKE 'invalid%')")
def retail_silver_product_catalog():
    return spark.readStream.table("standardized_product_catalog")