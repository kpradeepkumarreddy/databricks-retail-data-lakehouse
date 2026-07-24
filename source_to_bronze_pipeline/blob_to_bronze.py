# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Auto Loader: Ingest CSV files from Volume to Bronze table
# Read CSV files from Volume using Auto Loader
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/transactions_schema")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Volumes/retail_q/volumes/blob_source/transactions/")
)

# Write to bronze Delta table
(df.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/transactions_checkpoint")
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable("retail_q.bronze.transactions")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retail_q.bronze.transactions;