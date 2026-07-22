# Databricks notebook source
# DBTITLE 1,Auto Loader: Ingest CSV files from Volume to Bronze table
# Read CSV files from Volume using Auto Loader
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/transactions_schema")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Volumes/retail_q/volumes/blob_source/transactions_source/")
)

# Write to bronze Delta table
(df.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/transactions_checkpoint")
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable("retail_q.blob_bronze.transactions")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retail_q.blob_bronze.transactions;