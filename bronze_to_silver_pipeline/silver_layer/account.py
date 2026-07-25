from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.silver.account",
    comment="Silver layer account data with standardizations and data quality rules"
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
@dp.expect_or_drop("valid_name", "customer_name IS NOT NULL AND TRIM(customer_name) != ''")
@dp.expect("valid_phone", "phone IS NULL OR LENGTH(TRIM(phone)) >= 10")
@dp.expect("valid_website", "website IS NULL OR website LIKE 'http%'")
@dp.expect("valid_dates", "created_date <= last_modified_date")
def silver_account():
    """
    Transforms bronze account data to silver layer by:
    - Reading from bronze streaming table
    - Removing columns with all null values
    - Applying standardizations (trim strings, proper case, date handling)
    - Enforcing data quality rules
    """
    return (
        spark.readStream.table("retail_q.bronze.account")
        .select(
            # Core identifiers
            F.col("Id").alias("id"),
            F.col("IsDeleted").alias("is_deleted"),
            
            # Account details - with standardizations
            F.trim(F.initcap(F.col("Name"))).alias("customer_name"),
            F.trim(F.col("Type")).alias("type"),
            F.col("ParentId").alias("parent_id"),
            
            # Billing address - standardized
            F.trim(F.initcap(F.col("BillingStreet"))).alias("billing_street"),
            F.trim(F.initcap(F.col("BillingCity"))).alias("billing_city"),
            F.trim(F.initcap(F.col("BillingState"))).alias("billing_state"),
            F.trim(F.col("BillingPostalCode")).alias("billing_postal_code"),
            F.trim(F.upper(F.col("BillingCountry"))).alias("billing_country"),
            F.trim(F.upper(F.col("BillingStateCode"))).alias("billing_state_code"),
            F.trim(F.upper(F.col("BillingCountryCode"))).alias("billing_country_code"),
            
            # Shipping address - standardized
            F.trim(F.initcap(F.col("ShippingStreet"))).alias("shipping_street"),
            F.trim(F.initcap(F.col("ShippingCity"))).alias("shipping_city"),
            F.trim(F.initcap(F.col("ShippingState"))).alias("shipping_state"),
            F.trim(F.col("ShippingPostalCode")).alias("shipping_postal_code"),
            F.trim(F.upper(F.col("ShippingCountry"))).alias("shipping_country"),
            
            # Contact information - standardized
            F.regexp_replace(F.trim(F.col("Phone")), r"[^0-9+]", "").alias("phone"),
            F.trim(F.lower(F.col("Website"))).alias("website"),
            
            # Business details
            F.coalesce(F.trim(F.col("Industry")), F.lit("UNKNOWN")).alias("industry"),
            F.col("AnnualRevenue").alias("annual_revenue"),
            F.col("NumberOfEmployees").alias("number_of_employees"),
            F.trim(F.col("Description")).alias("description"),
            
            # Ownership and tracking
            F.col("OwnerId").alias("owner_id"),
            F.col("CreatedDate").alias("created_date"),
            F.col("CreatedById").alias("created_by_id"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("LastModifiedById").alias("last_modified_by_id"),
            F.col("SystemModstamp").alias("system_modstamp"),
            F.col("LastViewedDate").alias("last_viewed_date"),
            F.col("LastReferencedDate").alias("last_referenced_date"),
            
            # Flags
            F.col("IsCustomerPortal").alias("is_customer_portal"),
            F.col("IsBuyer").alias("is_buyer"),
            
            # CDC tracking columns
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),
            
            # Compute is_active: True when __END_AT is null (active record), False otherwise
            F.when(F.col("__END_AT").isNull(), True).otherwise(False).alias("is_active")
        )
    )