CREATE OR REPLACE MATERIALIZED VIEW retail_q.gold.dim_customer AS
SELECT
    id AS customer_id,
    customer_name,
    type AS customer_type,
    billing_city,
    billing_state,
    billing_country,
    phone,
    website,
    industry,
    annual_revenue,
    number_of_employees,
    description
FROM retail_q.silver.account
WHERE is_deleted = false and is_active=true;