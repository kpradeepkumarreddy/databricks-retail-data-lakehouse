CREATE OR REPLACE MATERIALIZED VIEW retail_q.gold.dim_product AS
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    product_segment,
    unit_price,
    supplier_name,
    launch_date,
    updated_at
FROM retail_q.silver.product_catalog
where is_active=true;