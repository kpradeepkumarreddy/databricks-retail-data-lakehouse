CREATE OR REPLACE  MATERIALIZED VIEW retail_q.gold.fact_inventory AS
SELECT
    inventory_id,
    product_id,
    stock_quantity,
    reorder_level,
    inventory_status,
    warehouse_location,
    last_stock_update
FROM retail_q.silver.inventory;