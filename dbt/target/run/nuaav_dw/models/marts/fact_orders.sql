
  
    

create or replace transient table NUAAV_DW.marts.fact_orders
    
    
    
    as (

-- Line-item fact table: one row per item/product in an order
-- Grain: Transaction ID + Order ID + Item/Product
-- Purpose: Analyze sales by product, customer, date with detailed item info

WITH stg_transactions AS (
    SELECT * FROM NUAAV_DW.staging.stg_transactions
),

dim_client AS (
    SELECT * FROM NUAAV_DW.marts.dim_client
),

dim_customer AS (
    SELECT * FROM NUAAV_DW.marts.dim_customer
),

dim_product AS (
    SELECT * FROM NUAAV_DW.marts.dim_product
),

dim_date AS (
    SELECT * FROM NUAAV_DW.marts.dim_date
),

dim_payment_type AS (
    SELECT * FROM NUAAV_DW.marts.dim_payment_type
),

stg_orders AS (
    SELECT * FROM NUAAV_DW.staging.stg_orders
),

order_items_with_keys AS (
    SELECT
        st.transaction_id,
        st.order_id,
        dc.client_key,
        COALESCE(dcust.customer_key, -1) AS customer_key,  -- Default to -1 (Unknown) if unmatched
        COALESCE(dp.product_key, -1) AS product_key,  -- Default to -1 (Unknown) if unmatched
        COALESCE(dd.date_key, -1) AS date_key,  -- Default to -1 (Unknown) if unmatched
        COALESCE(dpt.payment_type_key, (SELECT payment_type_key FROM dim_payment_type WHERE payment_method = 'Unknown')) AS payment_type_key,
        st.quantity,
        ABS(st.unit_price) AS unit_price,
        ABS(COALESCE(st.quantity * st.unit_price, 0)) AS line_total,
        ABS(st.payment_amount) AS payment_amount,
        COALESCE(st.payment_method, 'Unknown') AS payment_method,
        st.sku,
        st.customer_id,
        st.first_name,
        st.last_name,
        st.email,
        -- Data quality flags for dimension matching (identify records that used default unknown records)
        CASE WHEN dcust.customer_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_customer,
        CASE WHEN dp.product_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_product,
        CASE WHEN dd.date_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_date,
        st._source_file,
        st._loaded_at
    FROM stg_transactions st
    INNER JOIN dim_client dc ON st.client_id = dc.client_id
    LEFT JOIN stg_orders so ON st.order_id = so.order_id
    LEFT JOIN dim_customer dcust ON st.customer_id = dcust.customer_id AND dc.client_key = dcust.client_key
    LEFT JOIN dim_product dp ON st.sku = dp.sku AND dc.client_key = dp.client_key
    LEFT JOIN dim_date dd ON TRY_TO_DATE(so.order_date, 'YYYY-MM-DD') = dd.full_date
    LEFT JOIN dim_payment_type dpt ON st.payment_method = dpt.payment_method
)

SELECT * FROM order_items_with_keys
    )
;


  