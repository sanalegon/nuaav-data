
  
    

create or replace transient table NUAAV_DW.marts.fact_transactions
    
    
    
    as (

-- Transaction header fact table: one row per transaction/batch
-- Grain: Transaction ID
-- Purpose: Analyze transactions by customer, channel, payment method with aggregated metrics

WITH stg_transactions AS (
    SELECT * FROM NUAAV_DW.staging.stg_transactions
),

dim_client AS (
    SELECT * FROM NUAAV_DW.marts.dim_client
),

dim_customer AS (
    SELECT * FROM NUAAV_DW.marts.dim_customer
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

-- Aggregate to transaction level (header info only)
transaction_aggregates AS (
    SELECT
        st.transaction_id,
        st.order_id,
        st.client_id,
        st.customer_id,
        st.payment_method,
        so.order_status,
        so.order_date,
        MAX(so.channel) AS order_channel,
        COUNT(DISTINCT st.order_id) AS order_count,
        COUNT(*) AS item_count,
        SUM(st.quantity) AS total_quantity,
        ABS(SUM(COALESCE(st.quantity * st.unit_price, 0))) AS total_line_amount,
        ABS(MAX(st.payment_amount)) AS payment_amount,
        MAX(st._source_file) AS source_file,
        MAX(st._loaded_at) AS loaded_at
    FROM stg_transactions st
    LEFT JOIN stg_orders so ON st.order_id = so.order_id
    WHERE st.transaction_id IS NOT NULL
    GROUP BY st.transaction_id, st.order_id, st.client_id, st.customer_id, st.payment_method, so.order_status, so.order_date, so.channel
),

transactions_with_keys AS (
    SELECT
        ta.transaction_id,
        ta.order_id,
        dc.client_key,
        COALESCE(dcust.customer_key, -1) AS customer_key,  -- Default to -1 (Unknown) if unmatched
        COALESCE(dd.date_key, -1) AS date_key,  -- Default to -1 (Unknown) if unmatched
        COALESCE(dpt.payment_type_key, (SELECT payment_type_key FROM dim_payment_type WHERE payment_method = 'Unknown')) AS payment_type_key,
        COALESCE(ta.order_status, 'Unknown') AS order_status,  -- Default to 'Unknown' if NULL
        COALESCE(ta.order_channel, 'Unknown') AS order_channel,  -- Default to 'Unknown' if NULL
        ta.order_count,
        ta.item_count,
        ta.total_quantity,
        ta.total_line_amount,
        ta.payment_amount,
        -- Data quality flags for dimension matching (identify records that used default unknown records)
        CASE WHEN dcust.customer_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_customer,
        CASE WHEN dd.date_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_date,
        ta.source_file,
        ta.loaded_at
    FROM transaction_aggregates ta
    INNER JOIN dim_client dc ON ta.client_id = dc.client_id
    LEFT JOIN dim_customer dcust ON ta.customer_id = dcust.customer_id AND dc.client_key = dcust.client_key
    LEFT JOIN dim_date dd ON TRY_TO_DATE(ta.order_date, 'YYYY-MM-DD') = dd.full_date
    LEFT JOIN dim_payment_type dpt ON ta.payment_method = dpt.payment_method
)

SELECT * FROM transactions_with_keys
    )
;


  