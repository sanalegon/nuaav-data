

WITH staging_products AS (
    SELECT * FROM NUAAV_DW.staging.stg_products
),

dim_client AS (
    SELECT * FROM NUAAV_DW.marts.dim_client
),

products_with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY sp.client_id, sp.sku) AS product_key,
        sp.sku,
        dc.client_key,
        sp.product_name,
        sp.category,
        sp.unit_price,
        sp.currency,
        sp.is_active,
        CASE WHEN sp.unit_price <= 0 THEN FALSE ELSE TRUE END AS is_price_valid,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM staging_products sp
    INNER JOIN dim_client dc ON sp.client_id = dc.client_id
),

unknown_product AS (
    -- Default record for unmatched/unknown products
    SELECT
        -1 AS product_key,
        'UNKNOWN' AS sku,
        1 AS client_key,  -- Assume client_key=1 is the base client
        'Unknown Product' AS product_name,
        'UNMATCHED' AS category,
        0.00 AS unit_price,
        'USD' AS currency,
        FALSE AS is_active,
        FALSE AS is_price_valid,
        CURRENT_TIMESTAMP() AS loaded_at
),

final_products AS (
    SELECT * FROM products_with_keys
    UNION ALL
    SELECT * FROM unknown_product
)

SELECT * FROM final_products