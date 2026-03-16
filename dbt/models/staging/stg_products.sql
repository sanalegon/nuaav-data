-- =============================================================================
-- MODEL      : stg_products
-- PURPOSE    : Unified Products staging for all clients (A, C, and future).
--              • Client A: straightforward rename + price validation
--              • Client C: same logic
--              • Union both clients with client_id tag
--              • Flag invalid prices (≤ 0)
--              • Deduplicate per client per SKU
-- =============================================================================

WITH client_a_raw AS (
    SELECT
        TRIM(sku) AS sku,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(unit_price) AS unit_price_str,
        TRIM(currency) AS currency,
        CASE WHEN LOWER(TRIM(is_active)) IN ('true', '1', 'yes') THEN TRUE
             WHEN LOWER(TRIM(is_active)) IN ('false', '0', 'no') THEN FALSE
             ELSE NULL
        END AS is_active,
        'A' AS client_id,
        _loaded_at
    FROM {{ source('nuaav_raw', 'CLIENT_A_PRODUCTS') }}
),

client_c_raw AS (
    SELECT
        TRIM(sku) AS sku,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(unit_price) AS unit_price_str,
        TRIM(currency) AS currency,
        CASE WHEN LOWER(TRIM(is_active)) IN ('true', '1', 'yes') THEN TRUE
             WHEN LOWER(TRIM(is_active)) IN ('false', '0', 'no') THEN FALSE
             ELSE NULL
        END AS is_active,
        'C' AS client_id,
        _loaded_at
    FROM {{ source('nuaav_raw', 'CLIENT_C_PRODUCTS') }}
),

union_clients AS (
    SELECT * FROM client_a_raw
    UNION ALL
    SELECT * FROM client_c_raw
),

with_casts AS (
    SELECT
        sku,
        product_name,
        category,
        TRY_TO_NUMBER(unit_price_str, 12, 2) AS unit_price,
        currency,
        is_active,
        client_id,
        _loaded_at
    FROM union_clients
),

with_flags AS (
    SELECT
        sku,
        product_name,
        category,
        unit_price,
        currency,
        is_active,
        client_id,
        -- Flag invalid prices: <= 0
        CASE WHEN unit_price IS NULL OR unit_price <= 0 THEN FALSE
             ELSE TRUE
        END AS is_price_valid,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY client_id, sku ORDER BY _loaded_at) AS rn
    FROM with_casts
)

SELECT
    sku,
    product_name,
    category,
    unit_price,
    currency,
    is_active,
    is_price_valid,
    client_id,
    _loaded_at
FROM with_flags
WHERE rn = 1  -- Deduplicate: keep first occurrence per client per SKU
ORDER BY client_id, sku
