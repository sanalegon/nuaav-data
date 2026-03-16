
  create or replace   view NUAAV_DW.staging.stg_orders
  
  
  
  
  as (
    -- =============================================================================
-- MODEL      : stg_orders
-- PURPOSE    : Unified Orders staging for all clients (A, C, and future).
--              • Client A: strip inline annotation comments from CHANNEL field
--              • Client C: no special handling (no CHANNEL field)
--              • Union both clients with client_id tag
--              • Validate order_date (must be parseable as YYYY-MM-DD)
--              • Validate customer_id (must not be NULL or empty)
--              • Deduplicate per client per order_id; keep first occurrence
--              • Flag invalid records but filter them from output
-- =============================================================================

WITH client_a_raw AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        TRIM(order_date) AS order_date,
        TRIM(order_status) AS order_status,
        REGEXP_REPLACE(TRIM(channel), '\\s{2,}.*$', '') AS channel,  -- Strip "  <-- annotation"
        'A' AS client_id,
        _loaded_at
    FROM NUAAV_DW.RAW.CLIENT_A_ORDERS
),

client_c_raw AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        TRIM(order_date) AS order_date,
        TRIM(order_status) AS order_status,
        NULL::VARCHAR AS channel,  -- Not available in Client C; use NULL for compatibility
        'C' AS client_id,
        _loaded_at
    FROM NUAAV_DW.RAW.CLIENT_C_ORDERS
),

union_clients AS (
    SELECT * FROM client_a_raw
    UNION ALL
    SELECT * FROM client_c_raw
),

with_flags AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_status,
        channel,
        client_id,
        -- Date validation: try to parse order_date
        CASE WHEN TRY_TO_DATE(order_date, 'YYYY-MM-DD') IS NULL THEN TRUE
             ELSE FALSE
        END AS has_missing_date,
        -- Reference validation: will be verified later in mart via FK join
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN TRUE
             ELSE FALSE
        END AS has_invalid_ref,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY client_id, order_id ORDER BY _loaded_at) AS rn
    FROM union_clients
)

SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    channel,
    has_missing_date,
    has_invalid_ref,
    CASE WHEN rn > 1 THEN TRUE ELSE FALSE END AS is_duplicate,
    client_id,
    _loaded_at
FROM with_flags
WHERE rn = 1  -- Deduplicate: keep first occurrence per client per order_id
AND has_missing_date = FALSE  -- Exclude records with invalid/missing dates
AND has_invalid_ref = FALSE   -- Exclude records with missing customer references
ORDER BY client_id, order_id
  );

