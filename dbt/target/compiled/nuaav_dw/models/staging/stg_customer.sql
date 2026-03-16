-- =============================================================================
-- MODEL      : stg_customer
-- PURPOSE    : Unified Customer staging for all clients (A, C, and future).
--              • Client A: straightforward rename + loyalty tier handling
--              • Client C: split CUSTOMER_NAME into first/last, map segment to tier
--              • Union both clients with client_id tag
--              • Validate email format
--              • Deduplicate per client per customer_id
-- =============================================================================

WITH client_a_raw AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(email) AS email,
        COALESCE(NULLIF(TRIM(loyalty_tier), ''), 'UNKNOWN') AS loyalty_tier,
        TRIM(signup_source) AS signup_source,
        CASE WHEN LOWER(TRIM(is_active)) IN ('true', '1', 'yes') THEN TRUE
             WHEN LOWER(TRIM(is_active)) IN ('false', '0', 'no') THEN FALSE
             ELSE NULL
        END AS is_active,
        'A' AS client_id,
        _loaded_at
    FROM NUAAV_DW.RAW.CLIENT_A_CUSTOMERS
),

client_c_raw AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        CASE WHEN customer_name IS NULL OR TRIM(customer_name) = '' THEN NULL
             ELSE TRIM(SPLIT_PART(customer_name, ' ', 1))
        END AS first_name,
        CASE WHEN customer_name IS NULL OR TRIM(customer_name) = '' THEN NULL
             WHEN ARRAY_SIZE(SPLIT(TRIM(customer_name), ' ')) > 1
             THEN TRIM(SPLIT_PART(customer_name, ' ', 2))
             ELSE NULL
        END AS last_name,
        TRIM(email) AS email,
        -- Map Client C segment to canonical loyalty tier
        CASE WHEN UPPER(TRIM(segment)) = 'VIP' THEN 'PLATINUM'
             WHEN UPPER(TRIM(segment)) = 'REGULAR' THEN 'SILVER'
             WHEN UPPER(TRIM(segment)) = 'NEW' THEN 'BRONZE'
             WHEN segment IS NULL OR TRIM(segment) = '' THEN 'UNKNOWN'
             ELSE 'UNKNOWN'
        END AS loyalty_tier,
        NULL::VARCHAR AS signup_source,  -- Not available in Client C
        CASE WHEN LOWER(TRIM(is_active)) IN ('true', '1', 'yes') THEN TRUE
             WHEN LOWER(TRIM(is_active)) IN ('false', '0', 'no') THEN FALSE
             ELSE NULL
        END AS is_active,
        'C' AS client_id,
        _loaded_at
    FROM NUAAV_DW.RAW.CLIENT_C_CUSTOMERS
),

union_clients AS (
    SELECT * FROM client_a_raw
    UNION ALL
    SELECT * FROM client_c_raw
),

with_flags AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        loyalty_tier,
        signup_source,
        is_active,
        client_id,
        -- Email validation: check for pattern user@domain.tld
        CASE WHEN email IS NULL OR email = '' THEN FALSE
             WHEN email REGEXP '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$' THEN TRUE
             ELSE FALSE
        END AS is_email_valid,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY client_id, customer_id ORDER BY _loaded_at) AS rn
    FROM union_clients
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_tier,
    signup_source,
    is_active,
    is_email_valid,
    client_id,
    _loaded_at
FROM with_flags
WHERE rn = 1  -- Deduplicate: keep first occurrence per client per customer_id
ORDER BY client_id, customer_id