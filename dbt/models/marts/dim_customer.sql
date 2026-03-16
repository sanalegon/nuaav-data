{{ config(
    materialized='table',
    tags=['dimension', 'core'],
    meta={
        'unique_constraint': ['customer_id', 'client_key']
    }
) }}

WITH staging_customers AS (
    SELECT * FROM {{ ref('stg_customer') }}
),

dim_client AS (
    SELECT * FROM {{ ref('dim_client') }}
),

customers_with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY sc.client_id, sc.customer_id) AS customer_key,
        sc.customer_id,
        dc.client_key,
        sc.first_name,
        sc.last_name,
        CONCAT(sc.first_name, ' ', sc.last_name) AS full_name,
        sc.email,
        CASE 
            WHEN sc.client_id = 'A' THEN 
                CASE sc.loyalty_tier
                    WHEN 'PLATINUM' THEN 'PLATINUM'
                    WHEN 'GOLD' THEN 'GOLD'
                    WHEN 'SILVER' THEN 'SILVER'
                    WHEN 'BRONZE' THEN 'BRONZE'
                    ELSE 'UNKNOWN'
                END
            WHEN sc.client_id = 'C' THEN
                CASE sc.loyalty_tier
                    WHEN 'VIP' THEN 'PLATINUM'
                    WHEN 'REGULAR' THEN 'SILVER'
                    WHEN 'NEW' THEN 'BRONZE'
                    ELSE 'UNKNOWN'
                END
            ELSE 'UNKNOWN'
        END AS loyalty_tier,
        sc.signup_source,
        sc.is_active,
        CASE 
            WHEN REGEXP_LIKE(sc.email, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$') THEN TRUE
            ELSE FALSE
        END AS is_email_valid,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM staging_customers sc
    INNER JOIN dim_client dc ON sc.client_id = dc.client_id
),

unknown_customer AS (
    -- Default record for unmatched/unknown customers
    SELECT
        -1 AS customer_key,
        'UNKNOWN' AS customer_id,
        1 AS client_key,  -- Assume client_key=1 is the base client
        'Unknown' AS first_name,
        'Customer' AS last_name,
        'Unknown Customer' AS full_name,
        'unknown@unknown.com' AS email,
        'UNKNOWN' AS loyalty_tier,
        'UNMATCHED' AS signup_source,
        FALSE AS is_active,
        FALSE AS is_email_valid,
        CURRENT_TIMESTAMP() AS loaded_at
),

final_customers AS (
    SELECT * FROM customers_with_keys
    UNION ALL
    SELECT * FROM unknown_customer
)

SELECT * FROM final_customers
