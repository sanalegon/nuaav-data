
  create or replace   view NUAAV_DW.staging.stg_payments
  
  
  
  
  as (
    -- =============================================================================
-- MODEL      : stg_payments
-- PURPOSE    : Unified Payments staging for all clients (C currently available).
--              • Currently sources Client C only (RAW.CLIENT_C_PAYMENTS)
--              • Smart negative amount flagging: valid if status=REFUNDED
--              • Deduplicate per client per payment_id
--              • Designed to scale when future clients provide payment data
-- =============================================================================

WITH client_c_raw AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        TRIM(payment_method) AS payment_method,
        TRIM(amount) AS amount_str,
        TRIM(currency) AS currency,
        TRIM(status) AS status,
        'C' AS client_id,
        _loaded_at
    FROM NUAAV_DW.RAW.CLIENT_C_PAYMENTS
),

with_casts AS (
    SELECT
        payment_id,
        order_id,
        payment_method,
        TRY_TO_NUMBER(amount_str, 14, 2) AS amount,
        currency,
        status,
        client_id,
        _loaded_at
    FROM client_c_raw
),

with_flags AS (
    SELECT
        payment_id,
        order_id,
        payment_method,
        amount,
        currency,
        status,
        client_id,
        -- Flag negative amounts: negative is valid if status = REFUNDED
        CASE WHEN amount IS NULL THEN FALSE
             WHEN amount < 0 AND UPPER(TRIM(status)) != 'REFUNDED' THEN TRUE
             WHEN amount = 0 AND UPPER(TRIM(status)) NOT IN ('REFUNDED', 'CANCELLED') THEN TRUE
             ELSE FALSE
        END AS has_negative_amt,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY client_id, payment_id ORDER BY _loaded_at) AS rn
    FROM with_casts
)

SELECT
    payment_id,
    order_id,
    payment_method,
    amount,
    currency,
    status,
    has_negative_amt,
    client_id,
    _loaded_at
FROM with_flags
WHERE rn = 1  -- Deduplicate: keep first occurrence per client per payment_id
ORDER BY client_id, payment_id
  );

