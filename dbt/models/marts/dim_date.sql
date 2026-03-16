{{ config(
    materialized='table',
    tags=['dimension', 'core']
) }}

-- Generate date spine with recursive CTE and include Unknown record
WITH RECURSIVE date_spine(full_date) AS (
    SELECT '2025-11-01'::DATE AS full_date
    UNION ALL
    SELECT DATEADD(DAY, 1, full_date)
    FROM date_spine
    WHERE full_date < '2025-12-31'
),

valid_dates AS (
    SELECT
        TO_CHAR(full_date, 'YYYYMMDD')::INT AS date_key,
        full_date,
        YEAR(full_date) AS year,
        QUARTER(full_date) AS quarter,
        MONTH(full_date) AS month,
        TO_CHAR(full_date, 'MMMM') AS month_name,
        WEEKOFYEAR(full_date) AS week_of_year,
        DAY(full_date) AS day_of_month,
        DAYOFWEEK(full_date) AS day_of_week,
        TO_CHAR(full_date, 'DDDD') AS day_name,
        CASE WHEN DAYOFWEEK(full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM date_spine
),

unknown_date AS (
    SELECT
        -1 AS date_key,
        NULL::DATE AS full_date,
        NULL::INT AS year,
        NULL::INT AS quarter,
        NULL::INT AS month,
        'Unknown' AS month_name,
        NULL::INT AS week_of_year,
        NULL::INT AS day_of_month,
        NULL::INT AS day_of_week,
        'Unknown' AS day_name,
        NULL::BOOLEAN AS is_weekend,
        CURRENT_TIMESTAMP() AS loaded_at
),

final_result AS (
    SELECT * FROM valid_dates
    UNION ALL
    SELECT * FROM unknown_date
)

SELECT * FROM final_result
ORDER BY date_key
