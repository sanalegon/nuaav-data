-- Debug: Check product coverage in fact_orders after XML parsing fix
SELECT 
    COUNT(*) as total_items,
    COUNT(CASE WHEN product_key IS NOT NULL THEN 1 END) as matched_products,
    COUNT(CASE WHEN product_key IS NULL THEN 1 END) as missing_products,
    ROUND(100.0 * COUNT(CASE WHEN product_key IS NOT NULL THEN 1 END) / COUNT(*), 1) as match_pct
FROM NUAAV_DW.marts.fact_orders