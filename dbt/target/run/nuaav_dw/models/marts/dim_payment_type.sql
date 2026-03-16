
  
    

create or replace transient table NUAAV_DW.marts.dim_payment_type
    
    
    
    as (

SELECT
    ROW_NUMBER() OVER (ORDER BY payment_method) AS payment_type_key,
    payment_method,
    CURRENT_TIMESTAMP() AS loaded_at
FROM (
    SELECT 'BankTransfer' AS payment_method
    UNION ALL
    SELECT 'CreditCard'
    UNION ALL
    SELECT 'PayPal'
    UNION ALL
    SELECT 'Unknown'
)
ORDER BY payment_method
    )
;


  