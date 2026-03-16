-- =============================================================================
-- MODEL      : stg_transactions
-- PURPOSE    : Unified Transactions staging for all clients (A, C, and future).
--              • Client A: Parse XML transactions (XMLGET); flatten Items array
--              • Client C: Parse JSON transactions (JSON accessor); flatten items array
--              • Both: Flatten items array to line-item rows, filter NULL items
--              • Union both clients with client_id tag
--              • Validate transaction_id presence and not empty
--              • Deduplicate per client with natural key (txn_id, cust_id, order_id, sku, qty, price)
--
-- STRUCTURE NOTES:
--   Client A (XML):
--     - Root: <SalesData client="ClientA" generatedAt="...">
--     - Each: <Transaction> with <TransactionID>, <Order>, <Items>, <Payment>
--     - Customer, Order, Payment are SINGULAR (not arrays)
--     - Only Items contains multiple <Item> elements
--     - Timestamp: batch-level (generatedAt), not per transaction
--   Client C (JSON):
--     - Root: {client, batchTimestamp, transactions: [...]}
--     - Each transaction: {id, order{id, date, customer{id, name, email}}, items[], payment}
--     - Customer is nested in Order (not separate)
--     - Price is nested object {amount, currency}
--     - No per-transaction timestamp
-- =============================================================================

-- ============================================================================
-- CLIENT A: XML PARSING
-- ============================================================================
WITH client_a_raw_xml AS (
    SELECT
        _source_file,
        PARSE_XML(_raw_xml) AS xml_content,
        _loaded_at
    FROM NUAAV_DW.RAW.CLIENT_A_TRANSACTIONS_XML
),

client_a_flattened_txns AS (
    SELECT
        _source_file,
        _loaded_at,
        f.value AS transaction_node 
    FROM client_a_raw_xml,
    LATERAL FLATTEN(input => xml_content:"$") f
    WHERE GET(f.value, '@') = 'Transaction'
),

client_a_parsed_txns AS (
    SELECT
        XMLGET(transaction_node, 'TransactionID'):"$"::STRING AS transaction_id,
        XMLGET(XMLGET(transaction_node, 'Order'), 'OrderID'):"$"::STRING AS order_id,
        XMLGET(XMLGET(transaction_node, 'Order'), 'OrderDate'):"$"::STRING AS order_date,
        XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'CustomerID'):"$"::STRING AS customer_id,
        -- Handle both <LastName> and <LastLastName> anomaly (with :"$" accessor)
        COALESCE(
            XMLGET(XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Name'), 'LastName'):"$"::STRING,
            XMLGET(XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Name'), 'LastLastName'):"$"::STRING
        ) AS last_name,
        XMLGET(XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Name'), 'FirstName'):"$"::STRING AS first_name,
        XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Email'):"$"::STRING AS email,
        XMLGET(XMLGET(transaction_node, 'Payment'), 'Method'):"$"::STRING AS payment_method,
        TRY_TO_NUMBER(XMLGET(XMLGET(transaction_node, 'Payment'), 'Amount'):"$"::STRING, 14, 2) AS payment_amount,
        XMLGET(transaction_node, 'Items') AS items_container,
        'A' AS client_id,
        ROW_NUMBER() OVER (PARTITION BY XMLGET(transaction_node, 'TransactionID'):"$" ORDER BY _loaded_at) AS txn_rn,
        _source_file,
        _loaded_at
    FROM client_a_flattened_txns
),

client_a_parsed_txns_alt AS (
    SELECT
        XMLGET(transaction_node, 'TransactionID'):"$"::STRING AS transaction_id,
        XMLGET(XMLGET(transaction_node, 'Order'), 'OrderID'):"$"::STRING AS order_id,
        XMLGET(XMLGET(transaction_node, 'Order'), 'OrderDate'):"$"::STRING AS order_date,
        XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'CustomerID'):"$"::STRING AS customer_id,
        -- Handle both <LastName> and <LastLastName> anomaly (without :"$" accessor)
        COALESCE(
            XMLGET(XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Name'), 'LastName'):"$"::STRING,
            XMLGET(XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Name'), 'LastLastName'):"$"::STRING
        ) AS last_name,
        XMLGET(XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Name'), 'FirstName'):"$"::STRING AS first_name,
        XMLGET(XMLGET(XMLGET(transaction_node, 'Order'), 'Customer'), 'Email'):"$"::STRING AS email,
        XMLGET(XMLGET(transaction_node, 'Payment'), 'Method'):"$"::STRING AS payment_method,
        TRY_TO_NUMBER(XMLGET(XMLGET(transaction_node, 'Payment'), 'Amount'):"$"::STRING, 14, 2) AS payment_amount,
        XMLGET(transaction_node, 'Items') AS items_container,
        'A' AS client_id,
        ROW_NUMBER() OVER (PARTITION BY XMLGET(transaction_node, 'TransactionID'):"$" ORDER BY _loaded_at) AS txn_rn,
        _source_file,
        _loaded_at
    FROM client_a_flattened_txns
),

client_a_all_parsed_txns AS (
    SELECT * FROM client_a_parsed_txns
    UNION ALL
    SELECT * FROM client_a_parsed_txns_alt
),

client_a_flattened_items AS (
    SELECT
        transaction_id,
        customer_id,
        first_name,
        last_name,
        email,
        order_id,
        order_date,
        payment_method,
        payment_amount,
        -- Extracting values directly from the flattened item node
        XMLGET(item.value, 'SKU'):"$"::STRING AS sku,
        XMLGET(item.value, 'Description'):"$"::STRING AS description,
        ABS(TRY_TO_NUMBER(XMLGET(item.value, 'Quantity'):"$"::STRING)) AS quantity,
        TRY_TO_NUMBER(XMLGET(item.value, 'UnitPrice'):"$"::STRING, 12, 2) AS unit_price,
        client_id,
        txn_rn,
        DENSE_RANK() OVER (PARTITION BY transaction_id ORDER BY item.seq) AS item_rn,
        _source_file,
        _loaded_at
    FROM client_a_parsed_txns,
    -- CTE1: Use :"$" for proper array handling (works well for multi-item transactions)
    LATERAL FLATTEN(input => client_a_parsed_txns.items_container:"$") item
),

-- New CTE for single-item transactions without :"$" accessor
client_a_flattened_items_single AS (
    SELECT
        transaction_id,
        customer_id,
        first_name,
        last_name,
        email,
        order_id,
        order_date,
        payment_method,
        payment_amount,
        -- Extracting values directly from the flattened item node (no :"$")
        XMLGET(item.value, 'SKU'):"$"::STRING AS sku,
        XMLGET(item.value, 'Description'):"$"::STRING AS description,
        ABS(TRY_TO_NUMBER(XMLGET(item.value, 'Quantity'):"$"::STRING)) AS quantity,
        TRY_TO_NUMBER(XMLGET(item.value, 'UnitPrice'):"$"::STRING, 12, 2) AS unit_price,
        client_id,
        txn_rn,
        0 AS item_rn,  -- Will be deduplicated later
        _source_file,
        _loaded_at
    FROM client_a_parsed_txns,
    -- CTE2: Without :"$" for single-item transactions (catches items lost by CTE1)
    LATERAL FLATTEN(input => client_a_parsed_txns.items_container) item
),

-- Union both approaches and let QUALIFY deduplicate them later
client_a_all_items AS (
    SELECT * FROM client_a_flattened_items
    UNION ALL
    SELECT * FROM client_a_flattened_items_single
),

client_a_with_flags AS (
    SELECT
        transaction_id,
        customer_id,
        email,
        first_name,
        last_name,
        order_id,
        order_date,
        payment_method,
        payment_amount,
        sku,
        quantity,
        unit_price,
        client_id,
        _source_file,
        _loaded_at
    FROM client_a_all_items
    WHERE NOT (sku IS NULL AND description IS NULL AND quantity IS NULL AND unit_price IS NULL)
),

-- ============================================================================
-- CLIENT C: JSON PARSING
-- ============================================================================
client_c_raw_json AS (
    SELECT
        _source_file,
        _raw_json,
        _loaded_at
    FROM NUAAV_DW.RAW.CLIENT_C_TRANSACTIONS_JSON
),

client_c_flattened_txns AS (
    SELECT
        _source_file,
        _loaded_at,
        txns.value AS transaction_obj
    FROM client_c_raw_json,
    LATERAL FLATTEN(input => _raw_json:transactions, MODE => 'ARRAY') txns
),

client_c_parsed_json AS (
    SELECT
        transaction_obj:id::STRING AS transaction_id,
        transaction_obj:order.date::STRING AS order_date,
        transaction_obj:order.customer.id::STRING AS customer_id,
        transaction_obj:order.customer.name::STRING AS customer_name,
        transaction_obj:order.customer.email::STRING AS email,
        transaction_obj:order.id::STRING AS order_id,
        transaction_obj:payment.method::STRING AS payment_method,
        TRY_TO_NUMBER(transaction_obj:payment.total::STRING, 14, 2) AS payment_amount,
        transaction_obj:items AS items_array,
        'C' AS client_id,
        _source_file,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY transaction_obj:id ORDER BY _loaded_at) AS txn_rn
    FROM client_c_flattened_txns
),

client_c_filtered_txns AS (
    SELECT *
    FROM client_c_parsed_json
    WHERE ARRAY_SIZE(items_array) > 0 OR txn_rn = 1
),

client_c_flattened_items AS (
    SELECT
        transaction_id,
        customer_id,
        email,
        SPLIT_PART(customer_name, ' ', 1) AS first_name,
        SPLIT_PART(customer_name, ' ', -1) AS last_name,
        order_id,
        order_date,
        payment_method,
        payment_amount,
        item.value:sku::STRING AS sku,
        ABS(TRY_TO_NUMBER(item.value:qty::STRING)) AS quantity,
        TRY_TO_NUMBER(item.value:price.amount::STRING, 12, 2) AS unit_price,
        client_id,
        txn_rn,
        ROW_NUMBER() OVER (PARTITION BY transaction_id, item.value ORDER BY _loaded_at) AS item_rn,
        _source_file,
        _loaded_at
    FROM client_c_filtered_txns,
    LATERAL FLATTEN(input => client_c_filtered_txns.items_array) item 
),

client_c_with_flags AS (
    SELECT
        transaction_id,
        customer_id,
        email,
        first_name,
        last_name,
        order_id,
        order_date,
        payment_method,
        payment_amount,
        sku,
        quantity,
        unit_price,
        client_id,
        _source_file,
        _loaded_at
    FROM client_c_flattened_items
),

-- ============================================================================
-- UNION AND FINAL DEDUP
-- ============================================================================
union_clients AS (
    SELECT * FROM client_a_with_flags
    UNION ALL
    SELECT * FROM client_c_with_flags
),

final_dedup AS (
    SELECT
        transaction_id,
        customer_id,
        email,
        first_name,
        last_name,
        order_id,
        order_date,
        payment_method,
        payment_amount,
        sku,
        quantity,
        unit_price,
        client_id,
        _source_file,
        _loaded_at
    FROM union_clients
    WHERE transaction_id IS NOT NULL
    AND TRIM(transaction_id) != ''
)

SELECT * FROM final_dedup
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id, customer_id, order_id, sku, quantity, unit_price ORDER BY transaction_id) = 1
ORDER BY client_id, transaction_id