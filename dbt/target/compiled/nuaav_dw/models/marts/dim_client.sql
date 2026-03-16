

SELECT
    ROW_NUMBER() OVER (ORDER BY client_id) AS client_key,
    client_id,
    client_name,
    source_folder,
    is_active,
    CURRENT_TIMESTAMP() AS loaded_at
FROM (
    SELECT 'A' AS client_id, 'Client A' AS client_name, 'input_data/' AS source_folder, TRUE AS is_active
    UNION ALL
    SELECT 'B' AS client_id, 'Client B' AS client_name, 'input_data/' AS source_folder, FALSE AS is_active
    UNION ALL
    SELECT 'C' AS client_id, 'Client C' AS client_name, 'input_data/Client_B/' AS source_folder, TRUE AS is_active
)