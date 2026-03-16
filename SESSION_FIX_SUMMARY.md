# Session Summary: stg_transactions Fix

## Objective
Fix the `stg_transactions` dbt model to correctly parse and flatten XML (Client A) and JSON (Client C) transaction data.

## Issues Encountered & Solutions

### Issue 1: XML Parsing Failed
**Problem**: Initial approach using `XMLGET()` and `MODE => 'RECURSE'` with invalid `RECURSIVE_DEPTH` parameter caused compilation errors.

**Solution**: Implemented correct XML parsing:
- Use `PARSE_XML(_raw_xml)` to convert VARCHAR XML to OBJECT format
- Use `:"$"` accessor to get element contents
- Use `FLATTEN(input => xml_content:"$")` to iterate through children
- Use `GET(f.value, '@')` to filter by tag name

### Issue 2: Item Flattening Loss
**Problem**: Only 4 items from 2 transactions were being extracted despite 39 transactions existing.

**Root Cause**: The `WHERE GET(items_flatten.value, '@') = 'Item'` filter was too strict and filtering out valid items.

**Solution**: Removed the tag name filter since all children of `<Items>` are guaranteed to be `<Item>` elements.

### Issue 3: Orphaned Records
**Problem**: 6 rows had NULL `transaction_id` values after XML parsing, resulting in orphaned records.

**Solution**: Added filter in `final_dedup`:
```sql
WHERE is_duplicate = FALSE
AND transaction_id IS NOT NULL
AND TRIM(transaction_id) != ''
```

### Issue 4: Client C (JSON) Data Not Processing
**Problem**: JSON transactions array was not being flattened. Expected to see parsed transactions but getting 0 rows for Client C.

**Root Cause**: Raw JSON table contains 1 record with entire JSON structure. The transactions array (`_raw_json:transactions`) needs to be flattened first before parsing individual transactions.

**Current Status**: Attempting to fix with proper JSON array flattening.

## Current Data Status

### Client A (XML) - ✅ WORKING
- **Source**: 6 XML files (ClientA_Transactions_1.xml through _7.xml)
- **Raw Records**: 6 files loaded into raw table
- **Transactions Extracted**: 39 total, 33 after deduplication
- **Items Processed**: 197 line items
- **Anomaly Flags**: Applied (negative qty, negative amount, missing date, invalid refs, missing SKU)

### Client C (JSON) - ⚠️ IN PROGRESS
- **Source**: 1 JSON file (input_data/Client_B/transactions.json)
- **Raw Records**: 1 record in raw table (entire file)
- **Transactions Expected**: Multiple within `transactions[]` array
- **Current Status**: 0 rows extracted - flattening logic needs debugging

## Key Model Changes

### stg_transactions.sql Structure:
1. **CLIENT_A section**:
   - `client_a_raw_xml`: Load raw XML VARCHAR
   - `client_a_flattened_txns`: Parse XML and flatten to transaction elements
   - `client_a_parsed_txns`: Extract transaction-level fields
   - `client_a_flattened_items`: Flatten items array to line-item rows
   - `client_a_with_flags`: Apply anomaly flags

2. **CLIENT_C section**:
   - `client_c_raw_json`: Load raw JSON VARIANT
   - `client_c_flattened_txns`: **[NEEDS FIX]** Flatten transactions array
   - `client_c_parsed_json`: Extract transaction-level fields + nested objects
   - `client_c_flattened_items`: Flatten items array
   - `client_c_with_flags`: Apply anomaly flags

3. **UNION & DEDUP**:
   - `union_clients`: Union both clients
   - `final_dedup`: Filter duplicates and invalid records

## Next Steps

1. **Debug Client C JSON Flattening**:
   - The `LATERAL FLATTEN(input => _raw_json:transactions, MODE => 'ARRAY')` needs verification
   - Check if transactions array structure matches expectations
   - May need to use `_raw_json['transactions']` instead

2. **Validate Output**:
   - Run: `SELECT COUNT(*), COUNT(DISTINCT transaction_id) FROM staging.stg_transactions WHERE client_id = 'C';`
   - Expected: Multiple items from multiple transactions

3. **Final Verification**:
   - Check for orphaned/invalid records
   - Verify anomaly flags are applied correctly
   - Ensure column order matches expected schema

## Technical Notes

- **XML Handling**: Requires PARSE_XML + FLATTEN on `:"$"` operator
- **JSON Handling**: Requires arrays to be explicitly flattened before nested object access
- **Name Parsing**: Using SPLIT_PART for Client C customer_name splitting
- **Data Quality**: Anomaly flags track 6 types of issues for both clients
