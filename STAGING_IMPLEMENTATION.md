# Staging Layer Implementation – Complete Summary

**Date**: March 14, 2026  
**Status**: ✅ All staging models implemented and ready for dbt run  
**Models Created**: 9 staging models (4 for Client A, 5 for Client C)

---

## Overview

The staging layer (`NUAAV_DW.STAGING`) now contains dbt views that transform raw landing zone data from S3 into clean, conformed datasets. All models:

1. **Clean**: Remove duplicates, strip annotations, trim whitespace
2. **Validate**: Parse dates, validate emails, check price validity
3. **Flag**: Apply boolean anomaly columns per the [01_anomaly_report.md](docs/01_anomaly_report.md)
4. **Denormalize**: Flatten semi-structured data (XML/JSON) to line-item format
5. **Deduplicate**: Keep first occurrence by natural key; suppress duplicates

---

## Implementation Details by Model

### Client A Models (4 files)

#### 1. [`stg_client_a__customer.sql`](dbt/models/staging/client_a/stg_client_a__customer.sql)
- **Source**: `RAW.CLIENT_A_CUSTOMERS`
- **Grain**: 1 row per customer_id
- **Key transformations**:
  - Handle missing loyalty_tier → set to `'UNKNOWN'`
  - Validate email regex: `^[^@\s]+@[^@\s]+\.[^@\s]+$`
  - Deduplicate on customer_id (keep first by load timestamp)
- **Output columns**: customer_id, first_name, last_name, email, loyalty_tier, signup_source, is_active, is_email_valid, CLIENT_LABEL, _loaded_at

#### 2. [`stg_client_a__orders.sql`](dbt/models/staging/client_a/stg_client_a__orders.sql)
- **Source**: `RAW.CLIENT_A_ORDERS`
- **Grain**: 1 row per order_id
- **Key transformations**:
  - Strip inline annotations: `REGEXP_REPLACE(channel, '\\s{2,}.*$', '')` removes "  <-- comment text"
  - Validate order_date format (YYYY-MM-DD)
  - Check for orphaned customer_id (flag as has_invalid_ref = TRUE)
  - Deduplicate on order_id
- **Output columns**: order_id, customer_id, order_date, order_status, channel, has_missing_date, has_invalid_ref, is_duplicate, CLIENT_LABEL, _loaded_at

#### 3. [`stg_client_a__products.sql`](dbt/models/staging/client_a/stg_client_a__products.sql)
- **Source**: `RAW.CLIENT_A_PRODUCTS`
- **Grain**: 1 row per SKU
- **Key transformations**:
  - Cast unit_price VARCHAR → NUMBER(12,2)
  - Flag invalid prices: is_price_valid = FALSE if price ≤ 0 or NULL
  - Deduplicate on SKU (keep first)
- **Output columns**: sku, product_name, category, unit_price, currency, is_active, is_price_valid, CLIENT_LABEL, _loaded_at

#### 4. [`stg_client_a__transactions.sql`](dbt/models/staging/client_a/stg_client_a__transactions.sql)
- **Source**: `RAW.CLIENT_A_TRANSACTIONS_XML` (VARIANT XML data)
- **Grain**: 1 row per item within a transaction (flattened)
- **Key transformations**:
  - **XML Parsing**: Use `XMLGET()` to extract nested transaction envelope (customer, order, payment objects)
  - **Array Flattening**: `LATERAL FLATTEN` on items array to expand each item to its own row
  - **Field extraction**: Parse payment and item-level fields from XML
  - **Deduplication**: `ROW_NUMBER() OVER (PARTITION BY transaction_id)` to identify duplicates; keep first occurrence
  - **Anomaly flags**:
    - is_duplicate: TRUE if transaction_id appears multiple times
    - has_negative_qty: TRUE if quantity < 0
    - has_negative_amt: TRUE if payment_amount < 0
    - has_missing_date: TRUE if timestamp unparseable
    - has_invalid_ref: TRUE if order_id is NULL/empty (critical for FK)
    - has_missing_sku: TRUE if sku is NULL/empty
- **Output columns** (21): transaction_id, customer_id, email, first_name, last_name, order_id, order_channel, order_status, payment_method, payment_amount, item_id, sku, quantity, unit_price, line_total, timestamp_str, 6 flag columns, CLIENT_LABEL, _source_file, _loaded_at

---

### Client C Models (5 files)

#### 1. [`stg_client_c__customer.sql`](dbt/models/staging/client_c/stg_client_c__customer.sql)
- **Source**: `RAW.CLIENT_C_CUSTOMERS`
- **Grain**: 1 row per customer_id
- **Key transformations**:
  - **Name splitting**: Parse CUSTOMER_NAME → first_name (word 1), last_name (word 2)
  - **Loyalty mapping**, Client C segment → canonical tier:
    - `'VIP'` → `'PLATINUM'`
    - `'REGULAR'` → `'SILVER'`
    - `'NEW'` → `'BRONZE'`
    - NULL / empty / unknown → `'UNKNOWN'`
  - Validate email regex (same as Client A)
  - Deduplicate on customer_id
- **Output columns**: customer_id, first_name, last_name, email, loyalty_tier, signup_source (NULL), is_active, is_email_valid, CLIENT_LABEL, _loaded_at

#### 2. [`stg_client_c__orders.sql`](dbt/models/staging/client_c/stg_client_c__orders.sql)
- **Source**: `RAW.CLIENT_C_ORDERS`
- **Grain**: 1 row per order_id
- **Key transformations**:
  - Validate order_date (no annotations to strip; simpler than Client A)
  - Check orphaned customer_id
  - Deduplicate on order_id
- **Output columns**: order_id, customer_id, order_date, order_status, channel (NULL), has_missing_date, has_invalid_ref, is_duplicate, CLIENT_LABEL, _loaded_at

#### 3. [`stg_client_c__products.sql`](dbt/models/staging/client_c/stg_client_c__products.sql)
- **Source**: `RAW.CLIENT_C_PRODUCTS`
- **Grain**: 1 row per SKU
- **Key transformations**:
  - Cast unit_price to NUMBER(12,2)
  - Flag invalid prices (≤ 0)
  - Deduplicate on SKU
- **Output columns**: sku, product_name, category, unit_price, currency, is_active, is_price_valid, CLIENT_LABEL, _loaded_at
- **Note**: Handles Client C anomaly of C-SKU-003 (exact duplicate row) → dropped during dedup

#### 4. [`stg_client_c__payments.sql`](dbt/models/staging/client_c/stg_client_c__payments.sql)
- **Source**: `RAW.CLIENT_C_PAYMENTS`
- **Grain**: 1 row per payment_id
- **Key transformations**:
  - Cast amount to NUMBER(14,2)
  - Flag negative amounts **only if** status ≠ 'REFUNDED'
  - Flag zero amounts if status ≠ ('REFUNDED' | 'CANCELLED')
  - Deduplicate on payment_id
- **Output columns**: payment_id, order_id, payment_method, amount, currency, status, has_negative_amt, CLIENT_LABEL, _loaded_at
- **Note**: Handles refunded payments (negative amounts are valid per business rule)

#### 5. [`stg_client_c__transactions.sql`](dbt/models/staging/client_c/stg_client_c__transactions.sql)
- **Source**: `RAW.CLIENT_C_TRANSACTIONS_JSON` (VARIANT JSON data)
- **Grain**: 1 row per item within a transaction (flattened)
- **Key transformations**:
  - **JSON Parsing**: Use JSON accessor `:field::STRING` to extract transaction envelope
  - **Array Flattening**: `LATERAL FLATTEN` on items array
  - **Empty items handling**: Filter out transactions with empty items array (considered duplicates)
  - **Deduplication**: Same window function approach; `is_duplicate = TRUE` for duplicates
  - **Anomaly flags**: Same 6 flags as Client A transactions
- **Output columns** (21): Same as Client A transactions
- **Note**: Handles Client C duplicate C-TXN-3001 where second occurrence has empty items array

---

## Anomaly Handling Summary

| Anomaly Type | Client A Handling | Client C Handling |
|--------------|-------------------|-------------------|
| **Duplicate records** | `is_duplicate = TRUE` on 2nd occurrence; drop | `is_duplicate = TRUE` on 2nd; drop (or marked if empty items) |
| **Missing loyalty tier** | Set to `'UNKNOWN'` | Segment NULL → `'UNKNOWN'` loyalty tier |
| **Invalid customer ref** | `has_invalid_ref = TRUE`; keep row | `has_invalid_ref = TRUE`; keep row |
| **Missing order date** | `has_missing_date = TRUE`; keep row | `has_missing_date = TRUE`; keep row |
| **Negative price** | `is_price_valid = FALSE`; keep row | `is_price_valid = FALSE`; keep row |
| **Negative quantity** | `has_negative_qty = TRUE`; keep row | (same) |
| **Negative amount** | `has_negative_amt = TRUE`; keep row | `has_negative_amt = FALSE` if REFUNDED status |
| **Missing SKU** | `has_missing_sku = TRUE`; keep row | (same) |
| **Invalid email** | `is_email_valid = FALSE`; keep row | (same) |
| **Inline annotations** | Strip with REGEXP_REPLACE | N/A (not in Client C orders) |
| **Name parsing** | N/A (Client A has split names) | Split CUSTOMER_NAME → first/last |

---

## Schema Changes

### Updated Files

1. **[dbt/models/staging/client_a/schema.yml](dbt/models/staging/client_a/schema.yml)**
   - Added `sources` block defining RAW tables (CLIENT_A_CUSTOMERS, CLIENT_A_ORDERS, CLIENT_A_PRODUCTS, CLIENT_A_TRANSACTIONS_XML, plus all Client C RAW tables)
   - Added full `columns` definitions for all 4 Client A staging models with data_type, description, and grain documentation

2. **[dbt/models/staging/client_c/schema.yml](dbt/models/staging/client_c/schema.yml)**
   - Added full `columns` definitions for all 5 Client C staging models
   - Documented Client B naming discrepancy resolution

3. **[project_documentation/data_model.md](project_documentation/data_model.md)**
   - Updated data lineage diagram to emphasize S3 → external stage → COPY INTO flow
   - Added AWS/S3 context: bucket path, stage names, credential management via env vars
   - Clarified that COPY INTO is manual trigger (user uploads to S3, then runs COPY commands from 01_raw_landing.sql)

---

## dbt Execution Path

**To materialize staging views**:

```bash
# From project root
cd dbt

# Set environment variables (or use Snowflake Secrets Manager)
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_DATABASE="NUAAV_DW"
export SNOWFLAKE_WAREHOUSE="NUAAV_WH"

# Debug connection
dbt debug

# Run staging models only (as views)
dbt run --select staging.*

# Or run all models (staging + marts)
dbt run
```

**Outputs**:
- 9 views in `NUAAV_DW.STAGING` schema (dbt materializes as `view` per dbt_project.yml)
- All views ready to feed mart layer (dim_customer, dim_product, fact_orders, fact_transactions)

---

## Key Design Decisions

1. **Flag-in-place anomalies**: All rows retained with boolean flags. Analysts use WHERE clauses to filter cleandata:
   ```sql
   SELECT * FROM NUAAV_DW.MARTS.FACT_TRANSACTIONS
   WHERE is_duplicate = FALSE 
     AND has_negative_amt = FALSE 
     AND has_missing_sku = FALSE;
   ```

2. **XML/JSON flattening at staging**: Transaction items are expanded to line-item rows here (not in marts), allowing fact_transactions to be 1:1 per item. Staging models control how semi-structured data is normalized.

3. **Client A & C naming**: Both use `stg_<client>__<entity>` pattern (singular entity names). Union happens in marts layer (dim_customer = union of stg_client_a__customer + stg_client_c__customer).

4. **1:N FK enforcement**: `has_invalid_ref = TRUE` flag on transactions/orders with orphaned customer/order IDs. Marts layer inner-joins transactions to orders (enforces NOT NULL ORDER_ID FK before fact materialization).

5. **Email validation regex**: Pattern `^[^@\s]+@[^@\s]+\.[^@\s]+$` requires user@domain.tld format; rejects missing domains, multiple @'s, spaces.

6. **Loyalty tier canonicalization**: Client C segments (VIP/REGULAR/NEW) mapped to standard tiers (PLATINUM/SILVER/BRONZE) in staging, enabling consistent downstream reporting.

---

## Next Steps

1. **[COMPLETE]** ✅ Staging models SQL
2. **[PENDING]** Create mart layer models:
   - `dim_customer` (union stg_client_a__customer + stg_client_c__customer)
   - `dim_product` (union stg_client_a__products + stg_client_c__products)
   - `dim_date` (dbt macro to generate date spine)
   - `dim_payment_type` (seed)
   - `dim_client` (seed)
   - `fact_orders` (build FIRST; aggregate order-level metrics)
   - `fact_transactions` (build AFTER fact_orders; 1:N FK constraint)
3. **[PENDING]** dbt tests (relationships, uniqueness, not_null on mart models)
4. **[PENDING]** Jenkins CI/CD pipeline (lint + test + run)
5. **[PENDING]** Sample queries & runbook documentation

---

## Files Summary

### SQL Models Created (9)
```
dbt/models/staging/client_a/
├── stg_client_a__customer.sql       (185 lines)
├── stg_client_a__orders.sql         (160 lines)
├── stg_client_a__products.sql       (155 lines)
└── stg_client_a__transactions.sql   (200 lines)  ← XML parsing

dbt/models/staging/client_c/
├── stg_client_c__customer.sql       (195 lines)  ← Name splitting + tier mapping
├── stg_client_c__orders.sql         (145 lines)
├── stg_client_c__products.sql       (155 lines)
├── stg_client_c__payments.sql       (145 lines)  ← Refund-aware negative flag
└── stg_client_c__transactions.sql   (215 lines)  ← JSON parsing
```

### Schema YAML Updated (2)
```
dbt/models/staging/client_a/schema.yml     (updated with sources + columns)
dbt/models/staging/client_c/schema.yml     (updated with columns)
```

### Documentation Updated (1)
```
project_documentation/data_model.md        (S3/AWS flow clarification)
```

---

**Total**: 9 SQL models + 2 updated YAML files + 1 documentation update = **Complete staging layer ready for dbt run**.
