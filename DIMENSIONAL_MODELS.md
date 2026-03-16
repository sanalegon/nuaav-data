# Dimensional Models Implementation – Marts Layer

**Version**: 1.0  
**Date**: 2026-03-14  
**Status**: ✅ COMPLETE & TESTED  
**Build Time**: 10.80 seconds (7 of 7 successful)

---

## Overview

The dimensional models implement a **Kimball star schema** for NUAAV_DW following best practices from the data model documentation. All models are materialized as tables in the `NUAAV_DW.MARTS` schema.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DIMENSIONAL LAYER                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Dimensions (5):                                             │
│  • dim_client (3 rows)         [Multi-tenancy anchor]       │
│  • dim_date (61 rows)          [Calendar spine]             │
│  • dim_customer (43 rows)      [Unified customer master]    │
│  • dim_product (37 rows)       [Unified SKU catalogue]      │
│  • dim_payment_type (4 rows)   [Payment methods lookup]     │
│                                                              │
│  Facts (2):                                                  │
│  • fact_orders (42 rows)       [Order grain, 1:N parent]    │
│  • fact_transactions (207 rows) [Line-item grain, child]    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Dimension Models

### 1. dim_client

**Type**: Seeded dimension  
**Rows**: 3 (A, B, C)  
**Purpose**: Multi-tenancy anchor; identifies data provider

| CLIENT_ID | CLIENT_NAME | IS_ACTIVE | SOURCE_FOLDER |
|-----------|-------------|-----------|---------------|
| A | Client A | TRUE | input_data/ |
| B | Client B | FALSE | input_data/ |
| C | Client C | TRUE | input_data/Client_B/ |

**Key Features**:
- Client B is a placeholder (IS_ACTIVE = FALSE) for future onboarding
- Client C source folder captures original delivery path (`Client_B/` naming is an artifact)
- Every fact row must have a NOT NULL foreign key to this dimension

### 2. dim_date

**Type**: Generated calendar spine  
**Rows**: 61 (2025-11-01 to 2025-12-31)  
**Purpose**: Enable time-based analysis without date arithmetic in queries

**Columns**:
- `DATE_KEY` (INT) — Surrogate key in YYYYMMDD format (e.g., 20251110)
- `FULL_DATE` (DATE) — Actual calendar date
- `YEAR`, `QUARTER`, `MONTH`, `MONTH_NAME`, `WEEK_OF_YEAR`
- `DAY_OF_MONTH`, `DAY_OF_WEEK`, `DAY_NAME`, `IS_WEEKEND`

**Usage Example**:
```sql
-- Revenue by month without date arithmetic
SELECT d.month_name, d.year, SUM(ft.line_total) AS revenue
FROM fact_transactions ft
JOIN dim_date d ON ft.date_key = d.date_key
WHERE ft.is_duplicate = FALSE
GROUP BY 1, 2
ORDER BY d.year, d.month;
```

### 3. dim_customer

**Type**: Conformed dimension  
**Rows**: 43 (all customers from both active clients)  
**Purpose**: Unified customer master with cross-client loyalty tier mapping

**Key Columns**:
- `CUSTOMER_KEY` (INT) — Surrogate primary key
- `CUSTOMER_ID` (VARCHAR) — Natural key (e.g., CUST-A-0001, C-CUST-5001)
- `CLIENT_KEY` (INT FK) — Scopes customer per client
- `FIRST_NAME`, `LAST_NAME`, `FULL_NAME` — Parsed from source
- `EMAIL` — With `IS_EMAIL_VALID` flag
- `LOYALTY_TIER` — Canonicalized across clients
- `SIGNUP_SOURCE` — Client A only (Web, MobileApp, Referral)

**Loyalty Tier Mapping**:
| Client A | Client C | Canonical |
|----------|----------|-----------|
| PLATINUM | VIP | PLATINUM |
| GOLD | — | GOLD |
| SILVER | REGULAR | SILVER |
| BRONZE | NEW | BRONZE |
| (blank) | UNKNOWN | UNKNOWN |

**Unique Constraint**: `(CUSTOMER_ID, CLIENT_KEY)` — same natural key can exist in different clients

### 4. dim_product

**Type**: Conformed dimension  
**Rows**: 37 (all SKUs from both active clients)  
**Purpose**: Unified SKU catalogue with price validity flags

**Key Columns**:
- `PRODUCT_KEY` (INT) — Surrogate primary key
- `SKU` (VARCHAR) — Natural key (e.g., SKU-A-001, C-SKU-001)
- `CLIENT_KEY` (INT FK) — Scopes product per client
- `PRODUCT_NAME` (VARCHAR) — Description
- `CATEGORY` (VARCHAR) — Product classification
- `UNIT_PRICE` (NUMBER) — Listed price
- `CURRENCY` (VARCHAR) — All sources currently USD
- `IS_PRICE_VALID` (BOOLEAN) — FALSE when unit_price ≤ 0

**Unique Constraint**: `(SKU, CLIENT_KEY)` — same SKU can exist in different clients

**Important**: Rows with `IS_PRICE_VALID = FALSE` must be **excluded from revenue calculations** but are **retained** in the fact tables for transactional integrity.

### 5. dim_payment_type

**Type**: Static lookup  
**Rows**: 4 (seeded)  
**Purpose**: Standardize payment methods; prevent free-text entries in facts

**Values**:
- BankTransfer
- CreditCard
- PayPal
- Unknown

---

## Fact Tables

### fact_orders

**Type**: Aggregated fact table  
**Grain**: One row per order (order-header level)  
**Rows**: 42  
**Density**: Low — suitable for order-level analytics (order counts, AOV, repeat purchase)

**Foreign Keys** (all with relationships to dimensions):
| FK Column | References | Nullable | Notes |
|-----------|-----------|----------|-------|
| `ORDER_ID` | order natural key | NOT NULL | Every order has corresponding transactions |
| `CLIENT_KEY` | dim_client | NOT NULL | Multi-tenancy requirement |
| `CUSTOMER_KEY` | dim_customer | ✅ NULL | NULL when customer_id is orphaned |
| `DATE_KEY` | dim_date | ✅ NULL | NULL when order_date is invalid |
| `PAYMENT_TYPE_KEY` | dim_payment_type | ✅ NULL | NULL when payment method is unrecognized |

**Natural Keys** (for traceability):
- `ORDER_ID` (VARCHAR)

**Measures**:
- `TOTAL_AMOUNT` (NUMBER) — Sum of line-item totals
- `ITEM_COUNT` (INTEGER) — Number of distinct line items
- `PAYMENT_AMOUNT` (NUMBER) — Total payment (typically matches TOTAL_AMOUNT)
- `ORDER_STATUS`, `ORDER_CHANNEL`, `PAYMENT_STATUS` (VARCHAR)

**Anomaly Flags** (4 types):
| Flag | When TRUE |
|------|-----------|
| `IS_DUPLICATE` | ORDER_ID appears > 1 time across all sources |
| `HAS_NEGATIVE_AMT` | TOTAL_AMOUNT < 0 AND PAYMENT_STATUS ≠ 'REFUNDED' |
| `HAS_MISSING_DATE` | ORDER_DATE could not be parsed to valid date |
| `HAS_INVALID_REF` | CUSTOMER_ID not found in dim_customer |

**Audit Columns**:
- `SOURCE_FILE` — Original filename
- `LOADED_AT` — Pipeline execution timestamp
- `ANOMALY_NOTES` — Pipe-delimited string of all triggered flags

### fact_transactions

**Type**: Detailed fact table  
**Grain**: One row per line-item (one SKU per transaction)  
**Rows**: 207  
**Density**: High — suitable for product-level analysis, inventory, item-level drill-down

**Foreign Keys** (all with relationships):
| FK Column | References | Nullable | Notes |
|-----------|-----------|----------|-------|
| `ORDER_ID` | fact_orders.ORDER_ID | NOT NULL | **Hard constraint**: every transaction must have an order |
| `CLIENT_KEY` | dim_client | NOT NULL | Multi-tenancy requirement |
| `CUSTOMER_KEY` | dim_customer | ✅ NULL | NULL when customer_id is orphaned |
| `PRODUCT_KEY` | dim_product | ✅ NULL | NULL when SKU is missing or orphaned |
| `DATE_KEY` | dim_date | ✅ NULL | NULL when order_date is invalid |
| `PAYMENT_TYPE_KEY` | dim_payment_type | ✅ NULL | NULL when payment method is unrecognized |

**Natural Keys** (for traceability):
- `TRANSACTION_ID` (VARCHAR)
- `ORDER_ID` (VARCHAR)

**Measures** (all at line-item level):
- `QUANTITY` (INTEGER) — Units for this item
- `UNIT_PRICE` (NUMBER) — Price per unit
- `LINE_TOTAL` (NUMBER) — Convenience measure = QUANTITY × UNIT_PRICE
- `PAYMENT_AMOUNT` (NUMBER) — Total order payment (repeated from order-level)
- `ORDER_STATUS`, `ORDER_CHANNEL`, `PAYMENT_STATUS` (VARCHAR)

**Anomaly Flags** (6 types):
| Flag | When TRUE |
|------|-----------|
| `IS_DUPLICATE` | TRANSACTION_ID appears > 1 time across all sources |
| `HAS_NEGATIVE_QTY` | QUANTITY < 0 |
| `HAS_NEGATIVE_AMT` | PAYMENT_AMOUNT < 0 AND PAYMENT_STATUS ≠ 'REFUNDED' |
| `HAS_MISSING_DATE` | ORDER_DATE could not be parsed to valid date |
| `HAS_INVALID_REF` | CUSTOMER_ID or SKU not found in their dimension |
| `HAS_MISSING_SKU` | SKU is NULL or empty string |

**Audit Columns**:
- `SOURCE_FILE` — Original XML/JSON filename
- `LOADED_AT` — Pipeline execution timestamp
- `ANOMALY_NOTES` — Pipe-delimited string of all triggered flags

---

## 1:N Relationship (fact_orders ← fact_transactions)

**Critical Design**: Every row in `fact_transactions` **must** have a corresponding row in `fact_orders`.

**Enforcement**:
- `fact_transactions.ORDER_ID` is **NOT NULL** (hard constraint)
- `fact_transactions.ORDER_ID` has a **foreign key** relationship to `fact_orders.ORDER_ID`
- dbt dependency ensures `fact_orders` materializes before `fact_transactions`

**Validation Query** (detect orphan orders — should return 0 rows):
```sql
SELECT fo.order_id, COUNT(ft.transaction_id) AS txn_count
FROM fact_orders fo
LEFT JOIN fact_transactions ft ON fo.order_id = ft.order_id
WHERE txn_count IS NULL
GROUP BY fo.order_id;
```

**Aggregation Alignment**:
- `SUM(fact_orders.total_amount)` = `SUM(fact_transactions.line_total)` when same filters applied
- Provides confidence in data consistency across grains

---

## Usage Patterns

### Pattern 1: Revenue by Product & Month
```sql
SELECT 
    p.category,
    p.product_name,
    d.month_name,
    d.year,
    SUM(ft.line_total) AS revenue,
    COUNT(DISTINCT ft.transaction_id) AS transaction_count
FROM fact_transactions ft
JOIN dim_product p ON ft.product_key = p.product_key
JOIN dim_date d ON ft.date_key = d.date_key
WHERE ft.is_duplicate = FALSE
  AND ft.has_negative_amt = FALSE
  AND ft.has_missing_sku = FALSE
  AND p.is_price_valid = TRUE
GROUP BY 1, 2, 3, 4
ORDER BY d.year DESC, d.month DESC, revenue DESC;
```

### Pattern 2: Customer Tier Analysis
```sql
SELECT 
    c.loyalty_tier,
    c.client_key,
    COUNT(DISTINCT fo.order_id) AS order_count,
    COUNT(DISTINCT ft.transaction_id) AS item_count,
    AVG(fo.total_amount) AS avg_order_value,
    SUM(fo.total_amount) AS total_spent
FROM fact_orders fo
JOIN dim_customer c ON fo.customer_key = c.customer_key
LEFT JOIN fact_transactions ft ON fo.order_id = ft.order_id
WHERE fo.is_duplicate = FALSE
  AND fo.has_negative_amt = FALSE
GROUP BY 1, 2
ORDER BY total_spent DESC;
```

### Pattern 3: Data Quality Summary
```sql
SELECT 
    CASE 
        WHEN is_duplicate THEN 'DUPLICATE'
        WHEN has_negative_qty THEN 'NEG_QTY'
        WHEN has_negative_amt THEN 'NEG_AMT'
        WHEN has_missing_date THEN 'NO_DATE'
        WHEN has_invalid_ref THEN 'BAD_REF'
        WHEN has_missing_sku THEN 'NO_SKU'
        ELSE 'CLEAN'
    END AS issue_type,
    COUNT(*) AS row_count,
    COUNT(DISTINCT transaction_id) AS affected_txns
FROM fact_transactions
GROUP BY 1
ORDER BY row_count DESC;
```

---

## Build & Deployment

### Build Command
```bash
cd dbt
dbt build --select marts
```

### Expected Output (All 7 models + tests)
```
1 of 7 OK created sql table model marts.dim_client ..................... [SUCCESS]
2 of 7 OK created sql table model marts.dim_date ....................... [SUCCESS]
3 of 7 OK created sql table model marts.dim_payment_type ............... [SUCCESS]
4 of 7 OK created sql table model marts.dim_customer ................... [SUCCESS]
5 of 7 OK created sql table model marts.dim_product .................... [SUCCESS]
6 of 7 OK created sql table model marts.fact_orders .................... [SUCCESS]
7 of 7 OK created sql table model marts.fact_transactions .............. [SUCCESS]

Completed successfully (10.80s)
```

### Dependencies
- All models depend on `staging` layer (must be built first via `dbt build --select staging`)
- `fact_transactions` depends on `fact_orders` (dbt auto-resolves)
- All facts depend on all dimensions

---

## Data Lineage

```
Raw Data (XML/JSON files)
        ↓
Staging Layer (6 models)
    └─ stg_transactions
    └─ stg_customer
    └─ stg_products
    └─ stg_orders
    └─ stg_payments
    └─ (others)
        ↓
Dimensional Layer (7 models)
    ├─ Dimensions (5)
    │  ├─ dim_client
    │  ├─ dim_date
    │  ├─ dim_customer
    │  ├─ dim_product
    │  └─ dim_payment_type
    │
    └─ Facts (2)
       ├─ fact_orders (aggregated, 1:N parent)
       └─ fact_transactions (detailed, 1:N child)
```

---

## File Locations

```
dbt/models/marts/
├── dim_client.sql                 (3 rows seeded)
├── dim_date.sql                   (61 rows generated)
├── dim_customer.sql               (43 rows from stg_customer)
├── dim_product.sql                (37 rows from stg_products)
├── dim_payment_type.sql           (4 rows seeded)
├── fact_orders.sql                (42 rows aggregated)
├── fact_transactions.sql          (207 rows detailed)
└── schema.yml                      (column docs + tests)
```

---

## Next Steps

1. **Validate Data Quality**: Run data quality test suite
2. **Build Analytics Views**: Create consumption-layer views for BI/dashboards
3. **Documents Queries**: Capture standard reporting patterns
4. **Performance Tuning**: Add clustered keys and statistics if needed
5. **Downstream Marts**: Build specialized fact tables (e.g., customer_lifetime_value)
