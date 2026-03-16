# Data Model Documentation – NUAAV Data Warehouse

**Version**: 1.0  
**Date**: 2026-03-14  
**Author**: Data Engineering  
**Schema**: `NUAAV_DW.MARTS`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture: Star Schema](#2-architecture-star-schema)
3. [Dimensions](#3-dimensions)
   - [dim_client](#31-dim_client)
   - [dim_date](#32-dim_date)
   - [dim_customer](#33-dim_customer)
   - [dim_product](#34-dim_product)
   - [dim_payment_type](#35-dim_payment_type)
4. [Fact Tables](#4-fact-tables)
   - [fact_transactions](#41-fact_transactions)
   - [fact_orders](#42-fact_orders)
5. [Entity-Relationship Overview](#5-entity-relationship-overview)
6. [Data Lineage & Layer Architecture](#6-data-lineage--layer-architecture)
7. [How to Query the Model](#7-how-to-query-the-model)
8. [Data Quality Flags](#8-data-quality-flags)
9. [Proposed Future Improvements](#9-proposed-future-improvements)

---

## 1. Overview

This data model consolidates financial transaction data from multiple clients (currently **Client A** and **Client C**) into a single canonical representation. The model is designed following **Ralph Kimball's dimensional modelling** methodology, producing a star schema that is optimised for analytical queries and BI tool consumption.

### Design principles

| Principle | Decision |
|-----------|----------|
| Methodology | Kimball star schema — simple joins, fast aggregations |
| Grain | Two fact tables at different grains: **`fact_transactions`** at line-item level (one row per SKU per transaction); **`fact_orders`** at order level (one row per order) |
| Fact relationship | **1:N relationship**: every transaction belongs to exactly one order. `fact_transactions.ORDER_ID` is a foreign key to `fact_orders.ORDER_ID`. This ensures referential integrity and makes the data lineage explicit. |
| Shared dimensions | Both facts share five common dimensions (dim_client, dim_customer, dim_product, dim_date, dim_payment_type) for consistency and easy drill-down |
| Multi-tenancy | A `dim_client` dimension tags every fact row with its source client so a single schema serves all clients |
| No data loss | Anomalous rows are **kept** in fact tables and surfaced via boolean flags rather than discarded |
| Surrogate keys | All dimension primary keys are integer surrogates (autoincrement); natural keys are preserved for traceability |
| Null-safe FK | All foreign keys on fact tables are **nullable** — orphaned references become `NULL` + an anomaly flag rather than causing load failures, **except** `ORDER_ID` which is NOT NULL (hard constraint) |

---

## 2. Architecture: Star Schema

```
                     ┌──────────────────┐
                     │ dim_payment_type │
                     └────────┬─────────┘
                              │
                 ┌────────────┼────────────┐
                 │            │            │
┌──────────────┐ │  ┌─────────▼──────────┐ │  ┌──────────────┐
│ dim_customer │◄┼──│ fact_transactions   │─┼─►│ dim_product  │
└──────────────┘ │  │ (line-item grain)   │ │  └──────────────┘
                 │  └─────────▲──────────┘ │
                 │            │            │
┌──────────────┐ │  ┌─────────┴──────────┐ │  ┌──────────────┐
│  dim_client  │◄┼──│  fact_orders        │─┼─►│   dim_date   │
└──────────────┘ │  │  (order grain)      │ │  └──────────────┘
                 │  └────────────────────┘ │
                 │                         │
                 └─────────────┬───────────┘
                               │
                        (shared dimensions)
```

The diagram shows **five shared dimensions** (all in singular form) surrounding **two fact tables** at different grains:  
- `fact_transactions`: one row per line-item (SKU within a transaction)  
- `fact_orders`: one row per order-header (order-level summary)

**Key relationship**: Every transaction belongs to exactly one order. `fact_transactions.ORDER_ID` is a NOT NULL foreign key to `fact_orders.ORDER_ID`, establishing a **1:N relationship** (one order → many transactions).

Both facts carry a direct `CLIENT_KEY` so client-level filtering never requires a join through `dim_customer` or `dim_product`.

---

## 3. Dimensions

### 3.1 `dim_client`

**Purpose**: Identifies the data provider (client / tenant). Acts as the multi-tenancy anchor for the entire model.

| Column | Type | Description |
|--------|------|-------------|
| `CLIENT_KEY` | INTEGER (PK) | Surrogate key |
| `CLIENT_ID` | VARCHAR(10) | Natural key — values: `'A'`, `'B'`, `'C'` |
| `CLIENT_NAME` | VARCHAR(100) | Human-readable name |
| `SOURCE_FOLDER` | VARCHAR(200) | Original delivery folder path |
| `IS_ACTIVE` | BOOLEAN | FALSE = no data received yet |
| `LOADED_AT` | TIMESTAMP_NTZ | Audit timestamp |

**Seeded rows**
TODO: CHECK MENTION OF CLIENT B. MIGHT NOT BE REQUIRED TO SAY IT'S MISSING CLIENT B

| CLIENT_ID | CLIENT_NAME | IS_ACTIVE | Note |
|-----------|-------------|-----------|------|
| A | Client A | ✅ TRUE | XML transactions + CSV customers/orders/products |
| C | Client C | ✅ TRUE | Delivered inside `input_data/Client B/` — folder name is a delivery artefact |
| B | Client B | ❌ FALSE | Placeholder — no data received as of v1.0 |

**Particularities**
- The delivery folder for Client C is named `Client B`. This is a naming error from the source system; all internal IDs use the `C-` prefix. The `SOURCE_FOLDER` column preserves the original path for audit purposes.
- `IS_ACTIVE = FALSE` on Client B signals to downstream consumers and pipelines that no data exists, without requiring schema or code changes when Client B eventually onboards.

---

### 3.2 `dim_date`

**Purpose**: Standard date spine enabling time-intelligence queries (YoY, MoM, week-over-week) without date arithmetic in application code.

| Column | Type | Description |
|--------|------|-------------|
| `DATE_KEY` | INTEGER (PK) | Surrogate — format `YYYYMMDD` (e.g. 20251110) |
| `FULL_DATE` | DATE | Actual calendar date |
| `YEAR` | INTEGER | Calendar year |
| `QUARTER` | INTEGER | 1–4 |
| `MONTH` | INTEGER | 1–12 |
| `MONTH_NAME` | VARCHAR(20) | Full month name (e.g. `November`) |
| `WEEK_OF_YEAR` | INTEGER | ISO 8601 week number |
| `DAY_OF_MONTH` | INTEGER | 1–31 |
| `DAY_OF_WEEK` | INTEGER | 0 = Sunday … 6 = Saturday |
| `DAY_NAME` | VARCHAR(20) | Full day name (e.g. `Monday`) |
| `IS_WEEKEND` | BOOLEAN | TRUE for Saturday / Sunday |
| `LOADED_AT` | TIMESTAMP_NTZ | Audit timestamp |

**How to use**
```sql
-- Revenue by month
SELECT d.year, d.month_name, SUM(f.line_total) AS revenue
FROM   NUAAV_DW.MARTS.fact_transactions f
JOIN   NUAAV_DW.MARTS.dim_date d ON f.date_key = d.date_key
WHERE  f.is_duplicate = FALSE
  AND  f.has_negative_amt = FALSE
GROUP  BY 1, 2
ORDER  BY d.year, d.month;
```

**Particularities**
- Rows with `HAS_MISSING_DATE = TRUE` in `fact_transactions` have `DATE_KEY = NULL` and will be excluded from any `JOIN` or `GROUP BY` on this dimension — this is intentional.
- The date spine is generated by a dbt macro covering at minimum the full range of transaction dates (Nov 2025) plus a configurable buffer. It does **not** store time-of-day; a separate `dim_time` would be needed for intra-day analysis (see §9).

---

### 3.3 `dim_customer`

**Purpose**: Unified customer master across all clients. Each customer is scoped per client (a customer ID from Client A and a different customer ID from Client C are independent rows).

| Column | Type | Description |
|--------|------|-------------|
| `CUSTOMER_KEY` | INTEGER (PK) | Surrogate key |
| `CUSTOMER_ID` | VARCHAR(50) | Natural key from source (e.g. `CUST-A-0001`, `C-CUST-5001`) |
| `CLIENT_KEY` | INTEGER (FK) | References `dim_client` |
| `FIRST_NAME` | VARCHAR(100) | First name (Client A: explicit; Client C: split from `CUSTOMER_NAME`) |
| `LAST_NAME` | VARCHAR(100) | Last name (same split logic) |
| `FULL_NAME` | VARCHAR(200) | Concatenation of first + last |
| `EMAIL` | VARCHAR(200) | Email address |
| `LOYALTY_TIER` | VARCHAR(50) | Canonical tier — see mapping below |
| `SIGNUP_SOURCE` | VARCHAR(100) | Client A only (`Web`, `MobileApp`, `Referral`). NULL for Client C |
| `IS_ACTIVE` | BOOLEAN | Source `is_active` flag |
| `IS_EMAIL_VALID` | BOOLEAN | FALSE if email fails `^[^@\s]+@[^@\s]+\.[^@\s]+` check |
| `LOADED_AT` | TIMESTAMP_NTZ | Audit timestamp |

**Unique constraint**: `(CUSTOMER_ID, CLIENT_KEY)` — the same natural key can exist across clients.

**Loyalty tier canonical mapping**

| Source client | Source value | Canonical `LOYALTY_TIER` |
|---------------|-------------|--------------------------|
| Client A | PLATINUM | PLATINUM |
| Client A | GOLD | GOLD |
| Client A | SILVER | SILVER |
| Client A | BRONZE | BRONZE |
| Client A | *(blank)* | UNKNOWN |
| Client C | VIP | PLATINUM |
| Client C | REGULAR | SILVER |
| Client C | NEW | BRONZE |
| Client C | UNKNOWN | UNKNOWN |

**Particularities**
- Client A provides `FIRST_NAME` / `LAST_NAME` separately; Client C provides a single `CUSTOMER_NAME` field which staging splits on the first space character.
- Customers with anomalous emails (`IS_EMAIL_VALID = FALSE`) are **retained** as they still participate validly in transaction history.
- `CUST-A-9999` and `C-CUST-9999` are orphan references found only in orders — they do not exist in the customer files. These IDs are **not loaded** into `dim_customer`; the corresponding fact rows carry `CUSTOMER_KEY = NULL` and `HAS_INVALID_REF = TRUE`.

---

### 3.4 `dim_product`

**Purpose**: Unified SKU catalogue across all clients. Each SKU is scoped per client.

| Column | Type | Description |
|--------|------|-------------|
| `PRODUCT_KEY` | INTEGER (PK) | Surrogate key |
| `SKU` | VARCHAR(50) | Natural key from source |
| `CLIENT_KEY` | INTEGER (FK) | References `dim_client` |
| `PRODUCT_NAME` | VARCHAR(200) | Product description |
| `CATEGORY` | VARCHAR(100) | Product category (e.g. `Accessories`, `Electronics`) |
| `UNIT_PRICE` | NUMBER(12,2) | Listed unit price |
| `CURRENCY` | VARCHAR(10) | Currency code — all current sources: `USD` |
| `IS_ACTIVE` | BOOLEAN | Source `is_active` flag |
| `IS_PRICE_VALID` | BOOLEAN | FALSE when `unit_price <= 0` |
| `LOADED_AT` | TIMESTAMP_NTZ | Audit timestamp |

**Unique constraint**: `(SKU, CLIENT_KEY)`

**Particularities**
- `IS_PRICE_VALID = FALSE` rows must be **excluded** from revenue calculations (`SUM(line_total)`) but are kept to preserve transactional integrity — a transaction that referenced a bad-price SKU must still appear in the fact table.
- Client A `SKU-A-011` (Mouse Pad) has `unit_price = -9.99` in the source; Client C `C-SKU-011` has `unit_price = -59.99`. Both get `IS_PRICE_VALID = FALSE`.
- `C-SKU-999` ("Unknown Product", price 0.00, inactive) is loaded for audit traceability — it was explicitly sent in the source file.
- Both clients share several product names (e.g. `HDMI Cable`, `Portable SSD`). These are treated as **separate SKUs** because they are scoped to different clients. A future cross-client product master match would be done via a separate `dim_product_master` (see §9).

---

### 3.5 `dim_payment_type`

**Purpose**: Small static lookup dimension for payment methods. Avoids storing free-text method names in the fact table and enables grouping/filtering by method without string matching.

| Column | Type | Description |
|--------|------|-------------|
| `PAYMENT_TYPE_KEY` | INTEGER (PK) | Surrogate key |
| `PAYMENT_METHOD` | VARCHAR(100) | Canonical name |
| `LOADED_AT` | TIMESTAMP_NTZ | Audit timestamp |

**Seeded values**

| PAYMENT_METHOD |
|----------------|
| CreditCard |
| PayPal |
| BankTransfer |
| Unknown |

**Particularities**
- Methods are seeded at deploy time. New methods from incoming client data are surfaced as anomalies in the staging layer and added to the seed before the next pipeline run.
- `Unknown` is a catch-all for fact rows where the payment method could not be parsed.

---

## 4. Fact Tables

### 4.1 `fact_transactions`

**Purpose**: Central fact table recording every financial transaction at **line-item grain** — one row per SKU per transaction.

**Grain**: One transaction can contain multiple items; each item produces one row. This allows item-level revenue analysis and avoids measure fan-out when joining to product or customer dimensions.

#### Columns

**Fact-to-fact relationship** (NEW in v1.0)

| Column | References | Nullable | Notes |
|--------|-----------|----------|-------|
| `ORDER_ID` | `fact_orders.ORDER_ID` | ❌ NOT NULL | **Every transaction must belong to an order.** This is a hard constraint ensuring referential integrity. |

**Foreign keys to dimensions**

| Column | References | Nullable | Notes |
|--------|-----------|----------|-------|
| `CLIENT_KEY` | `dim_client` | ❌ NOT NULL | Every fact row must have a client |
| `CUSTOMER_KEY` | `dim_customer` | ✅ NULL | NULL when customer_id is orphaned |
| `PRODUCT_KEY` | `dim_product` | ✅ NULL | NULL when SKU is missing or unknown |
| `DATE_KEY` | `dim_date` | ✅ NULL | NULL when order_date is missing |
| `PAYMENT_TYPE_KEY` | `dim_payment_type` | ✅ NULL | NULL when method is unrecognised |

**Natural keys (traceability)**

| Column | Type | Description |
|--------|------|-------------|
| `TRANSACTION_ID` | VARCHAR(50) | Source transaction identifier |
| `ORDER_ID` | VARCHAR(50) | Source order identifier |

**Measures**

| Column | Type | Description |
|--------|------|-------------|
| `QUANTITY` | INTEGER | Number of units for this line item |
| `UNIT_PRICE` | NUMBER(12,2) | Price per unit at time of transaction |
| `LINE_TOTAL` | NUMBER(14,2) | `QUANTITY × UNIT_PRICE` — computed in staging |
| `PAYMENT_AMOUNT` | NUMBER(14,2) | Total payment amount for the order (from payment record) |
| `PAYMENT_STATUS` | VARCHAR(50) | `SETTLED`, `FAILED`, `REFUNDED`, `PENDING` |
| `ORDER_STATUS` | VARCHAR(50) | `COMPLETED`, `CANCELLED`, `PENDING` |
| `ORDER_CHANNEL` | VARCHAR(100) | `Web`, `Mobile`, etc. |

**Data quality flags** *(see §8 for detail)*

| Flag | When TRUE |
|------|-----------|
| `IS_DUPLICATE` | `TRANSACTION_ID` appears more than once across all sources |
| `HAS_NEGATIVE_QTY` | `QUANTITY < 0` |
| `HAS_NEGATIVE_AMT` | `PAYMENT_AMOUNT < 0` **and** `PAYMENT_STATUS != 'REFUNDED'` |
| `HAS_MISSING_DATE` | `ORDER_DATE` could not be parsed to a valid date |
| `HAS_INVALID_REF` | `CUSTOMER_ID` or `SKU` not found in their respective dimension |
| `HAS_MISSING_SKU` | `SKU` is NULL or empty string |
| `ANOMALY_NOTES` | Pipe-delimited string listing all triggered anomaly reasons |

**Audit**

| Column | Description |
|--------|-------------|
| `SOURCE_FILE` | Original filename from the internal stage |
| `LOADED_AT` | Pipeline execution timestamp |

**Important usage rules**
- Standard revenue/volume queries **must** filter `IS_DUPLICATE = FALSE AND HAS_NEGATIVE_AMT = FALSE AND HAS_MISSING_SKU = FALSE` to exclude bad data.
- Refund analysis should **include** rows where `PAYMENT_STATUS = 'REFUNDED'` even when `PAYMENT_AMOUNT < 0` — those are valid signed amounts.
- `LINE_TOTAL` is a pre-computed convenience measure. For the most accurate figure use `QUANTITY * UNIT_PRICE` only on rows with `IS_PRICE_VALID = TRUE` (join `dim_product`).

### 4.2 `fact_orders`

**Purpose**: Order-header fact table recording summary metrics at the **order level** — one row per order.

**Grain**: One row per order, regardless of how many line items it contains. This fact table is optimized for order-level analytics (order counts, average order value, order frequency) without requiring aggregation.

#### Columns

**Foreign keys**

| Column | References | Nullable | Notes |
|--------|-----------|----------|-------|
| `CLIENT_KEY` | `dim_client` | ❌ NOT NULL | Every fact row must have a client |
| `CUSTOMER_KEY` | `dim_customer` | ✅ NULL | NULL when customer_id is orphaned |
| `DATE_KEY` | `dim_date` | ✅ NULL | NULL when order_date is missing |
| `PAYMENT_TYPE_KEY` | `dim_payment_type` | ✅ NULL | NULL when method is unrecognised |

**Natural keys (traceability)**

| Column | Type | Description |
|--------|------|-------------|
| `ORDER_ID` | VARCHAR(50) | Source order identifier (unique per client) |

**Measures**

| Column | Type | Description |
|--------|------|-------------|
| `ORDER_STATUS` | VARCHAR(50) | `COMPLETED`, `CANCELLED`, `PENDING` |
| `ORDER_CHANNEL` | VARCHAR(100) | `Web`, `Mobile`, etc. |
| `PAYMENT_STATUS` | VARCHAR(50) | `SETTLED`, `FAILED`, `REFUNDED`, `PENDING` |
| `TOTAL_AMOUNT` | NUMBER(14,2) | Sum of all line-item totals for this order |
| `ITEM_COUNT` | INTEGER | Number of distinct line items in the order |
| `PAYMENT_AMOUNT` | NUMBER(14,2) | Total payment amount (typically matches `TOTAL_AMOUNT`) |

**Data quality flags** *(see §8 for detail)*

| Flag | When TRUE |
|------|-----------|
| `IS_DUPLICATE` | `ORDER_ID` appears more than once across all sources |
| `HAS_NEGATIVE_AMT` | `TOTAL_AMOUNT < 0` **and** `PAYMENT_STATUS != 'REFUNDED'` |
| `HAS_MISSING_DATE` | `ORDER_DATE` could not be parsed to a valid date |
| `HAS_INVALID_REF` | `CUSTOMER_ID` not found in `dim_customer` |
| `ANOMALY_NOTES` | Pipe-delimited string listing all triggered anomaly reasons |

**Audit**

| Column | Description |
|--------|-------------|
| `SOURCE_FILE` | Original filename from the internal stage |
| `LOADED_AT` | Pipeline execution timestamp |

**Relationship to `fact_orders` (1:N — NOT NULL FK)**

Every row in `fact_transactions` **must** have a corresponding row in `fact_orders`. This is enforced by:
- `fact_transactions.ORDER_ID` is **NOT NULL** (hard constraint).
- `fact_transactions.ORDER_ID` has a **foreign key** relationship to `fact_orders.ORDER_ID` within the same `CLIENT_KEY`.

This design ensures:
- **Data integrity**: No orphan transactions (transactions without orders) can exist.
- **Business rule enforcement**: The pipeline will fail fast if source data violates the 1:N assumption.
- **Query certainty**: Analysts never need to worry about missing order rows when joining facts.

**Complementary usage**

`fact_orders` and `fact_transactions` serve different analytical needs:
- **Use `fact_orders`** for order counts, order-level conversions, average order value (AOV), repeat purchase analysis, and order-header reporting.
- **Use `fact_transactions`** for product-level revenue, item-level drill-down, inventory/SKU analysis, and line-item-level detail.
- **Aggregate alignment**: `SUM(fact_orders.total_amount)` = `SUM(fact_transactions.line_total)` for clean data (same anomaly filters applied to both).

Example validation query:
```sql
-- Verify 1:N integrity: each order has at least one transaction
SELECT fo.order_id, COUNT(DISTINCT ft.transaction_id) AS transaction_count
FROM   NUAAV_DW.MARTS.fact_orders fo
LEFT   JOIN NUAAV_DW.MARTS.fact_transactions ft ON fo.order_id = ft.order_id
WHERE  transaction_count IS NULL  -- Would indicate orphan order (data error)
GROUP  BY fo.order_id;
```

---

## 5. Entity-Relationship Overview

```
dim_client (CLIENT_KEY)
   │
   ├── fact_transactions.CLIENT_KEY      [NOT NULL]
   ├── fact_orders.CLIENT_KEY            [NOT NULL]
   │
dim_customer (CUSTOMER_KEY)
   │
   ├── fact_transactions.CUSTOMER_KEY   [nullable]
   └── fact_orders.CUSTOMER_KEY         [nullable]

dim_product (PRODUCT_KEY)
   │
   └── fact_transactions.PRODUCT_KEY    [nullable]

dim_date (DATE_KEY)
   │
   ├── fact_transactions.DATE_KEY       [nullable]
   └── fact_orders.DATE_KEY             [nullable]

dim_payment_type (PAYMENT_TYPE_KEY)
   │
   ├── fact_transactions.PAYMENT_TYPE_KEY [nullable]
   └── fact_orders.PAYMENT_TYPE_KEY       [nullable]

fact_orders (ORDER_ID)
   │
   └── fact_transactions.ORDER_ID      [NOT NULL, 1:N]  ← **Every transaction must belong to an order**
```

Both fact tables use the same dimension keys to ensure consistent joins and drill-down across different analytical grains. The **1:N relationship** between orders and transactions is explicit via a NOT NULL foreign key, ensuring every transaction has a corresponding order row.

---

## 6. Data Lineage & Layer Architecture

```
S3 Bucket (AWS)                 ← Raw source files (XML, CSV, JSON)
(general-purpose-datalake/     uploaded manually by user
 nuaav/input_data/)
      │
      ▼ (Snowflake external stage + COPY INTO)
NUAAV_DW.RAW.*                   ← Landing zone: zero transformation
  (01_raw_landing.sql)            CSV → VARCHAR columns; XML/JSON → VARIANT
  * Stages: STAGE_CLIENT_A, STAGE_CLIENT_C
  * File formats: CSV_FORMAT, XML_FORMAT, JSON_FORMAT
  * COPY INTO commands ready; executed via Jenkins or manual SQL
      │
      ▼
NUAAV_DW.STAGING.*               ← dbt views: clean, cast, rename, deduplicate
  (dbt models: stg_client_a__*, stg_client_c__*)
  * Handle anomalies (duplicates, invalid refs, missing dates, negative prices)
  * Flatten semi-structured data (XML/JSON)
  * Inner-join transactions to orders (enforces 1:N FK before mart load)
      │
      ├─► NUAAV_DW.MARTS.fact_orders    ← Built FIRST (referenced by fact_transactions)
      │   (dbt model: fact_orders)
      │
      ▼
NUAAV_DW.MARTS.*                 ← dbt tables: dimensions + fact
  (dbt models: dim_*, fact_orders, fact_transactions)
      │
      └─► NUAAV_DW.MARTS.fact_transactions  ← Built AFTER fact_orders (FK dependency)
          (dbt model: fact_transactions)
```

**Data Ingestion Flow**:
1. **Manual Upload**: Source files are uploaded to S3 at `s3://general-purpose-datalake/nuaav/input_data/`
2. **Snowflake External Stages**: `01_raw_landing.sql` defines Snowflake external stages with S3 credentials (set via environment variables for security)
3. **COPY INTO (Manual Trigger)**: DBA runs COPY INTO commands from `01_raw_landing.sql` after files appear in S3
4. **dbt Pipeline (CI/CD)**: Jenkins triggers dbt to transform RAW → STAGING → MARTS

**Build order**: dbt automatically resolves the `fact_transactions.ORDER_ID → fact_orders.ORDER_ID` dependency. `fact_orders` is materialized before `fact_transactions` to ensure no FK violations during ingestion.

Files in each layer:

| Model | Source Tables | Purpose |
|-------|---------------|---------|
| **stg_customer** | CLIENT_A_CUSTOMERS + CLIENT_C_CUSTOMERS | Union customers from both clients; maps Client C segment to canonical loyalty tier |
| **stg_orders** | CLIENT_A_ORDERS + CLIENT_C_ORDERS | Union orders; strips Client A channel annotations; validates date/customer ref |
| **stg_products** | CLIENT_A_PRODUCTS + CLIENT_C_PRODUCTS | Union products; flags invalid prices (≤ 0) |
| **stg_transactions** | CLIENT_A_TRANSACTIONS_XML + CLIENT_C_TRANSACTIONS_JSON | Parses XML/JSON; flattens items to line-item rows; cleans customer names; validates order ref |
| **stg_payments** | CLIENT_C_PAYMENTS | Client C payments; smart negative flagging (refund-aware); enriches fact_orders |

**Unified Design Benefits**:
- **Scalable**: Adding Client B = add CTE to union, no new model files
- **DRY**: Bug fix in email validation happens once (not 3 places)
- **Client tagging**: Each row has `client_id` for filtering and multi-tenant reporting
- **Future-proof**: Designed to handle 5, 10, 20+ clients without folder explosion

**1:N constraint enforcement** (v1.0): Prior to building `fact_transactions` in the marts layer, the staging layer ensures:
- Each transaction is inner-joined to its corresponding order (verifying ORDER_ID exists).
- Orphan transactions (ORDER_ID not found in orders) are either flagged as `HAS_INVALID_REF = TRUE` and dropped, or the pipeline halts depending on staging configuration.
- This guarantees the NOT NULL foreign key constraint on `fact_transactions.ORDER_ID → fact_orders.ORDER_ID` is satisfied before mart materialization.

---

## 7. How to Query the Model

### 7.1 Revenue by client (clean data only)

```sql
SELECT
    c.client_name,
    SUM(f.line_total) AS gross_revenue
FROM   NUAAV_DW.MARTS.fact_transactions f
JOIN   NUAAV_DW.MARTS.dim_client c       ON f.client_key = c.client_key
JOIN   NUAAV_DW.MARTS.dim_product p      ON f.product_key = p.product_key
WHERE  f.is_duplicate = FALSE
  AND  f.has_negative_amt = FALSE
  AND  f.has_missing_sku = FALSE
  AND  p.is_price_valid = TRUE
GROUP  BY 1;
```

### 7.2 Top 10 customers by spend

```sql
SELECT
    cu.full_name,
    c.client_name,
    SUM(f.line_total) AS total_spend
FROM   NUAAV_DW.MARTS.fact_transactions f
JOIN   NUAAV_DW.MARTS.dim_customer cu    ON f.customer_key = cu.customer_key
JOIN   NUAAV_DW.MARTS.dim_client c       ON f.client_key   = c.client_key
JOIN   NUAAV_DW.MARTS.dim_product p      ON f.product_key  = p.product_key
WHERE  f.is_duplicate = FALSE
  AND  f.has_negative_amt = FALSE
  AND  p.is_price_valid = TRUE
GROUP  BY 1, 2
ORDER  BY total_spend DESC
LIMIT  10;
```

### 7.3 Average order value (AOV) by client

```sql
SELECT
    c.client_name,
    COUNT(*)                          AS order_count,
    SUM(o.total_amount)               AS total_revenue,
    ROUND(AVG(o.total_amount), 2)     AS avg_order_value
FROM   NUAAV_DW.MARTS.fact_orders o
JOIN   NUAAV_DW.MARTS.dim_client c ON o.client_key = c.client_key
WHERE  o.is_duplicate = FALSE
  AND  o.order_status = 'COMPLETED'
  AND  o.has_negative_amt = FALSE
GROUP  BY 1
ORDER  BY avg_order_value DESC;
```

### 7.4 Monthly transaction and order volume over time

```sql
SELECT
    d.year,
    d.month_name,
    COUNT(DISTINCT f.transaction_id) AS line_items,
    COUNT(DISTINCT fo.order_id)      AS orders,
    SUM(fo.total_amount)             AS revenue
FROM   NUAAV_DW.MARTS.fact_transactions f
FULL   OUTER JOIN NUAAV_DW.MARTS.fact_orders fo
       ON f.order_id = fo.order_id
       AND f.client_key = fo.client_key
JOIN   NUAAV_DW.MARTS.dim_date d ON COALESCE(f.date_key, fo.date_key) = d.date_key
WHERE  (f.is_duplicate = FALSE OR f.is_duplicate IS NULL)
  AND  (fo.is_duplicate = FALSE OR fo.is_duplicate IS NULL)
GROUP  BY 1, 2, d.month
ORDER  BY d.year, d.month;
```

### 7.5 Data quality audit — rows with anomalies

```sql
-- Transaction-level anomalies
SELECT
    'transaction' AS fact_table,
    c.client_name,
    f.transaction_id,
    f.order_id,
    f.anomaly_notes,
    f.is_duplicate,
    f.has_negative_qty,
    f.has_negative_amt,
    f.has_missing_date,
    f.has_invalid_ref,
    f.has_missing_sku
FROM   NUAAV_DW.MARTS.fact_transactions f
JOIN   NUAAV_DW.MARTS.dim_client c ON f.client_key = c.client_key
WHERE  f.is_duplicate      = TRUE
    OR f.has_negative_qty  = TRUE
    OR f.has_negative_amt  = TRUE
    OR f.has_missing_date  = TRUE
    OR f.has_invalid_ref   = TRUE
    OR f.has_missing_sku   = TRUE

UNION ALL

-- Order-level anomalies
SELECT
    'order' AS fact_table,
    c.client_name,
    NULL AS transaction_id,
    fo.order_id,
    fo.anomaly_notes,
    fo.is_duplicate,
    NULL,
    fo.has_negative_amt,
    fo.has_missing_date,
    fo.has_invalid_ref,
    NULL
FROM   NUAAV_DW.MARTS.fact_orders fo
JOIN   NUAAV_DW.MARTS.dim_client c ON fo.client_key = c.client_key
WHERE  fo.is_duplicate      = TRUE
    OR fo.has_negative_amt  = TRUE
    OR fo.has_missing_date  = TRUE
    OR fo.has_invalid_ref   = TRUE

ORDER  BY client_name, fact_table, order_id, transaction_id;
```

---

## 8. Data Quality Flags

All anomaly flags live directly on both fact tables. This "flag-in-place" strategy means:

- Clean data (`WHERE is_duplicate = FALSE AND has_negative_amt = FALSE …`) is immediately queryable in production.
- The full raw history — including anomalies — is always available for audit and reprocessing without re-running ingestion.
- Adding a new flag is a non-breaking schema change (adding a column) rather than a breaking structural change.

**On `fact_transactions`**: `ORDER_ID` is **always NOT NULL**. If a source transaction cannot be mapped to an order, the **pipeline halts** rather than loading the row (unlike dimension FKs which set the key to NULL). This is a hard business constraint: every transaction must belong to an order.

| Flag | Applies to | Logic in staging | Recommended action |
|------|-----------|-----------------|-------------------|
| `IS_DUPLICATE` | Both facts | `ROW_NUMBER() OVER (PARTITION BY natural_key) > 1` | Always exclude with `= FALSE` |
| `HAS_NEGATIVE_AMT` | Both facts | `total_amount < 0 AND payment_status != 'REFUNDED'` | Exclude from revenue; valid for REFUNDED rows |
| `HAS_MISSING_DATE` | Both facts | `TRY_TO_DATE(order_date) IS NULL` | Exclude from time-series; back-fill when date is recovered |
| `HAS_INVALID_REF` | Both facts | Customer natural key not found in `dim_customer` | `customer_key` will be NULL |
| `HAS_NEGATIVE_QTY` | `fact_transactions` only | `quantity < 0` | Exclude from volume/revenue; investigate with client |
| `HAS_MISSING_SKU` | `fact_transactions` only | SKU is NULL or empty string | Exclude from product analysis |

**Pipeline safety**: The 1:N constraint via NOT NULL FK ensures:
- Orphan transactions (transactions without corresponding orders) cannot be loaded.
- Pipeline failures are explicit and actionable — investigate the source rather than silently accepting bad data.
- All downstream analytics have guaranteed order context.

---

## 9. Proposed Future Improvements

The following additions are **not in scope for v1.0** but are recommended as the platform grows:

### 9.1 `dim_order` — order attributes as a separate dimension

Currently degenerate dimensions (`ORDER_STATUS`, `ORDER_CHANNEL`) are stored directly on `fact_orders`. As the number of order-level attributes grows (e.g. shipping method, warehouse, fulfillment time), extracting these into a dedicated `dim_order` would clean up the fact table.

```
dim_order (order_key, order_id, client_key, order_status_key, channel_key, …)
```

### 9.2 `dim_order_status` / `dim_payment_status` — status lookup tables

Both `ORDER_STATUS` and `PAYMENT_STATUS` are currently stored as free-text. Small lookup tables would allow adding descriptions, display names, and groupings (e.g. `is_terminal_state`).

### 9.3 SCD Type 2 on `dim_customer` and `dim_product`

The current model is **SCD Type 1** (overwrite). If a customer changes their email or loyalty tier, the history is lost. Implementing SCD Type 2 (adding `VALID_FROM`, `VALID_TO`, `IS_CURRENT`) would enable point-in-time analysis.

### 9.4 `dim_time` — intra-day analytics

`batchTimestamp` is available in the JSON source files. A `dim_time` table (keyed on seconds-since-midnight) would enable hour-of-day and peak-traffic analysis once transactional volume justifies it.

### 9.5 `dim_product_master` — cross-client product matching

Several product names appear in both Client A and Client C catalogues (`HDMI Cable`, `Portable SSD`, `Webcam`, etc.) but under different SKU prefixes. A fuzzy-matched master product dimension would enable cross-client category analysis without double-counting.

### 9.6 `fact_payments` — separate payment fact

Today `PAYMENT_AMOUNT` and `PAYMENT_STATUS` are embedded in both `fact_transactions` and `fact_orders`. A dedicated `fact_payments` table at payment-level grain would cleanly separate the commercial/fulfilment story (transactions / orders) from the financial/settlement story (payments) — especially important as Client C already provides a dedicated `Payments.csv` that can have a different cardinality from transactions.

### 9.7 `fact_daily_customer_summary` — aggregate fact (periodic snapshot)

A pre-aggregated daily snapshot fact (`customer_key`, `date_key`, `total_spend`, `order_count`, `item_count`) would dramatically speed up customer lifetime value (LTV) and cohort queries that currently require full fact table scans.

### 9.8 Client B onboarding

When Client B data is received, the `dim_client` placeholder row (`IS_ACTIVE = FALSE`) needs updating and corresponding RAW tables, staging models, and seed files added. The canonical schema requires **no changes** — it is already designed for multi-client ingestion.
