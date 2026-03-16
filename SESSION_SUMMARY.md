# NUAAV Data Warehouse Project – Session Summary (2026-03-14)

## Project Overview
Financial transaction data ingestion from multiple clients (A, C) into a canonical Snowflake data warehouse using Kimball star schema, managed through dbt and Jenkins CI/CD pipeline.

---

## Key Findings

### Client B / C Discrepancy
- **Discovery**: Folder named `input_data/Client B/` contains files labeled **Client C** internally (`clientC_customers.csv`, `"client": "ClientC"`, ID prefixes `C-CUST-`, `C-ORD-`, etc.)
- **Action Taken**: Documented as delivery artefact; all data ingested as **Client C**; placeholder row seeded for Client B in `dim_client` with `IS_ACTIVE = FALSE`

### Anomalies Documented
- **Client A**: 28 distinct data quality issues across customers, orders, products, and transactions (duplicates, missing fields, negative prices/quantities, orphan references)
- **Client C**: 25 distinct issues in customers, orders, products, payments, and transactions (same categories plus invalid emails)
- Full catalogue in [docs/01_anomaly_report.md](docs/01_anomaly_report.md)

---

## Architecture: Canonical Data Model (v1.0)

### Dimensions (Singular Names per v1.0 Design)
1. **dim_client** – Multi-tenancy anchor (A, B placeholder, C active); tracks source folder
2. **dim_date** – ISO date spine (YYYYMMDD key); year/quarter/month/week/day attributes
3. **dim_customer** – Unified customer master per client; loyalty tier canonical mapping (VIP→PLATINUM, REGULAR→SILVER, NEW→BRONZE)
4. **dim_product** – SKU catalogue per client; flags invalid prices (≤0)
5. **dim_payment_type** – Static lookup (CreditCard, PayPal, BankTransfer, Unknown)

### Fact Tables (NEW TWO-FACT DESIGN)
1. **fact_transactions** (line-item grain)
   - Grain: 1 row per SKU per transaction
   - **NOT NULL FK to fact_orders.ORDER_ID** — every transaction must belong to an order (1:N constraint)
   - Measures: quantity, unit_price, line_total, payment_amount, order_status, payment_status, order_channel
   - Flags: is_duplicate, has_negative_qty, has_negative_amt, has_missing_date, has_invalid_ref, has_missing_sku
   - Build order: materialized AFTER fact_orders (dbt auto-resolves dependency)

2. **fact_orders** (order grain)
   - Grain: 1 row per order
   - Measures: total_amount, item_count, payment_amount, order_status, payment_status, order_channel
   - Flags: is_duplicate, has_negative_amt, has_missing_date, has_invalid_ref
   - Build order: materialized FIRST (referenced by fact_transactions FK)

### Data Quality Strategy
- **Flag-in-place**: All anomalies kept in fact tables with boolean flags (no data loss)
- **Null-safe FKs**: Dimension FKs nullable; orphaned refs → NULL + `HAS_INVALID_REF = TRUE`
- **Hard constraint**: `ORDER_ID` NOT NULL on fact_transactions (1:N enforcement)
- Sample query filters: `WHERE is_duplicate = FALSE AND has_negative_amt = FALSE AND has_missing_sku = FALSE`

### Architecture Layer
```
RAW (NUAAV_DW.RAW)           ← CSV→VARCHAR, XML/JSON→VARIANT; zero transformation
    ↓ (COPY INTO)
STAGING (NUAAV_DW.STAGING)   ← dbt views; rename, cast, deduplicate, flag anomalies
    ↓ (inner-join validation for ORDER_ID FK)
MARTS (NUAAV_DW.MARTS)       ← dbt tables; fact_orders built first, then fact_transactions
```

---

## Files Created (By Category)

### Snowflake DDL
- [snowflake/ddl/00_setup.sql](snowflake/ddl/00_setup.sql) — Database, schemas (RAW/STAGING/MARTS), warehouse, file formats, internal stages
- [snowflake/ddl/01_raw_landing.sql](snowflake/ddl/01_raw_landing.sql) — Raw tables, COPY INTO commands, file format definitions
- [snowflake/ddl/02_canonical.sql](snowflake/ddl/02_canonical.sql) — All 5 dimensions + 2 fact tables with full DDL

### dbt Project Configuration
- [dbt/dbt_project.yml](dbt/dbt_project.yml) — Project config, model materialization (staging→views, marts→tables), per-layer settings
- [dbt/packages.yml](dbt/packages.yml) — Dependency: `dbt-labs/dbt_utils` for surrogate keys, date spine
- [dbt/profiles.yml.template](dbt/profiles.yml.template) — Snowflake connection template; credentials via env vars

### dbt Model Schemas
- [dbt/models/staging/client_a/schema.yml](dbt/models/staging/client_a/schema.yml) — Staging model contracts (stg_client_a__customer, orders, products, transactions)
- [dbt/models/staging/client_c/schema.yml](dbt/models/staging/client_c/schema.yml) — Staging model contracts (stg_client_c__customer, orders, products, payments, transactions)
- [dbt/models/marts/schema.yml](dbt/models/marts/schema.yml) — Mart model contracts with dbt `relationships` tests; fact_orders built first

### Documentation
- [project_documentation/data_model.md](project_documentation/data_model.md) — **COMPREHENSIVE** data model documentation (600+ lines):
  - Overview, design principles, star schema architecture
  - Full column definitions per dimension/fact
  - Canonical loyalty tier mapping (Client A → Client C)
  - 5 sample queries (revenue, AOV, monthly volume, anomaly audit, validation)
  - ER overview with 1:N fact relationship
  - Data lineage diagram with build order
  - Data quality flags table
  - 8 proposed future improvements (SCD Type 2, dim_order, fact_payments, etc.)
- [docs/01_anomaly_report.md](docs/01_anomaly_report.md) — Full anomaly catalogue per client/table with handling decisions

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Singular dimension names** | Consistency, cleaner naming (v1.0 requirement) |
| **Two fact tables** | Different grains serve different analytic needs; 1:N ensures integrity |
| **NOT NULL ORDER_ID FK** | Hard business rule: every transaction must belong to order; pipeline fails fast on violation |
| **Flag-in-place anomalies** | Preserve full history without data loss; analysts filter as needed |
| **Null-safe dimension FKs** | Flexibility for orphaned references while flagging them |
| **Multi-tenant single schema** | dim_client tags all rows; one pipeline serves all clients |
| **Composite FK (ORDER_ID, CLIENT_KEY)** | Ensures transactions don't cross client boundaries |
| **dbt dependency auto-resolution** | fact_orders materialized before fact_transactions automatically |

---

## Staging Layer Strategy

**Naming convention**: `stg_<client>__<entity>` (Client A: stg_client_a__customer, stg_client_a__orders, etc.)

**Per-entity responsibility**:
1. **stg_client_a__customer** / **stg_client_c__customer** 
   - Rename, cast, deduplicate on natural key
   - Split Client C's CUSTOMER_NAME into first/last
   - Map loyalty tier (Client A tiers 1:1; Client C VIP→PLATINUM, REGULAR→SILVER, NEW→BRONZE)
   - Flag invalid emails

2. **stg_client_a__orders** / **stg_client_c__orders**
   - Strip inline comment annotations (e.g. "Web  <-- invalid customer")
   - Deduplicate on order_id
   - Flag missing dates, orphan customers
   - **CRITICAL for fact_transactions FK**: Ensure order exists

3. **stg_client_a__products** / **stg_client_c__products**
   - Deduplicate on SKU
   - Flag negative/zero prices (is_price_valid=FALSE)

4. **stg_client_a__transactions** (XML via XMLGET/LATERAL FLATTEN)
   - Parse nested XML; flatten items to line-item rows
   - Deduplicate on transaction_id
   - **Inner-join to stg_client_a__orders** for ORDER_ID FK validation

5. **stg_client_c__transactions** (JSON via LATERAL FLATTEN)
   - Parse nested JSON; flatten items array
   - Deduplicate, keeping row with non-empty items
   - **Inner-join to stg_client_c__orders** for ORDER_ID FK validation

6. **stg_client_c__payments** (Client C only; enriches fact_orders)
   - Deduplicate on payment_id
   - Flag negative amounts (except REFUNDED status)

---

## Mart Layer Strategy

**Build order** (dbt resolves dependencies automatically):
1. `dim_client` (seed)
2. `dim_date` (generated by macro)
3. `dim_payment_type` (seed)
4. `dim_customer` (from stg_client_a__customer + stg_client_c__customer; union; deduplicate on CUSTOMER_ID + CLIENT_KEY)
5. `dim_product` (from stg_client_a__products + stg_client_c__products; union; deduplicate on SKU + CLIENT_KEY)
6. **`fact_orders`** (from stg_client_a__orders + stg_client_c__orders + payments; aggregates line totals; joins lookups)
7. **`fact_transactions`** — materialized AFTER fact_orders (references ORDER_ID FK)

---

## Status: What's Complete

✅ Data model design (canonical star schema, 1:N constraint, anomaly handling)  
✅ Snowflake DDL (setup, raw landing, canonical schema)  
✅ dbt project scaffolding (dbt_project.yml, packages, profiles.template)  
✅ dbt schema.yml contracts and tests  
✅ Documentation (data_model.md, anomaly_report.md)  
✅ Architecture diagrams (ER, data lineage, build order)  

---

## Status: What's NOT Complete (Next Steps)

❌ **Step 2: Jenkins CI/CD Pipeline**
- Jenkinsfile with stages: lint (sqlfluff), test (dbt test), build (dbt run)
- Credential injection (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD)
- .sqlfluff config for SQL linting

❌ **Step 3: dbt Staging Models (SQL)**
- Actual SQL for all `stg_client_a__*` models (7 files)
- Actual SQL for all `stg_client_c__*` models (5 files)
- XML/JSON parsing logic
- Deduplication and anomaly flagging logic

❌ **Step 4: dbt Mart Models (SQL)**
- SQL for `dim_customer`, `dim_product` (union + deduplicate logic)
- SQL for `fact_orders` (aggregation + joins)
- SQL for `fact_transactions` (inner-join to orders for FK validation)

❌ **Step 5: Additional Documentation**
- Data modeling best practices & future improvements details
- Deployment & runbook guide
- Troubleshooting guide

---

## Quick Reference: File Locations

```
/home/grana/projects/self-learning/interview-exercises/nuaav -data/
├── snowflake/
│   └── ddl/
│       ├── 00_setup.sql               (database setup)
│       ├── 01_raw_landing.sql         (raw schema)
│       └── 02_canonical.sql           (marts schema)
├── dbt/
│   ├── dbt_project.yml                (config)
│   ├── packages.yml                   (deps)
│   ├── profiles.yml.template          (creds template)
│   └── models/
│       ├── staging/
│       │   ├── client_a/
│       │   │   └── schema.yml
│       │   └── client_c/
│       │       └── schema.yml
│       └── marts/
│           └── schema.yml
├── project_documentation/
│   └── data_model.md                  (MAIN: 600+ lines, comprehensive)
├── docs/
│   └── 01_anomaly_report.md           (all issues found)
├── prompt-exercise.txt                (original requirements)
├── prompt-context.txt                 (context & tech stack)
└── input_data/                        (source files; NOT modified)
```

---

## Critical Constraints & Assumptions

1. **ORDER_ID NOT NULL on fact_transactions** → Pipeline fails if transaction has no order
2. **Staging validates ORDER_ID FK** → Inner-join to orders; orphans dropped
3. **dbt resolves build dependencies** → fact_orders auto-materialized before fact_transactions
4. **All clients in one schema** → dim_client tags tenant; CLIENT_KEY is non-nullable on all facts
5. **Dimension FKs are nullable** → Orphaned refs → NULL + anomaly flag; staging decides whether to keep/reject
6. **All timestamps are UTC** (TIMESTAMP_NTZ)
7. **All monetary values are NUMBER(14,2); all prices are NUMBER(12,2)**
8. **Email validation regex**: `^[^@\s]+@[^@\s]+\.[^@\s]+`
9. **Client C loyalty mapping is irreversible** → VIP→PLATINUM; future Client B schema may differ

---

## For the Next Session

1. **Context needed**: This summary + the workspace structure (DDL, dbt config, documentation all exist)
2. **Starting point**: Run `dbt debug` to verify Snowflake connection; then implement Step 2 (Jenkins) or Step 3 (staging SQL models)
3. **Key files to review first**: 
   - [project_documentation/data_model.md](project_documentation/data_model.md) — comprehensive reference
   - [snowflake/ddl/02_canonical.sql](snowflake/ddl/02_canonical.sql) — schema reference
   - [dbt/models/marts/schema.yml](dbt/models/marts/schema.yml) — test contracts

---

**Project Status**: 40% complete (architecture + DDL + scaffolding). Ready for implementation (staging models → mart models → pipeline).
