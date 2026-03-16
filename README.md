# NUAAV Data Warehouse Project

A multi-client transaction data warehouse built with **dbt** and **Snowflake**, designed to unify and analyze transactional data from multiple clients with disparate data formats (XML, JSON, CSV).

## 📋 Project Overview

NUAAV is a comprehensive data integration and analytics solution that:

- **Ingests** transaction data from multiple clients (A, C) with different formats
- **Transforms** raw data through a clean staging layer
- **Enriches** data with standardized dimensions (Customer, Product, Date, Payment Type)
- **Serves** fact tables optimized for BI analysis
- **Guarantees** data quality with comprehensive NULL handling, validation, and audit flags

**Key Features:**
- ✅ Multi-format data parsing (XML, JSON, CSV)
- ✅ Stratified NULL handling with Unknown dimension records
- ✅ Duplicate detection and deduplication
- ✅ Data quality audit flags for unmatched dimensions
- ✅ Normalized numeric values (ABS for quantities/amounts)
- ✅ Star schema dimensional model
- ✅ Fully automated with dbt

---

## 🏗️ Architecture

### Layer Model

```
┌─────────────────────────────────────────────────────────┐
│  RAW LAYER (Snowflake Native)                          │
│  - CLIENT_A_TRANSACTIONS_XML                           │
│  - CLIENT_A_ORDERS, CLIENT_A_CUSTOMERS, CLIENT_A_*    │
│  - CLIENT_C_TRANSACTIONS_JSON                          │
│  - CLIENT_C_ORDERS, CLIENT_C_CUSTOMERS, CLIENT_C_*     │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  STAGING LAYER (dbt Views)                             │
│  - stg_transactions (unified XML/JSON parsing)        │
│  - stg_orders (order context + validation)             │
│  - stg_customers (customer dedup + validation)        │
│  - stg_products (product catalog with flags)          │
│  - stg_payments (payment detail + anomaly flags)      │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  DIMENSION TABLES (dbt Tables)                         │
│  - dim_client (3 rows: A, B, C)                        │
│  - dim_customer (43 + 1 Unknown record)                │
│  - dim_product (54 + 1 Unknown record)                 │
│  - dim_date (61 dates + 1 Unknown record)              │
│  - dim_payment_type (3 types + Unknown)                │
│                                                        │
│  Key Pattern: key = -1 for Unknown/Unmatched records  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  FACT TABLES (dbt Tables)                              │
│  - fact_orders (45 rows, grain: txn_id + order_id +  │
│                 item, with unmatched_* audit flags)    │
│  - fact_transactions (43 rows, grain: transaction_id, │
│                       aggregated metrics)              │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

**Client A (XML)** → XML parsing → flatten Items → filter NULLs → dedup on natural key → staging

**Client C (JSON)** → JSON extraction → split customer name → flatten items → staging

**Both** → union → final dedup → dimension lookups → fact tables with audit flags

---

## 📁 Project Structure

```
nuaav-data/
├── README.md                          # This file
├── DATA_QUALITY_SUMMARY.md            # Detailed NULL handling strategy
├── prompt-context.txt                 # Original requirements/context
├── prompt-exercise.txt                # Exercise details
├──
├── input_data/                        # Raw data files
│   ├── ClientA_Transactions_*.xml     # XML transactions
│   ├── ClientA_*.csv                  # Orders, Customers, Products
│   ├── Client B/                      # Client C data (named B in folder)
│   │   ├── transactions.json
│   │   ├── *.csv
│   └── ...
│
├── dbt/                               # dbt project root
│   ├── dbt_project.yml                # dbt configuration
│   ├── profiles.yml                   # Snowflake connection (user-local)
│   ├── models/
│   │   ├── staging/                   # Staging layer (views)
│   │   │   ├── stg_transactions.sql   # Core: XML/JSON parsing, flatten items
│   │   │   ├── stg_orders.sql         # Order context + validation flags
│   │   │   ├── stg_customers.sql      # Customer dedup + email validation
│   │   │   ├── stg_products.sql       # Product catalog + price validation
│   │   │   └── stg_payments.sql       # Payment details + anomaly detection
│   │   │
│   │   └── marts/                     # Analytics layer (tables)
│   │       ├── dim_client.sql         # Client dimension
│   │       ├── dim_customer.sql       # Customer + Unknown (-1) record
│   │       ├── dim_product.sql        # Product + Unknown (-1) record
│   │       ├── dim_date.sql           # Date spine + Unknown (-1) record
│   │       ├── dim_payment_type.sql   # Payment methods
│   │       ├── fact_orders.sql        # Line-item facts with audit flags
│   │       ├── fact_transactions.sql  # Transaction header facts
│   │       └── fact_orders_product_coverage.sql  # Debug query
│   │
│   ├── seeds/
│   │   └── (source data defined via sources.yml)
│   │
│   ├── schema.yml                     # Column documentation
│   ├── sources.yml                    # Raw source definitions
│   └── macros/                        # (if any custom macros)
│
├── project_documentation/
│   └── data_model.md                  # High-level architecture docs
│
└── docs/                              # Additional documentation
```

---

## 🚀 Getting Started

### Prerequisites

- **Snowflake** account with credentials
- **dbt CLI** (version 1.11.7+)
- **Python** 3.9+ (for dbt)

### Setup

#### 1. Clone/Navigate to Project
```bash
cd ~/projects/self-learning/interview-exercises/nuaav-data/dbt
```

#### 2. Configure Snowflake Connection
The `profiles.yml` uses environment variables for secure credential management. Set these variables in your shell:

```bash
export SNOWFLAKE_ACCOUNT=your-account-id
export SNOWFLAKE_USER=your-username
export SNOWFLAKE_PASSWORD=your-password
```

**Notes:**
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (e.g., `xy12345.us-east-1`)
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_PASSWORD`: Your Snowflake password
- The `profiles.yml` already has the correct configuration — no need to edit it

To persist these variables across shell sessions, add them to your `~/.bashrc` or `~/.zshrc`:
```bash
export SNOWFLAKE_ACCOUNT=your-account-id
export SNOWFLAKE_USER=your-username
export SNOWFLAKE_PASSWORD=your-password
```

#### 3. Verify Configuration
```bash
dbt debug
```

#### 4. Load Seed Data (Raw Files)
```bash
dbt seed
```

This loads data from `input_data/` into Snowflake RAW schema.

#### 5. Run dbt Models
```bash
# Build all models (staging + marts)
dbt build

# Or run specific layers
dbt run --select staging              # Staging views only
dbt run --select marts                # Mart tables only

# Or build with specific selection
dbt build --select fact_orders        # Specific model
dbt build --select tag:fact           # By tag
```

#### 6. View Results
```bash
# Preview staging layer
dbt show --select stg_transactions --limit 10

# Preview fact tables
dbt show --select fact_transactions --limit 10
dbt show --select fact_orders --limit 10
```

---

## 📊 Models & Grain

### Staging Models (Views)

| Model | Grain | Rows | Purpose |
|-------|-------|------|---------|
| **stg_transactions** | Line-item | ~88 | Unified transaction items from XML/JSON |
| **stg_orders** | Order | ~20 | Order context + validation flags |
| **stg_customers** | Customer | ~43 | Deduplicated customers + email validation |
| **stg_products** | Product | ~54 | Product catalog + price validation |
| **stg_payments** | Payment | ~1 | Payment detail (Client C only currently) |

**Key Features:**
- ✅ Dual-path extraction for XML items (multi vs. single-item transactions)
- ✅ NULL filtering on all 4 item fields (SKU, Description, Quantity, UnitPrice)
- ✅ Validation flags (has_missing_date, has_invalid_ref, has_negative_amt, is_price_valid)
- ✅ Deduplication per client (keeping first occurrence)
- ✅ Data normalization (ABS for quantities/amounts)

### Dimension Models (Tables)

| Model | Rows | Key Field | Unknown Record |
|-------|------|-----------|-----------------|
| **dim_client** | 3 | client_key | N/A |
| **dim_customer** | 44 | customer_key=-1 | ✅ "Unknown Customer" |
| **dim_product** | 55 | product_key=-1 | ✅ "Unknown Product" |
| **dim_date** | 62 | date_key=-1 | ✅ "Unknown" date |
| **dim_payment_type** | 4 | payment_type_key | ✅ "Unknown" method |

**Unknown Records:**
- All dimension keys support **-1** for unmatched/missing values
- Enables safe BI joins without NULL handling complexity
- Dimension attributes default to 'Unknown' strings for readability

### Fact Models (Tables)

#### fact_orders (Line-item Facts)
- **Grain**: transaction_id + order_id + item/product
- **Rows**: 45
- **Key Columns**: 
  - Customer Key, Product Key, Date Key, Payment Type Key
  - Quantity, Unit Price, Line Total, Payment Amount
  - Audit flags: unmatched_customer, unmatched_product, unmatched_date

#### fact_transactions (Transaction Header Facts)
- **Grain**: transaction_id
- **Rows**: 43
- **Key Columns**:
  - Customer Key, Date Key, Payment Type Key
  - Order Status, Order Channel
  - Order Count, Item Count, Total Quantity, Total Line Amount, Payment Amount
  - Audit flags: unmatched_customer, unmatched_date

---

## 🔍 Data Quality Features

### NULL Handling Strategy (Stratified)

**Dimension Keys** → Default to **-1** + lookup Unknown dimension
```sql
COALESCE(dcust.customer_key, -1) AS customer_key
```

**Dimension Attributes** → Default to **'Unknown'** string
```sql
COALESCE(ta.order_status, 'Unknown') AS order_status
```

**Measures** → Default to **0** with ABS normalization
```sql
ABS(COALESCE(st.quantity * st.unit_price, 0)) AS line_total
```

### Audit Flags

Every fact table includes data quality flags to identify problematic records:

```sql
CASE WHEN dcust.customer_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_customer
CASE WHEN dp.product_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_product
CASE WHEN dd.date_key IS NULL THEN TRUE ELSE FALSE END AS unmatched_date
```

**Monitor Data Quality:**
```sql
-- Find unmatched customers
SELECT COUNT(*) FROM fact_orders WHERE unmatched_customer = TRUE;

-- Get clean records only
SELECT * FROM fact_orders WHERE unmatched_customer = FALSE AND unmatched_product = FALSE;
```

### Validation Filters

**stg_orders** now ACTIVELY filters:
```sql
WHERE rn = 1
AND has_missing_date = FALSE      -- Exclude invalid dates
AND has_invalid_ref = FALSE       -- Exclude missing customer refs
```

---

## 🔧 Running Specific Tasks

### Build Only Staging Layer
```bash
dbt run --select staging
```

### Build Only Dimensions
```bash
dbt run --select tag:dim
```

### Build Only Facts
```bash
dbt run --select tag:fact
```

### Test Models
```bash
dbt test                          # Run all tests
dbt test --select fact_orders     # Test specific model
```

### Generate Documentation
```bash
dbt docs generate
dbt docs serve                    # Opens docs at localhost:8000
```

### Rebuild from Scratch
```bash
dbt clean                         # Remove target/artifacts
dbt seed                          # Reload raw data
dbt build                         # Full rebuild
```

---

## 📈 Key Insights from Data

### Transaction Volumes
- **Total Line Items**: 45 (fact_orders)
- **Total Transactions**: 43 (fact_transactions)
- **Customers**: 44 real + 1 Unknown
- **Products**: 54 real + 1 Unknown
- **Date Range**: 2025-11-01 to 2025-12-31

### Data Quality Status
- ✅ **Zero NULLs in dimension keys** (all have valid FK references)
- ✅ **All dates linked** (61 actual dates + 1 Unknown date)
- ✅ **Duplicate handling**: Dedup on natural keys per client
- ✅ **Negative amounts normalized**: All quantities/prices converted to positive via ABS()

### Known Issues & Workarounds
- **XML Item Extraction**: Some single-item transactions required dual-path extraction (with/without `:"$"` accessor)
- **Customer Names**: XML shows occasional LastName/LastLastName variations (handled with COALESCE)
- **Date Matching**: Order dates must be parseable as YYYY-MM-DD

---

## 🛠️ Troubleshooting

### "Connection test failed"
```bash
# Verify credentials in ~/.dbt/profiles.yml
dbt debug

# Check Snowflake account/role/database exist
snowsql -c <connection-name> -q "SELECT 1;"
```

### "Model failed to compile"
```bash
# Check for syntax errors
dbt parse

# Show compiled SQL
dbt show --select <model_name>

# View error details
dbt build --select <model_name> --debug
```

### "Unmatched dimension keys appearing"
```bash
# Check what's unmatched
SELECT * FROM fact_orders WHERE unmatched_product = TRUE;

# Verify dimension has the record
SELECT * FROM dim_product WHERE sku = <unmatched_sku>;

# Add missing product to source data if needed
```

---

## 📚 Documentation

- **[DATA_QUALITY_SUMMARY.md](DATA_QUALITY_SUMMARY.md)** — Detailed NULL handling strategy and implementation
- **dbt docs** — Generated via `dbt docs generate` and `dbt docs serve`
  - Run `dbt docs serve` and navigate to the documentation site
  - For deeper architectural insights, see **[data_model.md](dbt/docs/data_model.md)** inside dbt docs — covers dimensional design, star schema, data lineage, and query examples
  - All model columns are documented in `schema.yml`

---

## 🔄 Development Workflow

### Adding a New Model
1. Create `.sql` file in `models/staging/` or `models/marts/`
2. Define `{{ config(...) }}` block (materialization, schema, tags)
3. Reference upstream models: `{{ ref('stg_transactions') }}`
4. Reference source data: `{{ source('nuaav_raw', 'CLIENT_A_ORDERS') }}`
5. Run: `dbt run --select <your_model>`
6. Test: `dbt test --select <your_model>`

### Updating Documentation
1. Edit `schema.yml` to add column descriptions
2. Run: `dbt docs generate && dbt docs serve`
3. Preview at `http://localhost:8000`

### Creating a New Dimension
1. Create staging model (if raw data needs cleanup)
2. Create dimension model with Unknown record (**key = -1**)
3. Update fact table joins to include new dimension
4. Add audit flag if dimension is optional (LEFT JOIN)
5. Test: Verify no NULL keys in fact tables

---

## � Roadmap & Future Enhancements

The following components are recommended for production deployment:

### **Airflow Orchestration**
- **Purpose**: Schedule and monitor the dbt pipeline
- **Use case**: Daily/hourly triggers of `dbt build`, error notifications, retry logic
- **Benefit**: Decouples pipeline execution from manual triggers; enables complex DAGs

### **Jenkins CI/CD Pipeline**
- **Purpose**: Automate testing, building, and deployment of dbt models
- **Use case**: 
  - Trigger `dbt test` on every pull request
  - Validate model syntax and dependencies before merge
  - Auto-deploy to production on main branch merge
- **Benefit**: Prevents broken models from reaching production; enables rapid iteration with safety

Both are already referenced in code comments (e.g., `profiles.yml` mentions Jenkins credentials injection). Integration is straightforward once infrastructure is in place.

For issues, questions, or feature requests, refer to:
- **Schema Documentation**: `schema.yml` (column-level docs)
- **Model Comments**: SQL files contain detailed comments
- **dbt Docs**: Run `dbt docs serve` for interactive documentation

---

## 📝 License & Attribution

This is a self-learning interview exercise project demonstrating:
- Multi-client data integration
- dbt best practices
- Snowflake transformation patterns  
- Data warehouse dimensional modeling
- Production-grade data quality handling

**Created**: March 2026  
**Framework**: dbt 1.11.7 + Snowflake
