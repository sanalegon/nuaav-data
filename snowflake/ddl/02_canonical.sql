-- =============================================================================
-- FILE        : 02_canonical.sql
-- PURPOSE     : DDL for the canonical Kimball star schema in NUAAV_DW.MARTS.
--               Tables are created here for reference and documentation.
--               In practice dbt manages these objects; run this file only
--               if you want to pre-create them outside of dbt.
-- RUN AFTER   : 00_setup.sql
-- =============================================================================

USE DATABASE  NUAAV_DW;
USE SCHEMA    NUAAV_DW.MARTS;
USE WAREHOUSE NUAAV_WH;

-- ===========================================================================
-- DIMENSION: DIM_CLIENT
-- Identifies the originating client (data provider).
-- Currently two active clients: A and C.
-- Client B folder contained Client C data (naming mismatch in source delivery).
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.MARTS.DIM_CLIENT (
    CLIENT_KEY         INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,
    CLIENT_ID          VARCHAR(10)   NOT NULL UNIQUE,   -- 'A', 'B', 'C'
    CLIENT_NAME        VARCHAR(100),
    SOURCE_FOLDER      VARCHAR(200)  COMMENT 'Original folder name in the delivery',
    IS_ACTIVE          BOOLEAN       DEFAULT TRUE,
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed static client reference data
INSERT INTO NUAAV_DW.MARTS.DIM_CLIENT (CLIENT_ID, CLIENT_NAME, SOURCE_FOLDER, IS_ACTIVE)
VALUES
    ('A', 'Client A', 'input_data/',    TRUE),
    -- NOTE: Source folder is named "Client B" but internal data labels are "Client C".
    --       Ingested as Client C; the folder name is a delivery artefact.
    ('C', 'Client C', 'input_data/Client B/', TRUE),
    -- Placeholder: no data received yet for Client B.
    ('B', 'Client B', NULL,             FALSE);

-- ===========================================================================
-- DIMENSION: DIM_DATE
-- Date spine used by fact_transactions (and any future fact tables).
-- Populated by a dbt macro or a stored procedure over the required date range.
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.MARTS.DIM_DATE (
    DATE_KEY           INTEGER       NOT NULL PRIMARY KEY,  -- YYYYMMDD
    FULL_DATE          DATE          NOT NULL UNIQUE,
    YEAR               INTEGER       NOT NULL,
    QUARTER            INTEGER       NOT NULL,              -- 1-4
    MONTH              INTEGER       NOT NULL,              -- 1-12
    MONTH_NAME         VARCHAR(20)   NOT NULL,
    WEEK_OF_YEAR       INTEGER       NOT NULL,              -- ISO week
    DAY_OF_MONTH       INTEGER       NOT NULL,              -- 1-31
    DAY_OF_WEEK        INTEGER       NOT NULL,              -- 0=Sun … 6=Sat
    DAY_NAME           VARCHAR(20)   NOT NULL,
    IS_WEEKEND         BOOLEAN       NOT NULL,
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===========================================================================
-- DIMENSION: DIM_CUSTOMER
-- Unified customer dimension across all clients.
-- Client A provides first_name / last_name / loyalty_tier / signup_source.
-- Client C provides a combined name / segment.
-- Canonical mapping:
--   loyalty_tier ← Client A tier (GOLD/SILVER/BRONZE/PLATINUM)  /
--                  Client C segment (VIP → PLATINUM, NEW → BRONZE, REGULAR → SILVER)
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.MARTS.DIM_CUSTOMER (
    CUSTOMER_KEY       INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID        VARCHAR(50)   NOT NULL,             -- natural key from source
    CLIENT_KEY         INTEGER       NOT NULL REFERENCES NUAAV_DW.MARTS.DIM_CLIENT(CLIENT_KEY),
    FIRST_NAME         VARCHAR(100),
    LAST_NAME          VARCHAR(100),
    FULL_NAME          VARCHAR(200),
    EMAIL              VARCHAR(200),
    LOYALTY_TIER       VARCHAR(50),   -- canonical: PLATINUM/GOLD/SILVER/BRONZE/UNKNOWN
    SIGNUP_SOURCE      VARCHAR(100),  -- Client A only; NULL for Client C
    IS_ACTIVE          BOOLEAN        DEFAULT TRUE,
    IS_EMAIL_VALID     BOOLEAN        DEFAULT TRUE,  -- FALSE if email fails basic regex
    LOADED_AT          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_customer UNIQUE (CUSTOMER_ID, CLIENT_KEY)
);

-- ===========================================================================
-- DIMENSION: DIM_PRODUCT
-- Unified product / SKU catalogue across all clients.
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.MARTS.DIM_PRODUCT (
    PRODUCT_KEY        INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,
    SKU                VARCHAR(50)   NOT NULL,
    CLIENT_KEY         INTEGER       NOT NULL REFERENCES NUAAV_DW.MARTS.DIM_CLIENT(CLIENT_KEY),
    PRODUCT_NAME       VARCHAR(200),
    CATEGORY           VARCHAR(100),
    UNIT_PRICE         NUMBER(12, 2),
    CURRENCY           VARCHAR(10)   DEFAULT 'USD',
    IS_ACTIVE          BOOLEAN       DEFAULT TRUE,
    IS_PRICE_VALID     BOOLEAN       DEFAULT TRUE,  -- FALSE when price <= 0
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_product UNIQUE (SKU, CLIENT_KEY)
);

-- ===========================================================================
-- DIMENSION: DIM_PAYMENT_TYPE
-- Small lookup dimension for payment methods observed across sources.
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.MARTS.DIM_PAYMENT_TYPE (
    PAYMENT_TYPE_KEY   INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,
    PAYMENT_METHOD     VARCHAR(100)  NOT NULL UNIQUE,
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed known payment methods
INSERT INTO NUAAV_DW.MARTS.DIM_PAYMENT_TYPE (PAYMENT_METHOD)
VALUES ('CreditCard'), ('PayPal'), ('BankTransfer'), ('Unknown');

-- ===========================================================================
-- FACT TABLE: FACT_TRANSACTIONS
-- Grain: one row per transaction line-item (one SKU per transaction).
-- Connects to dimensions and to FACT_ORDERS via ORDER_ID.
-- Every transaction MUST belong to an order: ORDER_ID is NOT NULL and FK.
-- Anomaly flags are surfaced here so analysts can easily filter bad data.
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.MARTS.FACT_TRANSACTIONS (
    TRANSACTION_KEY    INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,

    -- Natural keys (kept for traceability)
    TRANSACTION_ID     VARCHAR(50)   NOT NULL,
    ORDER_ID           VARCHAR(50)   NOT NULL,  -- FK to fact_orders: every transaction belongs to an order

    -- Dimension foreign keys
    CLIENT_KEY         INTEGER       NOT NULL REFERENCES NUAAV_DW.MARTS.DIM_CLIENT(CLIENT_KEY),
    CUSTOMER_KEY       INTEGER       REFERENCES NUAAV_DW.MARTS.DIM_CUSTOMER(CUSTOMER_KEY),
    PRODUCT_KEY        INTEGER       REFERENCES NUAAV_DW.MARTS.DIM_PRODUCT(PRODUCT_KEY),
    DATE_KEY           INTEGER       REFERENCES NUAAV_DW.MARTS.DIM_DATE(DATE_KEY),
    PAYMENT_TYPE_KEY   INTEGER       REFERENCES NUAAV_DW.MARTS.DIM_PAYMENT_TYPE(PAYMENT_TYPE_KEY),
    
    -- Fact-to-fact relationship
    -- CONSTRAINT fk_transactions_orders 
    --     FOREIGN KEY (ORDER_ID, CLIENT_KEY) 
    --     REFERENCES NUAAV_DW.MARTS.FACT_ORDERS (ORDER_ID, CLIENT_KEY),

    -- Measures
    QUANTITY           INTEGER,
    UNIT_PRICE         NUMBER(12, 2),
    LINE_TOTAL         NUMBER(14, 2),  -- QUANTITY * UNIT_PRICE (computed in staging)
    PAYMENT_AMOUNT     NUMBER(14, 2),
    ORDER_STATUS       VARCHAR(50),
    ORDER_CHANNEL      VARCHAR(100),

    -- Anomaly / data-quality flags
    -- These allow analysts to include or exclude bad rows without data loss.
    IS_DUPLICATE       BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when transaction_id appears more than once',
    HAS_NEGATIVE_QTY   BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when quantity < 0',
    HAS_NEGATIVE_AMT   BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when payment_amount or unit_price < 0',
    HAS_MISSING_DATE   BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when order_date could not be parsed',
    HAS_INVALID_REF    BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when customer_id or sku not found in dims',
    HAS_MISSING_SKU    BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when sku is NULL or empty',
    ANOMALY_NOTES      VARCHAR(1000) COMMENT 'Pipe-delimited list of all anomaly reasons for this row',

    -- Audit
    SOURCE_FILE        VARCHAR(200),
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===========================================================================
-- FACT TABLE: FACT_ORDERS
-- Grain: one row per order (order-level summary).
-- Connects to four dimensions (no product dimension at order level).
-- Anomaly flags allow analysts to filter clean data.
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.MARTS.FACT_ORDERS (
    ORDER_KEY          INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,

    -- Natural keys (kept for traceability)
    ORDER_ID           VARCHAR(50)   NOT NULL,

    -- Dimension foreign keys
    CLIENT_KEY         INTEGER       NOT NULL REFERENCES NUAAV_DW.MARTS.DIM_CLIENT(CLIENT_KEY),
    CUSTOMER_KEY       INTEGER       REFERENCES NUAAV_DW.MARTS.DIM_CUSTOMER(CUSTOMER_KEY),
    DATE_KEY           INTEGER       REFERENCES NUAAV_DW.MARTS.DIM_DATE(DATE_KEY),
    PAYMENT_TYPE_KEY   INTEGER       REFERENCES NUAAV_DW.MARTS.DIM_PAYMENT_TYPE(PAYMENT_TYPE_KEY),

    -- Measures
    TOTAL_AMOUNT       NUMBER(14, 2),  -- Sum of all line items in this order
    ITEM_COUNT         INTEGER,        -- Number of distinct line items
    PAYMENT_AMOUNT     NUMBER(14, 2),  -- Total payment amount (typically = TOTAL_AMOUNT)

    -- Anomaly / data-quality flags
    -- These allow analysts to include or exclude bad rows without data loss.
    IS_DUPLICATE       BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when order_id appears more than once',
    HAS_NEGATIVE_AMT   BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when total_amount < 0',
    HAS_MISSING_DATE   BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when order_date could not be parsed',
    HAS_INVALID_REF    BOOLEAN       DEFAULT FALSE COMMENT 'TRUE when customer_id not found in dim_customer',
    ANOMALY_NOTES      VARCHAR(1000) COMMENT 'Pipe-delimited list of all anomaly reasons for this row',

    -- Audit
    SOURCE_FILE        VARCHAR(200),
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
