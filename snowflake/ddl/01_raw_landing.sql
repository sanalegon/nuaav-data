-- =============================================================================
-- FILE        : 01_raw_landing.sql
-- PURPOSE     : DDL for the RAW landing zone.
--               • CSV columns are all VARCHAR – no type casting at ingestion.
--               • XML / JSON files are loaded as VARIANT to preserve structure.
--               • Snowflake internal stages and file formats are defined here.
-- RUN AFTER   : 00_setup.sql
-- =============================================================================

USE DATABASE NUAAV_DW;
USE SCHEMA   NUAAV_DW.RAW;
USE WAREHOUSE NUAAV_WH;

-- ===========================================================================
-- FILE FORMATS
-- ===========================================================================

-- CSV: the source files include a "START OF FILE" annotation on line 1 and
-- the actual column header on line 2, followed by a blank line.
-- SKIP_HEADER = 2 skips both non-data lines; SKIP_BLANK_LINES handles the gap.
CREATE OR REPLACE FILE FORMAT NUAAV_DW.RAW.CSV_FORMAT
    TYPE                      = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                   = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL       = TRUE
    SKIP_HEADER               = 2        -- skips "START OF FILE" + header row
    SKIP_BLANK_LINES          = TRUE
    TRIM_SPACE                = TRUE
    COMMENT                   = 'Annotated CSV sources – skips 2 preamble lines';

CREATE OR REPLACE FILE FORMAT NUAAV_DW.RAW.JSON_FORMAT
    TYPE              = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    COMMENT           = 'JSON semi-structured transactions';

CREATE OR REPLACE FILE FORMAT NUAAV_DW.RAW.XML_FORMAT
    TYPE    = 'XML'
    COMMENT = 'XML semi-structured transactions';

-- ===========================================================================
-- AWS CREDENTIALS (set via environment variables or Snowflake secrets)
-- ===========================================================================
-- IMPORTANT: Before using the external stages below, you must:
--
-- 1. CREATE AN IAM ROLE in AWS that allows Snowflake to access your S3 bucket:
--    a) Go to AWS IAM → Roles → Create Role
--    b) Select "Snowflake" as the trusted entity
--    c) Attach a policy allowing s3:GetObject and s3:ListBucket on your bucket:
--       {
--         "Version": "2012-10-17",
--         "Statement": [
--           {
--             "Effect": "Allow",
--             "Action": [
--               "s3:GetObject",
--               "s3:ListBucket"
--             ],
--             "Resource": [
--               "arn:aws:s3:::YOUR-BUCKET-NAME",
--               "arn:aws:s3:::YOUR-BUCKET-NAME/*"
--             ]
--           }
--         ]
--       }
--    d) Copy the IAM Role ARN (arn:aws:iam::ACCOUNT:role/snowflake-access-role)
--
-- 2. Option A (Recommended): Use AWS_ROLE with Snowflake integration
--    Replace AWS_ROLE_ARN below with your IAM role ARN
--
-- 3. Option B (Fallback): Use AWS Access Key ID and Secret Access Key
--    a) Create an IAM user with S3 access permissions
--    b) Generate access keys in AWS IAM
--    c) Replace the placeholder values below
--
-- Option 1 (Environment Variables - RECOMMENDED):
--   Before running this script, set:
--   export SNOWFLAKE_AWS_ACCESS_KEY_ID="your-access-key"
--   export SNOWFLAKE_AWS_SECRET_ACCESS_KEY="your-secret-key"
--
-- Option 2 (Hardcoded - NOT RECOMMENDED for production):
--   Replace {AWS_ACCESS_KEY} and {AWS_SECRET_KEY} below with actual values.

-- ===========================================================================
-- EXTERNAL STAGES  (S3 bucket)
-- ===========================================================================
CREATE OR REPLACE STAGE NUAAV_DW.RAW.STAGE_CLIENT_A
    URL = 's3://[YOUR-BUCKET-NAME]/nuaav/input_data/'
    CREDENTIALS = (
        AWS_KEY_ID = 'access_key_placeholder',  
        AWS_SECRET_KEY = 'secret_key_placeholder'
    )
    COMMENT = 'External S3 stage for Client A source files';

CREATE OR REPLACE STAGE NUAAV_DW.RAW.STAGE_CLIENT_C
    URL = 's3://[YOUR-BUCKET-NAME]/nuaav/input_data/'
    CREDENTIALS = (
        AWS_KEY_ID = 'access_key_placeholder',
        AWS_SECRET_KEY = 'secret_key_placeholder'
    )
    COMMENT = 'External S3 stage for Client C files (source folder: "Client B")';

-- ===========================================================================
-- CLIENT A  –  CSV tables
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_A_CUSTOMERS (
    CUSTOMER_ID        VARCHAR,
    FIRST_NAME         VARCHAR,
    LAST_NAME          VARCHAR,
    EMAIL              VARCHAR,
    LOYALTY_TIER       VARCHAR,
    SIGNUP_SOURCE      VARCHAR,
    IS_ACTIVE          VARCHAR,
    _SOURCE_FILE       VARCHAR       DEFAULT 'Customer.csv',
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_A_ORDERS (
    ORDER_ID           VARCHAR,
    CUSTOMER_ID        VARCHAR,
    ORDER_DATE         VARCHAR,
    ORDER_STATUS       VARCHAR,
    CHANNEL            VARCHAR,   -- NB: inline annotations ("  <-- ...") are
                                  -- part of this field value and stripped in staging
    _SOURCE_FILE       VARCHAR       DEFAULT 'Orders.csv',
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_A_PRODUCTS (
    SKU                VARCHAR,
    PRODUCT_NAME       VARCHAR,
    CATEGORY           VARCHAR,
    UNIT_PRICE         VARCHAR,
    CURRENCY           VARCHAR,
    IS_ACTIVE          VARCHAR,
    _SOURCE_FILE       VARCHAR       DEFAULT 'Products.csv',
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===========================================================================
-- CLIENT A  –  XML transaction files (7 files + 1 .txt continuation)
-- Each XML document is loaded as one VARIANT row per file.
-- Rows are later LATERAL FLATTEN-ed in the staging layer.
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_A_TRANSACTIONS_XML (
    _SOURCE_FILE       VARCHAR,
    _RAW_XML           VARIANT,
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===========================================================================
-- CLIENT C  –  CSV tables
-- NOTE: Source files reside in the "Client B" folder but all internal labels
--       and IDs reference Client C.  No Client B data exists in this delivery.
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_C_CUSTOMERS (
    CUSTOMER_ID        VARCHAR,
    CUSTOMER_NAME      VARCHAR,
    EMAIL              VARCHAR,
    SEGMENT            VARCHAR,
    IS_ACTIVE          VARCHAR,
    _SOURCE_FILE       VARCHAR       DEFAULT 'Client B/Customer.CSV',
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_C_ORDERS (
    ORDER_ID           VARCHAR,
    CUSTOMER_ID        VARCHAR,
    ORDER_DATE         VARCHAR,
    ORDER_STATUS       VARCHAR,
    _SOURCE_FILE       VARCHAR       DEFAULT 'Client B/Order.csv',
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_C_PRODUCTS (
    SKU                VARCHAR,
    PRODUCT_NAME       VARCHAR,
    CATEGORY           VARCHAR,
    UNIT_PRICE         VARCHAR,
    CURRENCY           VARCHAR,
    IS_ACTIVE          VARCHAR,
    _SOURCE_FILE       VARCHAR       DEFAULT 'Client B/Product.csv',
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_C_PAYMENTS (
    PAYMENT_ID         VARCHAR,
    ORDER_ID           VARCHAR,
    PAYMENT_METHOD     VARCHAR,
    AMOUNT             VARCHAR,
    CURRENCY           VARCHAR,
    STATUS             VARCHAR,
    _SOURCE_FILE       VARCHAR       DEFAULT 'Client B/Payments.csv',
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===========================================================================
-- CLIENT C  –  JSON transaction file
-- ===========================================================================
CREATE OR REPLACE TABLE NUAAV_DW.RAW.CLIENT_C_TRANSACTIONS_JSON (
    _SOURCE_FILE       VARCHAR,
    _RAW_JSON          VARIANT,
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===========================================================================
-- COPY INTO COMMANDS  (files loaded from S3)
-- ===========================================================================
-- Note: Replace {AWS_ACCESS_KEY} and {AWS_SECRET_KEY} in the stage definitions above
--       with actual credentials from your AWS IAM user, or use environment variables.
--
-- Step 1 – load CSV tables for Client A (columns matched positionally after skipping preamble)
COPY INTO NUAAV_DW.RAW.CLIENT_A_CUSTOMERS (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, LOYALTY_TIER, SIGNUP_SOURCE, IS_ACTIVE)
    FROM  @NUAAV_DW.RAW.STAGE_CLIENT_A/Customer.csv
    FILE_FORMAT = (FORMAT_NAME = 'NUAAV_DW.RAW.CSV_FORMAT')
    ON_ERROR    = 'CONTINUE';

COPY INTO NUAAV_DW.RAW.CLIENT_A_ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, CHANNEL)
    FROM  @NUAAV_DW.RAW.STAGE_CLIENT_A/Orders.csv
    FILE_FORMAT = (FORMAT_NAME = 'NUAAV_DW.RAW.CSV_FORMAT')
    ON_ERROR    = 'CONTINUE';

COPY INTO NUAAV_DW.RAW.CLIENT_A_PRODUCTS (SKU, PRODUCT_NAME, CATEGORY, UNIT_PRICE, CURRENCY, IS_ACTIVE)
    FROM  @NUAAV_DW.RAW.STAGE_CLIENT_A/Products.csv
    FILE_FORMAT = (FORMAT_NAME = 'NUAAV_DW.RAW.CSV_FORMAT')
    ON_ERROR    = 'CONTINUE';

----
-- Step 2 – load XML files for Client A as VARIANT (one row per file)
COPY INTO NUAAV_DW.RAW.CLIENT_A_TRANSACTIONS_XML (_SOURCE_FILE, _RAW_XML)
    FROM (
        SELECT METADATA$FILENAME, $1
        FROM   @NUAAV_DW.RAW.STAGE_CLIENT_A
        (FILE_FORMAT => 'NUAAV_DW.RAW.XML_FORMAT', PATTERN => '.*ClientA_Transactions.*\.xml')
    );

-- Step 3 – load CSV tables for Client C
COPY INTO NUAAV_DW.RAW.CLIENT_C_CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, SEGMENT, IS_ACTIVE)
    FROM  @NUAAV_DW.RAW.STAGE_CLIENT_C/Client_B/Customer.CSV
    FILE_FORMAT = (FORMAT_NAME = 'NUAAV_DW.RAW.CSV_FORMAT')
    ON_ERROR    = 'CONTINUE';

COPY INTO NUAAV_DW.RAW.CLIENT_C_ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS)
    FROM  @NUAAV_DW.RAW.STAGE_CLIENT_C/Client_B/Order.csv
    FILE_FORMAT = (FORMAT_NAME = 'NUAAV_DW.RAW.CSV_FORMAT')
    ON_ERROR    = 'CONTINUE';

COPY INTO NUAAV_DW.RAW.CLIENT_C_PRODUCTS (SKU, PRODUCT_NAME, CATEGORY, UNIT_PRICE, CURRENCY, IS_ACTIVE)
    FROM  @NUAAV_DW.RAW.STAGE_CLIENT_C/Client_B/Product.csv
    FILE_FORMAT = (FORMAT_NAME = 'NUAAV_DW.RAW.CSV_FORMAT')
    ON_ERROR    = 'CONTINUE';

COPY INTO NUAAV_DW.RAW.CLIENT_C_PAYMENTS (PAYMENT_ID, ORDER_ID, PAYMENT_METHOD, AMOUNT, CURRENCY, STATUS)
    FROM  @NUAAV_DW.RAW.STAGE_CLIENT_C/Client_B/Payments.csv
    FILE_FORMAT = (FORMAT_NAME = 'NUAAV_DW.RAW.CSV_FORMAT')
    ON_ERROR    = 'CONTINUE';

-- Step 4 – load JSON file for Client C as VARIANT
COPY INTO NUAAV_DW.RAW.CLIENT_C_TRANSACTIONS_JSON (_SOURCE_FILE, _RAW_JSON)
    FROM (
        SELECT METADATA$FILENAME, $1
        FROM   @NUAAV_DW.RAW.STAGE_CLIENT_C/Client_B
        (FILE_FORMAT => 'NUAAV_DW.RAW.JSON_FORMAT', PATTERN => '.*transactions.json')
    );
