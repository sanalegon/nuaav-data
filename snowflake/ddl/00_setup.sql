-- =============================================================================
-- FILE        : 00_setup.sql
-- PURPOSE     : One-time setup of the Snowflake database, schemas, warehouse,
--               and file formats for the NUAAV Data Warehouse.
-- RUN AS ROLE : SYSADMIN (or ACCOUNTADMIN)
-- NOTE        : Credentials are injected by Jenkins at pipeline runtime;
--               never hard-code them here.
-- =============================================================================

USE ROLE SYSADMIN;
-- TODO: CREATE ROLE FOR THIS APP
-- ---------------------------------------------------------------------------
-- Database
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS NUAAV_DW
    COMMENT = 'NUAAV financial transaction data warehouse';

-- ---------------------------------------------------------------------------
-- Schemas  (three-tier architecture)
-- ---------------------------------------------------------------------------
-- RAW      : landing zone – files ingested with zero transformation
CREATE SCHEMA IF NOT EXISTS NUAAV_DW.RAW
    COMMENT = 'Raw landing zone: source files loaded exactly as received';

-- STAGING  : cleaned, standardised, deduped data  (managed by dbt)
CREATE SCHEMA IF NOT EXISTS NUAAV_DW.STAGING
    COMMENT = 'Staging layer: rename, cast, deduplicate, flag anomalies';

-- MARTS    : canonical Kimball star schema             (managed by dbt)
CREATE SCHEMA IF NOT EXISTS NUAAV_DW.MARTS
    COMMENT = 'Marts layer: dimensions and fact tables (star schema)';

-- ---------------------------------------------------------------------------
-- Virtual Warehouse
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS NUAAV_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'NUAAV compute warehouse – auto-suspends after 60 s';

USE WAREHOUSE NUAAV_WH;
USE DATABASE  NUAAV_DW;
