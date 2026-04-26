/*====================================================================
File Name   : staging_and_audit_objects.sql
Script Type : DDL Deployment Script
Project     :  DataWarehouse
Layer       : Staging + Bronze Control Metadata

Author      : Amr Anter
Created On  : 2026-04-26
Version     : 1.0

Purpose:
    Creates staging landing tables for source-file ingestion and
    deploys the Bronze audit control table used for load monitoring.

Scope:
    This script performs:
      - Drop and recreate of staging landing tables
      - Drop and recreate of bronze.load_audit
      - Initialization of raw file landing structures
      - Deployment of ingestion monitoring metadata objects

Source Systems Covered:
    CRM
      - staging.crm_cust_info
      - staging.crm_prd_info
      - staging.crm_sales_details

    ERP
      - staging.erp_cust_az12
      - staging.erp_loc_a101
      - staging.erp_px_cat_g1v2

Control Metadata Objects
      - bronze.load_audit

Design Intent:
    Staging Layer
      - Temporary raw landing zone for source files
      - Mirrors source extract structure
      - Supports truncate-and-reload ingestion pattern

    Audit Layer
      - Stores batch execution metadata
      - Supports observability and restartability
      - Provides load lineage controls

Design Notes:
    - Staging tables intentionally contain minimal constraints
    - No transformations or business rules applied here
    - Data cleansing belongs in Silver layer
    - Audit table supports operational monitoring

Dependencies:
    - Database : DataWarehouse
    - Schemas  : staging, bronze

Deployment Notes:
    - Safe for repeatable development deployments
    - Existing objects dropped before recreation
    - Intended for environment initialization scripts

Change History
----------------------------------------------------------------------
Version   Date         Author        Description
1.0       2026-04-26   Amr Anter     Initial creation
====================================================================*/



USE DataWarehouse;
GO

IF OBJECT_ID('staging.crm_cust_info','U') IS NOT NULL
DROP TABLE IF EXISTS staging.crm_cust_info;
GO
CREATE TABLE staging.crm_cust_info
        (
                cst_id             int          ,
                cst_key            nvarchar(50)  ,
                cst_firstname      nvarchar(50)  ,
                cst_lastname       nvarchar(50)  ,
                cst_marital_status nvarchar(10)  ,
                cst_gndr           nvarchar(10)  ,
                cst_create_date    nvarchar(50)        
               
        )
;
GO
IF OBJECT_ID('staging.crm_prd_info','U') IS NOT NULL
DROP TABLE IF EXISTS staging.crm_prd_info;
GO
CREATE TABLE staging.crm_prd_info
        (
                prd_id           int          ,
                prd_key          nvarchar(50)  ,
                prd_nm           nvarchar(50)  ,
                prd_cost         decimal(10,2),
                prd_line         nvarchar(20) ,
                prd_start_dt     nvarchar(20)         ,
                prd_end_dt       nvarchar(20)         ,
             
        )
;
GO
IF OBJECT_ID('staging.crm_sales_details','U') IS NOT NULL
DROP TABLE IF EXISTS staging.crm_sales_details;
GO
CREATE TABLE staging.crm_sales_details
        (
                sls_ord_num      nvarchar(50) ,
                sls_prd_key      nvarchar(50) ,
                sls_cust_id      INT          ,
                sls_order_dt     nvarchar(20)         ,
                sls_ship_dt      nvarchar(20)         ,
                sls_due_dt       nvarchar(20)         ,
                sls_sales        decimal(10,2),
                sls_quantity     INT          ,
                sls_price        INT,
            
        )
;
GO
IF OBJECT_ID('staging.erp_cust_aZ12','U') IS NOT NULL
DROP TABLE IF EXISTS staging.erp_cust_aZ12;
GO
CREATE TABLE staging.erp_cust_aZ12
        (
                cid              nvarchar(50) ,
                bdate            nvarchar(20)        ,
                gen              nvarchar(50) ,
               
        )
;
GO
IF OBJECT_ID('staging.erp_loc_a101','U') IS NOT NULL
DROP TABLE IF EXISTS staging.erp_loc_a101;
GO
CREATE TABLE staging.erp_loc_a101
        (
                cid              NVARCHAR(50) ,
                cntry            NVARCHAR(25) ,
                
        )
;
GO
IF OBJECT_ID('staging.erp_px_cat_g1v2','U') IS NOT NULL
DROP TABLE IF EXISTS staging.erp_px_cat_g1v2;
GO
CREATE TABLE staging.erp_px_cat_g1v2
        (
                id               NVARCHAR(50) ,
                cat              NVARCHAR(50) ,
                subcat           NVARCHAR(50) ,
                maintenance      NVARCHAR(25) ,
                
        )
;
GO

IF OBJECT_ID('bronze.load_audit','U') IS NOT NULL
DROP TABLE IF EXISTS bronze.load_audit;
GO
CREATE TABLE bronze.load_audit (
    audit_id INT IDENTITY(1,1) PRIMARY KEY,
    batch_id BIGINT,
    table_name VARCHAR(100),
    load_start_time DATETIME2,
    load_end_time DATETIME2,
    rows_loaded BIGINT,
    load_status VARCHAR(20),
    error_message VARCHAR(4000)
);
