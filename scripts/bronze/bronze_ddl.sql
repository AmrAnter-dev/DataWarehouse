/*====================================================================
File Name   : bronze_ddl.sql
Project     :  DataWarehouse
Layer       : Bronze (Raw Ingestion Layer)

Author      : Amr Anter
Created On  : 2026-04-26
Version     : 1.0

Purpose:
    Creates raw source ingestion tables in the Bronze layer for
    CRM and ERP source systems.

Scope:
    - Drops existing objects if they exist
    - Creates source-aligned landing tables
    - Preserves source structure with minimal transformations
    - Adds lineage, audit, and ingestion metadata columns

Source Systems:
    CRM
      - crm_cust_info
      - crm_prd_info
      - crm_sales_details

    ERP
      - erp_cust_az12
      - erp_loc_a101
      - erp_px_cat_g1v2

Design Notes:
    - Bronze layer stores raw ingested data as close to source as possible
    - Data types mirror source extracts where feasible
    - Business transformations belong in Silver layer
    - Audit columns support lineage and batch traceability

Metadata Columns:
    dwh_batch_id
    dwh_record_source
    dwh_load_date
    dwh_ingestion_ts

Dependencies:
    - Database: DataWarehouse
    - Schema  : bronze

Change History
----------------------------------------------------------------------
Version   Date         Author        Description
1.0       2026-04-26   Amr Anter     Initial creation
====================================================================*/

USE DataWarehouse;
GO

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
DROP TABLE IF EXISTS bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info
        (
                cst_id             int          ,
                cst_key            nvarchar(50)  ,
                cst_firstname      nvarchar(50)  ,
                cst_lastname       nvarchar(50)  ,
                cst_marital_status nvarchar(10)  ,
                cst_gndr           nvarchar(10)  ,
                cst_create_date    nvarchar(50),
                -- Lineage / Audit columns
                dwh_batch_id       bigint,
                dwh_record_source  nvarchar(50)  default 'CRM',
                dwh_load_date      date default cast(GETDATE() as date),
                dwh_ingestion_ts   datetime2 default SYSDATETIME()
               
        )
;
GO
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
DROP TABLE IF EXISTS bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info
        (
                prd_id           int          ,
                prd_key          nvarchar(50)  ,
                prd_nm           nvarchar(50)  ,
                prd_cost         decimal(10,2),
                prd_line         nvarchar(20) ,
                prd_start_dt     nvarchar(20)         ,
                prd_end_dt       nvarchar(20)         ,
                -- Lineage / Audit columns
                dwh_batch_id       bigint,
                dwh_record_source  nvarchar(50)  default 'CRM',
                dwh_load_date      date default cast(GETDATE() as date),
                dwh_ingestion_ts   datetime2 default SYSDATETIME()
             
        )
;
GO
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
DROP TABLE IF EXISTS bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details
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
               -- Lineage / Audit columns
                dwh_batch_id       bigint,
                dwh_record_source  nvarchar(50) default 'CRM',
                dwh_load_date      date default cast(GETDATE() as date),
                dwh_ingestion_ts   datetime2 default SYSDATETIME()
            
        )
;
GO
IF OBJECT_ID('bronze.erp_cust_aZ12','U') IS NOT NULL
DROP TABLE IF EXISTS bronze.erp_cust_aZ12;
GO
CREATE TABLE bronze.erp_cust_aZ12
        (
                cid              nvarchar(50) ,
                bdate            nvarchar(20) ,
                gen              nvarchar(50) ,
                -- Lineage / Audit columns
                dwh_batch_id       bigint,
                dwh_record_source  nvarchar(50) default 'ERP' ,
                dwh_load_date      date default cast(GETDATE() as date),
                dwh_ingestion_ts   datetime2 default SYSDATETIME()
               
        )
;
GO
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
DROP TABLE IF EXISTS bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101
        (
                cid              NVARCHAR(50) ,
                cntry            NVARCHAR(25) ,
                -- Lineage / Audit columns
                dwh_batch_id       bigint,
                dwh_record_source  nvarchar(50) default 'ERP' ,
                dwh_load_date      date default cast(GETDATE() as date),
                dwh_ingestion_ts   datetime2 default SYSDATETIME()
        )
;
GO
IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2
        (
                id               NVARCHAR(50) ,
                cat              NVARCHAR(50) ,
                subcat           NVARCHAR(50) ,
                maintenance      NVARCHAR(25) ,
                -- Lineage / Audit columns
                dwh_batch_id       bigint,
                dwh_record_source  nvarchar(50)  default 'ERP',
                dwh_load_date      date default cast(GETDATE() as date),
                dwh_ingestion_ts   datetime2 default SYSDATETIME()
                
        )
;
GO

