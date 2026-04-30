/*==============================================================================
  PROJECT NAME      : Data Warehouse – Silver Layer Schema
  FILE NAME         : silver_ddl.sql

  DESCRIPTION:
      This script creates the Silver Layer tables for the Data Warehouse.
      The Silver layer represents cleaned, standardized, and conformed data
      coming from Bronze sources (CRM & ERP systems).

      It includes:
        - Customer, Product, Sales, and Reference data tables
        - Data cleansing ready schema design
        - Standardized data types and business keys
        - Audit & lineage columns for traceability
        - Data quality & change tracking support (hashing & validation)

  ARCHITECTURE LAYER:
      staging → Raw ingestion layer
      Bronze  → Raw ingestion layer with technical columns
      Silver  → Cleaned & standardized layer
      Gold    → Business-ready aggregated layer

  DATA SOURCES:
      - CRM System (Customer, Product, Sales)
      - ERP System (Customer Attributes, Location, Product Categories)

  DESIGN PRINCIPLES:
      - Schema-on-write transformation
      - Slowly Changing Data (SCD-ready design)
      - Data lineage and audit tracking
      - Data quality monitoring enabled
      - Consistent naming conventions across entities

  AUTHOR            : Amr Anter
  CREATED DATE      : 2026-04-30
  VERSION           : 1.0

==============================================================================*/

USE DataWarehouse;
GO

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
DROP TABLE IF EXISTS silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info
        (
                customer_id             INT          ,
                customer_key            NVARCHAR(50)  ,
                first_name              NVARCHAR(50)  ,
                last_name               NVARCHAR(50)  ,
                full_name               NVARCHAR(100),
                marital_status          NVARCHAR(50)  ,
                gender                  NVARCHAR(50)  ,
               
               
                create_date             DATE,
                -- Lineage / Audit columns
                dwh_batch_id            BIGINT,
                dwh_record_source       NVARCHAR(50)  DEFAULT 'CRM',
                dwh_load_date           DATE DEFAULT CAST(GETDATE() AS DATE),
                dwh_ingestion_ts        DATETIME2 DEFAULT SYSDATETIME(),
                -- data quality / processing columns
                dwh_last_updated_ts     DATETIME2 DEFAULT SYSDATETIME(),
                dwh_validation_error    NVARCHAR(100),
                dwh_hash_key            VARBINARY(32)

               
        )
;
GO
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
DROP TABLE IF EXISTS silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info
        (
                product_id              INT          ,
                category_id             nvarchar(20),
                product_key             nvarchar(50)  ,
                product_name            nvarchar(50)  ,
                product_cost            decimal(10,2),
                product_line            nvarchar(20) ,
                product_start_dt        date  ,
                product_end_dt          date         ,
                -- Lineage / Audit columns
                dwh_batch_id            BIGINT,
                dwh_record_source       NVARCHAR(50)  DEFAULT 'CRM',
                dwh_load_date           DATE DEFAULT CAST(GETDATE() AS DATE),
                dwh_ingestion_ts        DATETIME2 DEFAULT SYSDATETIME(),
                 -- data quality / processing columns
                dwh_last_updated_ts     DATETIME2 DEFAULT SYSDATETIME(),
                dwh_validation_error    NVARCHAR(100),
                dwh_hash_key            VARBINARY(32)
             
        )
;
GO
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
DROP TABLE IF EXISTS silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details
        (
                order_number            nvarchar(50) ,
                product_key             nvarchar(50) ,
                customer_id             INT          ,
                order_date              date     ,
                ship_date               date         ,
                due_date                date        ,
                total_sales             decimal(10,2),
                quantity                INT          ,
                unit_price              decimal(10,2),
               -- Lineage / Audit columns
                dwh_batch_id            BIGINT,
                dwh_record_source       NVARCHAR(50)  DEFAULT 'CRM',
                dwh_load_date           DATE DEFAULT cast(GETDATE() AS DATE),
                dwh_ingestion_ts        DATETIME2 DEFAULT SYSDATETIME(),
                 -- data quality / processing columns
                dwh_last_updated_ts     DATETIME2 DEFAULT SYSDATETIME(),
                dwh_validation_error    NVARCHAR(100),
                dwh_hash_key            VARBINARY(32)
            
        )
;
GO
IF OBJECT_ID('silver.erp_cust_aZ12','U') IS NOT NULL
DROP TABLE IF EXISTS silver.erp_cust_aZ12;
GO
CREATE TABLE silver.erp_cust_aZ12
        (
                customer_id              nvarchar(50) ,
                birthdate                date ,
                gender                   nvarchar(50) ,
                -- Lineage / Audit columns
                dwh_batch_id            BIGINT,
                dwh_record_source       NVARCHAR(50)  DEFAULT 'ERP',
                dwh_load_date           DATE DEFAULT cast(GETDATE() AS DATE),
                dwh_ingestion_ts        DATETIME2 DEFAULT SYSDATETIME(),
                 -- data quality / processing columns
                dwh_last_updated_ts     DATETIME2 DEFAULT SYSDATETIME(),
                dwh_validation_error    NVARCHAR(100),
                dwh_hash_key            VARBINARY(32)
               
        )
;
GO
IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
DROP TABLE IF EXISTS silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101
        (
                customer_id             NVARCHAR(50) ,
                country                 NVARCHAR(25) ,
                -- Lineage / Audit columns
                dwh_batch_id            BIGINT,
                dwh_record_source       NVARCHAR(50)  DEFAULT 'ERP',
                dwh_load_date           DATE DEFAULT cast(GETDATE() AS DATE),
                dwh_ingestion_ts        DATETIME2 DEFAULT SYSDATETIME(),
                 -- data quality / processing columns
                dwh_last_updated_ts     DATETIME2 DEFAULT SYSDATETIME(),
                dwh_validation_error    NVARCHAR(100),
                dwh_hash_key            VARBINARY(32)
        )
;
GO
IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2
        (
                product_id              NVARCHAR(50) ,
                category                NVARCHAR(50) ,
                subcategory             NVARCHAR(50) ,
                maintenance             NVARCHAR(25) ,
                -- Lineage / Audit columns
                dwh_batch_id            BIGINT,
                dwh_record_source       NVARCHAR(50)  DEFAULT 'ERP',
                dwh_load_date           DATE DEFAULT cast(GETDATE() AS DATE),
                dwh_ingestion_ts        DATETIME2 DEFAULT SYSDATETIME(),
                 -- data quality / processing columns
                dwh_last_updated_ts     DATETIME2 DEFAULT SYSDATETIME(),
                dwh_validation_error    NVARCHAR(100),
                dwh_hash_key            VARBINARY(32)
                
        )
;
GO

