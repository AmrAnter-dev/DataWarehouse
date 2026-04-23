/*
===============================================================================
Script Name     : init_database.sql
Project         : Data Warehouse Project
Author          : Amr Anter
Created Date    : 2026-04-23
SQL Server      : Microsoft SQL Server

Purpose:
    - Creates the DataWarehouse database.
    - Drops and recreates the database if it already exists.
    - Initializes Medallion architecture schemas:
        * bronze  -> raw/staging layer
        * silver  -> cleansed/conformed layer
        * gold    -> business-ready dimensional layer

Warning:
    - This script is DESTRUCTIVE.
    - If the database 'DataWarehouse' already exists, it will be dropped
      and recreated.
    - All existing objects, data, permissions, and configurations in the
      database will be permanently lost.

Safety Notes:
    - Intended for development, sandbox, or initial environment setup.
    - Do NOT run in production without approval and backup verification.
    - Review the database name before execution.
    - SINGLE_USER WITH ROLLBACK IMMEDIATE will terminate active connections.

Execution Order:
    1. Run this script first before loading objects or data.
    2. Follow with schema objects, tables, ETL pipelines, and seed data scripts.

===============================================================================
*/
USE master;
GO
-- Drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO
-- create the DataWarehouse database
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO
-- create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
