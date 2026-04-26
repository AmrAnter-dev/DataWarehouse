/*====================================================================
Object Name : staging.load_file
Object Type : Stored Procedure
Project     :  DataWarehouse
Layer       : Staging / Bronze Ingestion

Author      : Amr Anter
Created On  : 2026-04-26
Version     : 1.0

Purpose:
    Loads source flat files into staging tables using BULK INSERT,
    performs basic load validation, and records execution metadata
    in the audit framework.

Description:
    This procedure executes a standard raw-ingestion pattern:

      1. Truncates target staging table
      2. Loads source file via BULK INSERT
      3. Validates row counts
      4. Captures load audit metadata
      5. Raises exceptions on failure

Parameters:
    @table_name   Target staging table name
    @file_path    Source file path
    @batch_id     Batch execution identifier
    @num_rows     Output parameter returning rows loaded

Processing Pattern:
    Load Strategy    : Full Refresh (Truncate + Reload)
    Ingestion Method : BULK INSERT
    Validation       : Zero-row load check
    Audit Logging    : bronze.load_audit

Dependencies:
    - Target staging tables
    - Audit table: bronze.load_audit
    - SQL Server BULK INSERT permissions
    - Source files accessible to SQL Server service account

Operational Notes:
    - Intended for raw Bronze ingestion only
    - Assumes source files contain headers (FIRSTROW=2)
    - Assumes UTF-8 encoded CSV sources
    - Business transformations are deferred to Silver layer

Usage Example:
    DECLARE @rows INT;

    EXEC staging.load_file
         @table_name = 'bronze.crm_cust_info',
         @file_path  = 'C:\data\crm_customers.csv',
         @batch_id   = 1001,
         @num_rows   = @rows OUTPUT;

Failure Conditions:
    - Empty source file
    - Invalid file path
    - Bulk insert permission issues
    - Schema/file structure mismatch

Change History
----------------------------------------------------------------------
Version   Date         Author        Description
1.0       2026-04-26   Amr Anter     Initial creation
====================================================================*/

CREATE OR ALTER PROCEDURE staging.load_file
(
    @table_name NVARCHAR(100),
    @file_path  NVARCHAR(300),
    @batch_id   BIGINT,
    @num_rows   INT OUTPUT
)
AS
BEGIN

SET NOCOUNT ON;

DECLARE @sql NVARCHAR(MAX);
DECLARE @start_time DATETIME2;
DECLARE @end_time DATETIME2;

SET @start_time = SYSDATETIME();

BEGIN TRY

    PRINT '>> Truncating: ' + @table_name;

    ---------------------------------------
    -- Truncate Table
    ---------------------------------------
    SET @sql='TRUNCATE TABLE ' + @table_name;
    EXEC sp_executesql @sql;

    ---------------------------------------
    -- Bulk Insert
    ---------------------------------------
    SET @sql='
    BULK INSERT ' + @table_name + '
    FROM ''' + @file_path + '''
    WITH
    (
       FIRSTROW=2,
       FIELDTERMINATOR='','',
       ROWTERMINATOR=''0x0A'',
       CODEPAGE=''65001'',
       TABLOCK
    )';

    EXEC sp_executesql @sql;

    ---------------------------------------
    -- Row Count Validation
    ---------------------------------------
    SET @sql='SELECT @cnt = COUNT(*) FROM ' + @table_name;

    EXEC sp_executesql
         @sql,
         N'@cnt INT OUTPUT',
         @cnt=@num_rows OUTPUT;

    IF @num_rows = 0
      THROW 50001,'Source file loaded zero rows',1;

    SET @end_time=SYSDATETIME();

    ---------------------------------------
    -- Audit Log
    ---------------------------------------
    INSERT INTO bronze.load_audit
    (
      batch_id,
      table_name,
      load_start_time,
      load_end_time,
      rows_loaded,
      load_status
    )
    VALUES
    (
      @batch_id,
      @table_name,
      @start_time,
      @end_time,
      @num_rows,
      'SUCCESS'
    );

END TRY
BEGIN CATCH
   THROW;
END CATCH

END;
GO


CREATE OR ALTER PROCEDURE staging.load_file
(
    @table_name NVARCHAR(100),
    @file_path  NVARCHAR(300),
    @batch_id   BIGINT,
    @num_rows   INT OUTPUT
)
AS
BEGIN

SET NOCOUNT ON;

DECLARE @sql NVARCHAR(MAX);
DECLARE @start_time DATETIME2;
DECLARE @end_time DATETIME2;

SET @start_time = SYSDATETIME();

BEGIN TRY

    PRINT '>> Truncating: ' + @table_name;

    ---------------------------------------
    -- Truncate Table
    ---------------------------------------
    SET @sql='TRUNCATE TABLE ' + @table_name;
    EXEC sp_executesql @sql;

    ---------------------------------------
    -- Bulk Insert
    ---------------------------------------
    SET @sql='
    BULK INSERT ' + @table_name + '
    FROM ''' + @file_path + '''
    WITH
    (
       FIRSTROW=2,
       FIELDTERMINATOR='','',
       ROWTERMINATOR=''0x0A'',
       CODEPAGE=''65001'',
       TABLOCK
    )';

    EXEC sp_executesql @sql;

    ---------------------------------------
    -- Row Count Validation
    ---------------------------------------
    SET @sql='SELECT @cnt = COUNT(*) FROM ' + @table_name;

    EXEC sp_executesql
         @sql,
         N'@cnt INT OUTPUT',
         @cnt=@num_rows OUTPUT;

    IF @num_rows = 0
      THROW 50001,'Source file loaded zero rows',1;

    SET @end_time=SYSDATETIME();

    ---------------------------------------
    -- Audit Log
    ---------------------------------------
    INSERT INTO bronze.load_audit
    (
      batch_id,
      table_name,
      load_start_time,
      load_end_time,
      rows_loaded,
      load_status
    )
    VALUES
    (
      @batch_id,
      @table_name,
      @start_time,
      @end_time,
      @num_rows,
      'SUCCESS'
    );

END TRY
BEGIN CATCH
   THROW;
END CATCH

END;
GO
