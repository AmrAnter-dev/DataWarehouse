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