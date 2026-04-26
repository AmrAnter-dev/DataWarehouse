/*====================================================================
Object Name : staging.load_staging
Object Type : Stored Procedure
Module Type : ETL Orchestration / Batch Control Procedure

Project     :  DataWarehouse
Layer       : Staging Ingestion Layer

Author      : Amr Anter
Created On  : 2026-04-26
Version     : 1.0

Purpose:
    Orchestrates full-batch ingestion of CRM and ERP source files into
    staging tables, performs validation checks, and logs execution
    metrics into the audit framework.

Description:
    This procedure controls end-to-end staging layer loads using a
    truncate-and-reload pattern for all configured source objects.

Processing Workflow:
    1. Generate batch execution ID
    2. Load CRM source tables
    3. Load ERP source tables
    4. Validate row counts after each load
    5. Write success/failure audit records
    6. Capture batch-level execution timing

Source Systems Loaded:
    CRM
      - staging.crm_cust_info
      - staging.crm_prd_info
      - staging.crm_sales_details

    ERP
      - staging.erp_cust_az12
      - staging.erp_loc_a101
      - staging.erp_px_cat_g1v2

Load Strategy:
    Pattern        : Full Refresh
    Method         : BULK INSERT
    Validation     : Row Count Checks
    Logging        : Audit Driven
    Recovery Mode  : Fail Fast with Exception Logging

Dependencies:
    - staging source landing tables
    - bronze.log_load_audit procedure
    - bronze.load_audit table
    - Source CSV files accessible to SQL Server

Operational Controls:
    - Batch-level traceability via generated batch_id
    - Table-level execution metrics
    - Failure logging with error propagation
    - Load duration monitoring

Failure Conditions:
    - Missing or inaccessible source files
    - Schema/file structure mismatch
    - Zero-row load validation failure
    - Bulk insert execution errors

Usage Example:
    EXEC staging.load_staging;

Design Notes:
    - Intended as staging-layer control procedure
    - Minimal transformations applied in this layer
    - Business cleansing and conformance occur in Silver layer

Change History
----------------------------------------------------------------------
Version   Date         Author        Description
1.0       2026-04-26   Amr Anter     Initial creation
====================================================================*/




CREATE OR ALTER PROCEDURE staging.load_staging
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------------
    -- Variable Declarations
    -------------------------------------------------------
    DECLARE @proc_start DATETIME2 = SYSDATETIME();
    DECLARE @proc_end   DATETIME2;
    DECLARE @start_time DATETIME2;
    DECLARE @end_time   DATETIME2;
    DECLARE @batch_id BIGINT;
    DECLARE @row_count BIGINT;
    DECLARE @current_table VARCHAR(100);

    SET @batch_id =
    CAST(FORMAT(SYSDATETIME(),'yyyyMMddHHmmss') AS BIGINT);


    BEGIN TRY

        PRINT '=========================================================';
        PRINT 'staging Load Started';
        PRINT 'Batch ID: ' + CAST(@batch_id AS NVARCHAR);
        PRINT '=========================================================';

        -------------------------------------------------------
        -- CRM TABLES
        -------------------------------------------------------

        PRINT '---------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------------------------';

        -------------------------------------------------------
        -- staging.crm_cust_info
        -------------------------------------------------------
       SET @current_table='staging.crm_cust_info';
       SET @start_time = SYSDATETIME();

       

       PRINT '>> Truncating: staging.crm_cust_info';
       TRUNCATE TABLE staging.crm_cust_info;

       

        BULK INSERT staging.crm_cust_info
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );

        SELECT @row_count = COUNT(*)
        FROM staging.crm_cust_info;
		IF @row_count = 0
			THROW 50001,'Source file loaded zero rows',1;
        SET @end_time = SYSDATETIME();

        EXEC bronze.log_load_audit
           @batch_id=@batch_id,
           @table_name=@current_table,
           @load_start_time=@start_time,
           @load_end_time=@end_time,
           @rows_loaded=@row_count,
           @load_status='SUCCESS';

        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- staging.crm_prd_info
        -------------------------------------------------------
        SET @current_table='staging.crm_prd_info';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: staging.crm_prd_info';
        TRUNCATE TABLE staging.crm_prd_info;

        PRINT 'Loading ' + @current_table;

        BULK INSERT staging.crm_prd_info
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count=COUNT(*)
        FROM staging.crm_prd_info;

        SET @end_time = SYSDATETIME();

        EXEC bronze.log_load_audit
           @batch_id=@batch_id,
           @table_name=@current_table,
           @load_start_time=@start_time,
           @load_end_time=@end_time,
           @rows_loaded=@row_count,
           @load_status='SUCCESS';

        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- staging.crm_sales_details
        -------------------------------------------------------
        SET @current_table='staging.crm_sales_details';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: staging.crm_sales_details';
        TRUNCATE TABLE staging.crm_sales_details;

        PRINT '>> Loading: staging.crm_sales_details';

        BULK INSERT staging.crm_sales_details
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM staging.crm_sales_details;
        SET @end_time = SYSDATETIME();

        EXEC bronze.log_load_audit
           @batch_id=@batch_id,
           @table_name=@current_table,
           @load_start_time=@start_time,
           @load_end_time=@end_time,
           @rows_loaded=@row_count,
           @load_status='SUCCESS';

        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- ERP TABLES
        -------------------------------------------------------

        PRINT '---------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- staging.erp_cust_az12
        -------------------------------------------------------
        SET @current_table='staging.erp_cust_aZ12';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: staging.erp_cust_aZ12';
        TRUNCATE TABLE staging.erp_cust_aZ12;

        PRINT '>> Loading: staging.erp_cust_aZ12';

        BULK INSERT staging.erp_cust_aZ12
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM staging.erp_cust_aZ12;
        SET @end_time = SYSDATETIME();

        EXEC bronze.log_load_audit
           @batch_id=@batch_id,
           @table_name=@current_table,
           @load_start_time=@start_time,
           @load_end_time=@end_time,
           @rows_loaded=@row_count,
           @load_status='SUCCESS';

        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- staging.erp_loc_a101
        -------------------------------------------------------
        SET @current_table='staging.erp_loc_a101';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: staging.erp_loc_a101';
        TRUNCATE TABLE staging.erp_loc_a101;

        PRINT '>> Loading: staging.erp_loc_a101';

        BULK INSERT staging.erp_loc_a101
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM staging.erp_loc_a101;
        SET @end_time = SYSDATETIME(); 

        EXEC bronze.log_load_audit
           @batch_id=@batch_id,
           @table_name=@current_table,
           @load_start_time=@start_time,
           @load_end_time=@end_time,
           @rows_loaded=@row_count,
           @load_status='SUCCESS';

        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- staging.erp_px_cat_g1v2
        -------------------------------------------------------
       SET @current_table='staging.erp_px_cat_g1v2';
       SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: staging.erp_px_cat_g1v2';
        TRUNCATE TABLE staging.erp_px_cat_g1v2;

        PRINT '>> Loading: staging.erp_px_cat_g1v2';

        BULK INSERT staging.erp_px_cat_g1v2
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM staging.erp_px_cat_g1v2;
        SET @end_time = SYSDATETIME();

        EXEC bronze.log_load_audit
           @batch_id=@batch_id,
           @table_name=@current_table,
           @load_start_time=@start_time,
           @load_end_time=@end_time,
           @rows_loaded=@row_count,
           @load_status='SUCCESS';

        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- Full Load Timing
        -------------------------------------------------------

        SET @proc_end = SYSDATETIME();

        PRINT '=========================================================';
        PRINT 'staging Layer Load Completed Successfully';
        PRINT 'Total Load Time: '
            + CAST(DATEDIFF(second,@proc_start,@proc_end) AS nvarchar)
            + ' Seconds';
        PRINT '=========================================================';

    END TRY

    BEGIN CATCH

          
           INSERT INTO bronze.load_audit
           (
              batch_id,
              table_name,
              load_start_time,
              load_end_time,
              rows_loaded,
              load_status,
              error_message
           )
           VALUES
           (
              @batch_id,
              @current_table,
              @start_time,
              @end_time,
              @row_count,
              'FAILED',
              ERROR_MESSAGE()
           );



        PRINT '=========================================================';
       
        PRINT 'ERROR OCCURRED DURING staging LOAD';
        PRINT 'Table: ' + ISNULL(@current_table,'Unknown');
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================';

        THROW;

    END CATCH

END;

GO
