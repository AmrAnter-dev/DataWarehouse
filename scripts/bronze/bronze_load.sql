/*
===============================================================================
Procedure Name : bronze.load_bronze
Purpose        : Full refresh load of Bronze layer source tables
Load Type      : Idempotent Full Reload
Author         : Amr Anter
Created Date   : 2026-04-24

Behavior:
- Truncates and reloads Bronze tables
- Logs audit metrics
- Validates row counts
- Raises errors for orchestration tools
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
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
        PRINT 'Bronze Load Started';
        PRINT 'Batch ID: ' + CAST(@batch_id AS NVARCHAR);
        PRINT '=========================================================';

        -------------------------------------------------------
        -- CRM TABLES
        -------------------------------------------------------

        PRINT '---------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------------------------';

        -------------------------------------------------------
        -- bronze.crm_cust_info
        -------------------------------------------------------
       SET @current_table='bronze.crm_cust_info';
       SET @start_time = SYSDATETIME();

       

       PRINT '>> Truncating: bronze.crm_cust_info';
       TRUNCATE TABLE bronze.crm_cust_info;

       

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );

        SELECT @row_count = COUNT(*)
        FROM bronze.crm_cust_info;
        SET @end_time = SYSDATETIME();

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
             @current_table,
             @start_time,
             @end_time,
             @row_count,
             'SUCCESS'
            );

        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- bronze.crm_prd_info
        -------------------------------------------------------
        SET @current_table='bronze.crm_prd_info';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'Loading ' + @current_table;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count=COUNT(*)
        FROM bronze.crm_prd_info;

        SET @end_time = SYSDATETIME();
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
             @current_table,
             @start_time,
             @end_time,
             @row_count,
             'SUCCESS'
            );
        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- bronze.crm_sales_details
        -------------------------------------------------------
        SET @current_table='bronze.crm_sales_details';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Loading: bronze.crm_sales_details';

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM bronze.crm_sales_details;
        SET @end_time = SYSDATETIME();
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
             @current_table,
             @start_time,
             @end_time,
             @row_count,
             'SUCCESS'
            );
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
        -- bronze.erp_cust_az12
        -------------------------------------------------------
        SET @current_table='bronze.erp_cust_aZ12';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: bronze.erp_cust_aZ12';
        TRUNCATE TABLE bronze.erp_cust_aZ12;

        PRINT '>> Loading: bronze.erp_cust_aZ12';

        BULK INSERT bronze.erp_cust_aZ12
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM bronze.erp_cust_aZ12;
        SET @end_time = SYSDATETIME();
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
             @current_table,
             @start_time,
             @end_time,
             @row_count,
             'SUCCESS'
            );
        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- bronze.erp_loc_a101
        -------------------------------------------------------
        SET @current_table='bronze.erp_loc_a101';
        SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Loading: bronze.erp_loc_a101';

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM bronze.erp_loc_a101;
        SET @end_time = SYSDATETIME();
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
             @current_table,
             @start_time,
             @end_time,
             @row_count,
             'SUCCESS'
            );
        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';


        -------------------------------------------------------
        -- bronze.erp_px_cat_g1v2
        -------------------------------------------------------
       SET @current_table='bronze.erp_px_cat_g1v2';
       SET @start_time = SYSDATETIME();

        PRINT '>> Truncating: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Loading: bronze.erp_px_cat_g1v2';

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK
        );
        SELECT @row_count = COUNT(*)
        FROM bronze.erp_px_cat_g1v2;
        SET @end_time = SYSDATETIME();
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
             @current_table,
             @start_time,
             @end_time,
             @row_count,
             'SUCCESS'
            );
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
        PRINT 'Bronze Layer Load Completed Successfully';
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
         load_status,
         error_message
        )
        VALUES
        (
         @batch_id,
         @current_table,
         'FAILED',
         ERROR_MESSAGE()
        );


        PRINT '=========================================================';
       
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'Table: ' + ISNULL(@current_table,'Unknown');
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================';

        THROW;

    END CATCH

END;
GO