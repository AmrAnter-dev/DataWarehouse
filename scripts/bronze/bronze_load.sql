/*
===============================================================================
Procedure Name : bronze.load_bronze
Purpose        : batch append ingestion from staging layer
Load Type      : full append 
Author         : Amr Anter
Created Date   : 2026-04-24

Behavior:
- Appends source records into bronze history tables
- Captures batch lineage metadata
- Logs audit metrics
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
        PRINT 'bronze Load Started';
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

       insert into bronze.crm_cust_info(
           cst_id,
           cst_key,
           cst_firstname,
           cst_lastname,
           cst_marital_status,
           cst_gndr,
           cst_create_date,
           dwh_batch_id 
           
       )
       select
           cst_id,
           cst_key,
           cst_firstname,
           cst_lastname,
           cst_marital_status,
           cst_gndr,
           cst_create_date,
           @batch_id
         
        from staging.crm_cust_info
            
        SET @row_count = @@ROWCOUNT;

		IF @row_count = 0
			THROW 50001,'zero row loaded',1;
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
        -- bronze.crm_prd_info
        -------------------------------------------------------
        SET @current_table='bronze.crm_prd_info';
        SET @start_time = SYSDATETIME();

        

        PRINT 'Loading ' + @current_table;

        insert into bronze.crm_prd_info(
           prd_id,
           prd_key,
           prd_nm,
           prd_cost,
           prd_line,
           prd_start_dt,
           prd_end_dt,
           dwh_batch_id 
           
       )
       select
           prd_id,
           prd_key,
           prd_nm,
           prd_cost,
           prd_line,
           prd_start_dt,
           prd_end_dt,
           @batch_id
           
        from staging.crm_prd_info

        
        SET @row_count = @@ROWCOUNT;

        IF @row_count = 0
			THROW 50001,'zero row loaded',1;

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
        -- bronze.crm_sales_details
        -------------------------------------------------------
        SET @current_table='bronze.crm_sales_details';
        SET @start_time = SYSDATETIME();

      

        PRINT '>> Loading: bronze.crm_sales_details';

        insert into bronze.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price,
            dwh_batch_id
            
        )
        select
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price,
            @batch_id
            
        from staging.crm_sales_details;

       
        SET @row_count = @@ROWCOUNT;

        IF @row_count = 0
			THROW 50001,'zero row loaded',1;

        
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
        -- bronze.erp_cust_az12
        -------------------------------------------------------
        SET @current_table='bronze.erp_cust_aZ12';
        SET @start_time = SYSDATETIME();

        

        PRINT '>> Loading: staging.erp_cust_aZ12';

        INSERT into  bronze.erp_cust_aZ12
        (
        cid,
        bdate,
        gen,
        dwh_batch_id
       
        )
        select
        cid,
        bdate,
        gen,
        @batch_id
        
        from staging.erp_cust_aZ12;


        SET @row_count=@@ROWCOUNT;

        IF @row_count = 0
			THROW 50001,'zero row loaded',1;

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
        -- bronze.erp_loc_a101
        -------------------------------------------------------
        SET @current_table='bronze.erp_loc_a101';
        SET @start_time = SYSDATETIME();

        

        PRINT '>> Loading: staging.erp_loc_a101';

        INSERT into bronze.erp_loc_a101
        (
            cid,
            cntry,
            dwh_batch_id
         
        )
        select
            cid,
            cntry,
            @batch_id
          
        from staging.erp_loc_a101;

       
        SET @row_count=@@ROWCOUNT;

        IF @row_count = 0
			THROW 50001,'zero row loadeds',1;

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
        -- bronze.erp_px_cat_g1v2
        -------------------------------------------------------
       SET @current_table='bronze.erp_px_cat_g1v2';
       SET @start_time = SYSDATETIME();

        

        PRINT '>> Loading: bronze.erp_px_cat_g1v2';

        insert into bronze.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance,
            dwh_batch_id
          
         )
         select
            id,
            cat,
            subcat,
            maintenance,
            @batch_id
          
          from staging.erp_px_cat_g1v2;


        SET @row_count = @@ROWCOUNT;

        IF @row_count = 0
			THROW 50001,'zero row loaded ',1;

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
        PRINT 'BRONZE Layer Load Completed Successfully';
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