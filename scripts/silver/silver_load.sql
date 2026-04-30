/*===============================================================================
  File Name   : silver_load.sql
  Project     : Data Warehouse (staging → Bronze → Silver → Gold Architecture)
  Layer       : Silver (Cleansed / Conformed Layer)

  Author      : Amr Anter
  Created On  : 2026-04-30

  Description :
  ------------
  This script defines the **Silver Layer schema** for the Data Warehouse.
  The Silver layer represents cleansed, standardized, and conformed data
  loaded from the Bronze layer.

  Responsibilities of Silver Layer:
  - Data cleansing (NULL handling, trimming, standardization)
  - Data normalization (gender, country, product line, etc.)
  - Business key standardization
  - Deduplication support
  - Data quality tracking
  - Audit & lineage tracking columns

  Tables Created:
  --------------
  1. silver.crm_cust_info
  2. silver.crm_prd_info
  3. silver.crm_sales_details
  4. silver.erp_cust_aZ12
  5. silver.erp_loc_a101
  6. silver.erp_px_cat_g1v2

  Design Notes:
  -------------
  - Each table includes lineage columns:
      * dwh_batch_id
      * dwh_record_source
      * dwh_load_date
      * dwh_ingestion_ts
  - Each table includes data quality columns:
      * dwh_validation_error
      * dwh_hash_key
      * dwh_last_updated_ts
  - Supports both full load and incremental strategies downstream

===============================================================================*/


-- load silver layer
CREATE OR ALTER PROCEDURE silver.load_silver_fresh
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
    DECLARE @current_table VARCHAR(100);
    DECLARE @row_count BIGINT;

    BEGIN TRY

    PRINT '=========================================================';
    PRINT 'silver Load Started';
    PRINT '=========================================================';

    -------------------------------------------------------
    -- CRM TABLES
    -------------------------------------------------------

    PRINT '---------------------------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '---------------------------------------------------------';

        -------------------------------------------------------
        -- silver.crm_cust_info
        -------------------------------------------------------
      
    SET @start_time = SYSDATETIME();
    SET @current_table = 'silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        insert into silver.crm_cust_info(
        customer_id,
        customer_key,
        first_name,
        last_name,
        full_name,
        marital_status,
        gender,
        create_date,
        dwh_batch_id,
        dwh_record_source,
        dwh_load_date,
        dwh_ingestion_ts,
        dwh_validation_error,
        dwh_hash_key)

        SELECT 
        t.cst_id,
        t.cst_key,

        CASE
            WHEN TRIM(t.cst_firstname) = '' OR t.cst_firstname IS NULL THEN 'N/A'
            ELSE TRIM(t.cst_firstname)
            END first_name,
        CASE
            WHEN TRIM(t.cst_lastname) = '' OR t.cst_lastname IS NULL THEN 'N/A'
            ELSE TRIM(t.cst_lastname)
            END last_name,
        CASE
        WHEN TRIM(t.cst_firstname) = '' OR t.cst_firstname IS NULL
          OR TRIM(t.cst_lastname) = '' OR t.cst_lastname IS NULL 
          THEN 'N/A'
          ELSE TRIM(t.cst_firstname)+' '+TRIM(t.cst_lastname)
          END AS full_name,
                
        CASE
            WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'single'
            WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'married'
            ELSE 'N/A'
            END marital_status,-- normalize marital status to readable values

        CASE
            WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'female'
            WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'male'
            ELSE 'N/A'
            END gender, -- normalize gender values to readable formate

        d.cleaned_date AS cst_create_date,
        t.dwh_batch_id,
        t.dwh_record_source,
        t.dwh_load_date,
        t.dwh_ingestion_ts,
        NULLIF(
                CONCAT_WS('|',
                    CASE WHEN d.cleaned_date IS NULL THEN 'INVALID_DATE_FORMAT' END,
                    CASE WHEN t.cst_firstname IS NULL OR TRIM(t.cst_firstname) = '' THEN 'MISSING_FIRSTNAME' END,
                    CASE WHEN t.cst_lastname IS NULL OR TRIM(t.cst_lastname) = '' THEN 'MISSING_LASTNAME' END
                ),
            '') AS dwh_validation_error,

        HASHBYTES(
           'SHA2_256',
                        CONCAT_WS('|',
                                  COALESCE(t.cst_key,''),
                                  COALESCE(TRIM(t.cst_firstname),''),
                                  COALESCE(TRIM(t.cst_lastname),''),
                                  COALESCE(CONVERT(varchar(10), d.cleaned_date, 23),'')
                               )
              ) AS dwh_hash_key


        FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY TRY_CONVERT(date, cst_create_date) DESC
           ) AS rn
    FROM bronze.crm_cust_info
        ) t

        CROSS APPLY (
            SELECT cleaned_date = TRY_CONVERT(date,
                REPLACE(REPLACE(REPLACE(REPLACE(TRIM(t.cst_create_date),
                CHAR(160), ''),
                CHAR(13), ''),
                CHAR(10), ''),
                CHAR(9), '')
            )
        ) d

        WHERE t.rn = 1
          AND t.cst_id IS NOT NULL;
          

    SET @row_count = @@ROWCOUNT;

		IF @row_count = 0
			THROW 50001,'zero row loaded',1;
    SET @end_time = SYSDATETIME();
    PRINT 'Table :' +CAST(@current_table AS NVARCHAR)
    PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';

        -------------------------------------------------------
        -- silver.crm_prd_info
        -------------------------------------------------------
      
    SET @start_time = SYSDATETIME();
    SET @current_table = 'silver.crm_prd_info';

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info(
        product_id,
        category_id,
        product_key,
        product_name,
        product_cost,
        product_line,
        product_start_dt,
        product_end_dt,
        dwh_batch_id,
        dwh_record_source,
        dwh_load_date,
        dwh_ingestion_ts,
        dwh_validation_error,
        dwh_hash_key)
        
        SELECT 
    prd_id AS product_id,

    REPLACE(LEFT(prd_key,5),'-','_') AS category_id,

    SUBSTRING(prd_key,7,LEN(prd_key)) AS product_key,

    TRIM(prd_nm) AS product_name,

    prd_cost AS product_cost,

    CASE UPPER(TRIM(prd_line))
        WHEN 'R' THEN 'ROAD'
        WHEN 'S' THEN 'SPORT'
        WHEN 'M' THEN 'MOUNTAIN'
        WHEN 'T' THEN 'TOURING'
        ELSE 'N/A'
    END AS product_line,

    TRY_CONVERT(date,prd_start_dt) AS product_start_dt,

    DATEADD(
       day,
       -1,
       LEAD(
          TRY_CONVERT(date,prd_start_dt)
       ) OVER (
          PARTITION BY prd_key
          ORDER BY TRY_CONVERT(date,prd_start_dt)
       )
    ) AS product_end_dt,

    dwh_batch_id,
    dwh_record_source,
    dwh_load_date,
    dwh_ingestion_ts,

    CASE
       WHEN TRY_CONVERT(date,prd_start_dt) IS NULL
       THEN 'INVALID_DATE'
    END AS dwh_validation_error,

    HASHBYTES(
       'SHA2_256',
       CONCAT_WS('|',
          COALESCE(REPLACE(LEFT(prd_key,5),'-','_'),''),
          COALESCE(SUBSTRING(prd_key,7,LEN(prd_key)),''),
          COALESCE(TRIM(prd_nm),''),
          COALESCE(CONVERT(varchar(50),prd_cost),'')
       )
    ) AS dwh_hash_key

FROM bronze.crm_prd_info;

SET @row_count = @@ROWCOUNT;

		IF @row_count = 0
			THROW 50001,'zero row loaded',1;
    SET @end_time = SYSDATETIME();
    PRINT 'Table :' +CAST(@current_table AS NVARCHAR)
    PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
            + ' Seconds';

        PRINT '---------------------------------------------------------';

        -------------------------------------------------------
        -- silver.crm_sales_details
        -------------------------------------------------------
      
        SET @start_time = SYSDATETIME();
        SET @current_table = 'silver.crm_sales_details';

            TRUNCATE TABLE silver.crm_sales_details;

            INSERT INTO silver.crm_sales_details(
            order_number            ,
            product_key              ,
            customer_id                       ,
            order_date                  ,
            ship_date                        ,
            due_date                       ,
            total_sales             ,
            quantity                          ,
            unit_price              ,
            -- Lineage / Audit columns
            dwh_batch_id            ,
            dwh_record_source      ,
            dwh_load_date           ,
            dwh_ingestion_ts       ,
                -- data quality / processing columns
           
            dwh_validation_error   ,
            dwh_hash_key)
            
            
            select
            TRIM(sls_ord_num) AS order_number,
            TRIM(sls_prd_key) AS product_key,
            sls_cust_id AS customer_id,
            -- ORDER DATE CLEANING
            CASE
                WHEN TRY_CONVERT(date,sls_order_dt) IS NULL
                  OR sls_order_dt = '0'
                  OR TRY_CONVERT(date,sls_order_dt) > TRY_CONVERT(date,sls_ship_dt)
                  OR TRY_CONVERT(date,sls_order_dt) > TRY_CONVERT(date,sls_due_dt)

                THEN TRY_CONVERT(date,sls_ship_dt)

                ELSE TRY_CONVERT(date,sls_order_dt)
            END AS order_date,
            -- SHIP DATE CLEANING
            CASE
                WHEN TRY_CONVERT(date,sls_ship_dt) IS NULL THEN TRY_CONVERT(date,sls_order_dt)
                WHEN TRY_CONVERT(date,sls_ship_dt) < TRY_CONVERT(date,sls_order_dt)
                THEN TRY_CONVERT(date,sls_order_dt)
                ELSE TRY_CONVERT(date,sls_ship_dt)
                END AS ship_date,
            -- DUE DATE CLEANING
            CASE
            WHEN TRY_CONVERT(date,sls_due_dt) <
                 CASE
                     WHEN TRY_CONVERT(date,sls_ship_dt) >
                          TRY_CONVERT(date,sls_order_dt)
                     THEN TRY_CONVERT(date,sls_ship_dt)
                     ELSE TRY_CONVERT(date,sls_order_dt)
                 END

            THEN
                 CASE
                     WHEN TRY_CONVERT(date,sls_ship_dt) >
                          TRY_CONVERT(date,sls_order_dt)
                     THEN TRY_CONVERT(date,sls_ship_dt)
                     ELSE TRY_CONVERT(date,sls_order_dt)
                 END

            ELSE TRY_CONVERT(date,sls_due_dt)

            END AS due_date,

            -- SALES
               ROUND( 
                CAST(
                    CASE
                        WHEN (sls_sales IS NULL OR sls_sales = 0)
                            AND sls_quantity IS NOT NULL
                            AND sls_price IS NOT NULL
                        THEN ABS(sls_quantity) * ABS(sls_price)
                        WHEN sls_sales < 0 THEN ABS(sls_sales)
                        WHEN sls_sales < ABS(nullif(sls_quantity,0)) * ABS(nullif(sls_price,0)) then 
                            ABS(nullif(sls_quantity,0)) * ABS(nullif(sls_price,0))
                        ELSE sls_sales
                        END AS DECIMAL(10,2))
                        ,2) AS total_sales,
            sls_quantity AS quantity,
            -- PRICE CLEANING
            ROUND(
                CAST(
                    CASE 
                        WHEN sls_price < 0 THEN ABS(sls_price)

                        WHEN sls_price IS NULL OR sls_price = 0
                             AND sls_quantity <> 0
                        THEN sls_sales / NULLIF(sls_quantity,0)

                        ELSE sls_price
                    END
                        AS DECIMAL(10,2))
            ,2) AS unit_price,
            dwh_batch_id            ,
            dwh_record_source      ,
            dwh_load_date           ,
            dwh_ingestion_ts       ,
            --validation error inspection
            NULLIF(
                    CONCAT_WS('|',
                       CASE 
                          WHEN TRY_CONVERT(date,sls_order_dt) IS NULL
                            OR sls_order_dt='0'
                          THEN 'INVALID_ORDER_DATE'
                       END,

                       CASE 
                          WHEN sls_price < 0
                          THEN 'NEGATIVE_PRICE'
                       END,

                       CASE 
                          WHEN TRY_CONVERT(date,sls_order_dt) >
                               TRY_CONVERT(date,sls_ship_dt)
                          THEN 'DATE_SEQUENCE_CORRECTED'
                       END

                    ),'') AS dwh_validation_error,

            --hash_key identifer
            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    TRIM(sls_ord_num),TRIM(sls_prd_key),CONVERT(varchar(50),sls_cust_id)    
                         )
                     )AS dwh_hash_key

            from bronze.crm_sales_details

        SET @row_count = @@ROWCOUNT;

		IF @row_count = 0
			THROW 50001,'zero row loaded',1;
        SET @end_time = SYSDATETIME();
        PRINT 'Table :' +CAST(@current_table AS NVARCHAR)
        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
                + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
                + ' Seconds';

        PRINT '---------------------------------------------------------';

         -------------------------------------------------------
        -- silver.erp_cust_aZ12
        -------------------------------------------------------
      
        SET @start_time = SYSDATETIME();
        SET @current_table = 'silver.erp_cust_aZ12';

            TRUNCATE TABLE silver.erp_cust_aZ12;

            INSERT INTO silver.erp_cust_aZ12(
            customer_id            ,
            birthdate                ,
            gender                    ,
            -- Lineage / Audit columns
            dwh_batch_id            ,
            dwh_record_source         ,
            dwh_load_date           ,
            dwh_ingestion_ts        ,
                -- data quality / processing columns
            dwh_validation_error     ,
            dwh_hash_key )            
               
                        
            select
            -- cleaning customer id

            CASE 
	            WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
	            ELSE cid
	            END AS customer_id,
            -- cleaning birthdate
            CASE
                WHEN TRY_CONVERT(date,
                   bdate)
                     > GETDATE()
                OR TRY_CONVERT(date,
                    bdate) < '1926-01-01'
                THEN NULL

                ELSE TRY_CONVERT(date,
                    bdate)
    
            END AS birthdate,

	            -- cleaning low cardinality gender
            CASE
                WHEN UPPER(TRIM(
                    REPLACE(REPLACE(REPLACE(gen,
                    CHAR(160),''),
                    CHAR(13),''),
                    CHAR(10),'')
                )) IN ('FEMALE','F') THEN 'female'

                WHEN UPPER(TRIM(
                    REPLACE(REPLACE(REPLACE(gen,
                    CHAR(160),''),
                    CHAR(13),''),
                    CHAR(10),'')
                )) IN ('MALE','M') THEN 'male'

                ELSE 'N/A'
            END AS gender,

            dwh_batch_id             ,
            dwh_record_source        ,
            dwh_load_date           ,
            dwh_ingestion_ts ,
            NULLIF(
                CONCAT_WS('|',
                            CASE 
                                WHEN cid like 'NAS%' THEN 'INVALID CUSTOMER ID'
                                END,
                            CASE 
                                WHEN TRY_CONVERT(date,
                                    bdate)
                                        > GETDATE()
                                OR TRY_CONVERT(date,
                                    bdate) < '1926-01-01'
                                THEN  'INVALID BIRTHDATE'
                                END
                                )
                                ,'') AS dwh_validation_error,
            HASHBYTES('SHA2_256',
                CONCAT_WS('|',
                    COALESCE(CASE 
                        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
                        ELSE cid END,''),
                    COALESCE(CONVERT(nvarchar(10),
                        CASE
                            WHEN TRY_CONVERT(date,bdate) > GETDATE()
                              OR TRY_CONVERT(date,bdate) < '1926-01-01'
                            THEN NULL
                            ELSE TRY_CONVERT(date,bdate)
                        END, 23),'')
                )
            ) AS dwh_hash_key

            from bronze.erp_cust_aZ12;

        SET @row_count = @@ROWCOUNT;

		IF @row_count = 0
			THROW 50001,'zero row loaded',1;
        SET @end_time = SYSDATETIME();
        PRINT 'Table :' +CAST(@current_table AS NVARCHAR)
        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
                + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
                + ' Seconds';

        PRINT '---------------------------------------------------------';
          
        -------------------------------------------------------
        -- silver.erp_loc_a101
        -------------------------------------------------------
      
        SET @start_time = SYSDATETIME();
        SET @current_table = 'silver.erp_loc_a101';

            TRUNCATE TABLE silver.erp_loc_a101;
            INSERT INTO silver.erp_loc_a101(
                customer_id,
                country,
                dwh_batch_id,
                dwh_record_source,
                dwh_load_date,
                dwh_ingestion_ts,
                dwh_validation_error,
                dwh_hash_key
            )
            SELECT
                c.cleaned_customer_id,
                c.cleaned_country,
                dwh_batch_id,
                dwh_record_source,
                dwh_load_date,
                dwh_ingestion_ts,

                NULLIF(
                    CONCAT_WS('|',
                        CASE 
                            WHEN c.cleaned_customer_id = '' THEN 'INVALID_CUSTOMER_ID'
                        END,
                        CASE 
                            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'INVALID_COUNTRY_NAME'
                        END
                    ),
                '') AS dwh_validation_error,

                HASHBYTES('SHA2_256',
                    CONCAT_WS('|',
                        COALESCE(c.cleaned_customer_id,''),
                        COALESCE(c.cleaned_country,'')
                    )
                ) AS dwh_hash_key

            FROM bronze.erp_loc_a101

            CROSS APPLY (
                SELECT
                    cleaned_customer_id = REPLACE(TRIM(cid),'-',''),
                    cleaned_country =
                        CASE
                            WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'GERMANY'
                            WHEN UPPER(TRIM(cntry)) IN ('USA','UNITED STATES','US') THEN 'USA'
                            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
                            ELSE UPPER(TRIM(cntry))
                        END
            ) c;

            SET @row_count = @@ROWCOUNT;

		    IF @row_count = 0
			    THROW 50001,'zero row loaded',1;
            SET @end_time = SYSDATETIME();
            PRINT 'Table :' +CAST(@current_table AS NVARCHAR)
            PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
            PRINT '>> Load Duration: '
                    + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
                    + ' Seconds';

            PRINT '---------------------------------------------------------';  
            

        -------------------------------------------------------
        -- silver.erp_px_cat_g1v2
        -------------------------------------------------------
      
        SET @start_time = SYSDATETIME();
        SET @current_table = 'silver.erp_px_cat_g1v2';

            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            INSERT INTO silver.erp_px_cat_g1v2(
            product_id,
            category,
            subcategory,
            maintenance,
            dwh_batch_id,
            dwh_record_source,
            dwh_load_date,
            dwh_ingestion_ts,
            dwh_validation_error,
            dwh_hash_key)

            select 
                TRIM(id) AS product_id,
                TRIM(cat) AS category,
                TRIM(subcat) AS subcategory,
                TRIM(maintenance) AS maintenance,
                dwh_batch_id,
                dwh_record_source,
                dwh_load_date,
                dwh_ingestion_ts,
                NULLIF(
                CONCAT_WS('|',
                    CASE
                        WHEN TRIM(ID)='' OR ID IS NULL
                        THEN 'INAVLID_ID'
                        END,
                    CASE
                        WHEN TRIM(CAT) = '' OR CAT IS NULL
                        THEN ' INVALID_CATEGORY'
                        END,
                    CASE
                        WHEN TRIM(SUBCAT) = '' OR SUBCAT IS NULL
                        THEN 'INVALID_SUBCATEGORY'
                        END,
                    CASE
                        WHEN TRIM(MAINTENANCE) = '' OR MAINTENANCE IS NULL
                        OR MAINTENANCE NOT IN ('YES','NO')
                        THEN 'INVALID_MAINTENANCE'
                        END
                        ),'')AS dwh_validation_error ,
                        
                HASHBYTES('SHA2_256',
                                CONCAT_WS('|',
                                           COALESCE(TRIM(ID),''),
                                           COALESCE(TRIM(CAT),'') ,
                                           COALESCE(TRIM(SUBCAT),'' )
                                            )) AS dwh_hash_key
                                                                    
                FROM bronze.erp_px_cat_g1v2


        SET @row_count = @@ROWCOUNT;

		IF @row_count = 0
			THROW 50001,'zero row loaded',1;
        SET @end_time = SYSDATETIME();
        PRINT 'Table :' +CAST(@current_table AS NVARCHAR)
        PRINT 'Rows Loaded: '+CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: '
                + CAST(DATEDIFF(second,@start_time,@end_time) AS nvarchar)
                + ' Seconds';

        PRINT '---------------------------------------------------------';  
            


        SET @proc_end = SYSDATETIME();

        -------------------------------------------------------
        -- Full Load Timing
        -------------------------------------------------------

        PRINT '=========================================================';
        PRINT 'SILVER Layer Load Completed Successfully';
        PRINT 'Total Load Time: '
            + CAST(DATEDIFF(second,@proc_start,@proc_end) AS nvarchar)
            + ' Seconds';
        PRINT '=========================================================';

    END TRY

    BEGIN CATCH

          PRINT '=========================================================';
       
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT 'Table: ' + ISNULL(@current_table,'Unknown');
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================';

        THROW;

    END CATCH

END;
GO
