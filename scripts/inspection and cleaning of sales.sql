select * from bronze.crm_sales_details;



select
TRIM(sls_ord_num) AS sls_ord_num,
TRIM(sls_prd_key) AS sls_prd_key,
sls_cust_id,
-- ORDER DATE CLEANING
CASE
    WHEN TRY_CONVERT(date,sls_order_dt) IS NULL
      OR sls_order_dt = '0'
      OR TRY_CONVERT(date,sls_order_dt) > TRY_CONVERT(date,sls_ship_dt)
      OR TRY_CONVERT(date,sls_order_dt) > TRY_CONVERT(date,sls_due_dt)

    THEN TRY_CONVERT(date,sls_ship_dt)

    ELSE TRY_CONVERT(date,sls_order_dt)
END AS sls_order_dt,
-- SHIP DATE CLEANING
CASE
    WHEN TRY_CONVERT(date,sls_ship_dt) IS NULL THEN TRY_CONVERT(date,sls_order_dt)
    WHEN TRY_CONVERT(date,sls_ship_dt) < TRY_CONVERT(date,sls_order_dt)
    THEN TRY_CONVERT(date,sls_order_dt)
    ELSE TRY_CONVERT(date,sls_ship_dt)
    END AS sls_ship_dt,
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

END AS sls_due_dt,
-- SALES
ROUND((sls_quantity * ABS(ISNULL(sls_price,0))),2) AS sls_sales,
sls_quantity,
-- PRICE CLEANING
ROUND(
    CAST(
        CASE 
            WHEN sls_price < 0 THEN ABS(sls_price)

            WHEN (sls_price IS NULL OR sls_price = 0)
                 AND sls_quantity <> 0
            THEN sls_sales / NULLIF(sls_quantity,0)

            ELSE sls_price
        END
            AS DECIMAL(10,2))
,2) AS sls_price
from bronze.crm_sales_details;



--CHECK DEDUP OR NULL IN BUSSINESS KEY
select sls_ord_num,
count(*) as duplicate_count
from bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1 and sls_ord_num IS NULL;


--CHECK FOR UNWANTED SPACES
select sls_prd_key
from bronze.crm_sales_details
where sls_prd_key != TRIM(sls_prd_key)
	OR TRIM(sls_prd_key)=''
	OR sls_prd_key IS NULL;


--CHECK FOR NEGATIVE VALUE
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id <= 0
OR sls_cust_id IS NULL;

--CHECK FOR INVALID DATES
SELECT *
FROM bronze.crm_sales_details
WHERE TRY_CONVERT(DATE,sls_ship_dt) < TRY_CONVERT(DATE,sls_order_dt)
	OR TRY_CONVERT(DATE,sls_due_dt) < TRY_CONVERT(DATE,sls_order_dt)
	OR TRY_CONVERT(DATE,sls_ship_dt) IS NULL
	OR TRY_CONVERT(DATE,sls_order_dt) IS NULL
	OR TRY_CONVERT(DATE,sls_due_dt) IS NULL;

SELECT *
FROM bronze.crm_sales_details
WHERE TRY_CONVERT(DATE,sls_due_dt) < TRY_CONVERT(DATE,sls_order_dt)
	OR TRY_CONVERT(DATE,sls_due_dt) < TRY_CONVERT(DATE,sls_ship_dt)
	OR TRY_CONVERT(DATE,sls_ship_dt) IS NULL
	
	OR TRY_CONVERT(DATE,sls_due_dt) IS NULL;

--CKECK NEGATIVE OR UNRELIABLE PRICES
SELECT sls_price
FROM bronze.crm_sales_details
WHERE sls_price <=0
OR sls_price IS NULL;


SELECT sls_quantity
FROM bronze.crm_sales_details
WHERE sls_quantity <=0
OR sls_quantity IS NULL;

SELECT sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales <= 0
OR sls_sales IS NULL
OR sls_sales <> (sls_quantity * sls_price) ;



