
select

-- cleaning business key 
REPLACE(
		TRIM(cid)
                    ,'-','') AS cid,

CASE 
    WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'GERMANY'
    
    WHEN UPPER(TRIM(cntry)) IN ('USA','UNITED STATES','US') THEN 'USA'
    
    WHEN cntry IS NULL 
         OR TRIM(cntry) = '' THEN 'N/A'
         
    ELSE UPPER(TRIM(cntry))
    
END AS cntry

FROM bronze.erp_loc_a101;


select * from bronze.erp_loc_a101 where cntry IS NULL 



--inspecting duplicate and null in business key
select * from bronze.erp_loc_a101
where cid IS NULL;

select cid,
count(*) as duplicate
from bronze.erp_loc_a101
group by cid
having count(*) >1;

select cid,cntry,count(*) as duplicate
from bronze.erp_loc_a101
group by cid,cntry
having count(*) >1;

select * from bronze.erp_loc_a101
where cid != TRIM(cid);

-- checking low cardinality column
select distinct cntry from bronze.erp_loc_a101;