create or alter PROCEDURE silver.load_silver as
begin
    --transform and load silver.crm_cust_info----------------
    declare @batch_start datetime ,@batch_end datetime;
    set @batch_start=getdate();

    begin try
        declare @start_time datetime,@end_time datetime;

        PRINT '>>TRUNCATING Table silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info ;
        PRINT '>>LOADING silver.crm_cust_info';

        set @start_time=GETDATE();
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,                                   --trimming white spaces for data consistency
        trim(cst_lastname) as cst_lastname,

        case when UPPER(cst_marital_status)='S' then'Single'                    --data normalization & standardisation to a more readable format for especially columns with low cardinality
            when UPPER(cst_marital_status)='M' then 'Married'
            else 'n/a'                                                          --handelling missing values
        end cst_marital_status,

        case when UPPER(cst_gndr)='F' THEN 'Female'
            when UPPER(cst_gndr)='M' Then 'Male'
            else 'n/a'
        end cst_gndr,

        cst_create_date
        from(
            select * from (
        SELECT *,ROW_NUMBER() 
        OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last  --removing duplicates
        FROM bronze.crm_cust_info)t 
        where flag_last=1
        )
        final;
        set @end_time=GETDATE();
        PRINT '>>TIME TAKEN TO LOAD CUST_INFO:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';

        ---------------load crm_prd_info------------------------------------------------------------
        PRINT '>>TRUNCATING Table silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info ;
        PRINT '>>LOADING silver.crm_prd_info';
        set @start_time=GETDATE();
        INSERT INTO silver.crm_prd_info(
            prd_id       ,
            cat_id       ,
            prd_key      ,
            prd_nm       ,
            prd_cost     ,
            prd_line     ,
            prd_start_dt ,
            prd_end_dt  
        )
        SELECT 
        prd_id,
        replace(substring(prd_key,1,5),'-','_') as cat_id,
        substring(prd_key,7,LEN(prd_key)) as prd_key,
        prd_nm,
        isnull(prd_cost,0) as prd_cost,
        case upper(trim(prd_line))
            when 'M' THEN 'Mountain'
            when 'R' THEN 'Road'
            when 'S' THEN 'OTHER SALES'
            when 'T' THEN 'TOURISM'
            ELSE 'n/a'
        end as prd_line,
        prd_start_dt,
        DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        )) AS prd_end_dt --so that end date is prior to the next start date for the same product

        from bronze.prd_info;
        set @end_time=GETDATE();
        PRINT '>>TIME TAKEN TO LOAD prd_info:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';

        -------------------------load crm_sales_details ---------------------------------------------------------
        PRINT '>>TRUNCATING Table silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details ;
        PRINT '>>LOADING silver.crm_sales_details';
        set @start_time=GETDATE();
        INSERT INTO silver.crm_sales_details(
            sls_ord_num  ,
            sls_prd_key  ,
            sls_cust_id  ,
            sls_order_dt ,
            sls_ship_dt  ,
            sls_due_dt   ,
            sls_sales    ,
            sls_quantity ,
            sls_price    
        )
        SELECT 
            sls_ord_num
            ,sls_prd_key
            ,sls_cust_id
            ,case 
                when sls_order_dt=0 or LEN(sls_order_dt)!=8 then null
                else cast(cast(sls_order_dt as varchar)as Date) 
            end  as sls_order_dt

            ,case 
                when sls_ship_dt=0 or LEN(sls_ship_dt)!=8 then null
                else cast(cast(sls_ship_dt as varchar)as Date) 
            end  as sls_ship_dt
            ,case 
                when sls_due_dt=0 or LEN(sls_due_dt)!=8 then null
                else cast(cast(sls_due_dt as varchar)as Date) 
            end  as sls_due_dt
            ,case 
                when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*abs(sls_price)
                then sls_quantity*abs(sls_price)
                else sls_sales
            end as sls_sales,
            sls_quantity,
            case 
                when sls_price is null or sls_price<=0 then sls_sales/nullif(sls_quantity,0)
                else sls_price
            end as sls_price
            FROM bronze.sales_details;
            set @end_time=GETDATE();
            PRINT '>>TIME TAKEN TO LOAD sales_details:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';

        ---------------------------------------------------------------------------------
        PRINT '>>TRUNCATING Table silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12 ;
        PRINT '>>LOADING silver.crm_cust_info';
        INSERT INTO silver.erp_cust_az12
        (cid,bdate,gen)
        select 
        CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
            ELSE CID
        END AS CID,
        case when BDATE>GETDATE() THEN NULL
        ELSE BDATE
        END AS BDATE,
        CASE when UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
        when UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
        else 'n/a'
        End as GEN
        FROM bronze.cust_az12;
        set @end_time=GETDATE();
        PRINT '>>TIME TAKEN TO LOAD cust_az12:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';


        -------------------------------------------------------------------------------
        PRINT '>>TRUNCATING Table silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101 ;
        PRINT '>>LOADING silver.erp_loc_a101';
        set @start_time=GETDATE();
        INSERT INTO silver.erp_loc_a101(cid,cntry)
        select 
        SUBSTRING(CID,1,2)+SUBSTRING(CID,4,LEN(CID)) as CID,
        CASE WHEN LEN(TRIM(SUBSTRING(CNTRY,1,LEN(CNTRY)-1)))=0  
            THEN 'n/a'
            WHEN TRIM(SUBSTRING(CNTRY,1,LEN(CNTRY)-1)) IN ('US','USA','United States') then 'USA'
        ELSE TRIM(SUBSTRING(CNTRY,1,LEN(CNTRY)-1))
        END AS CNTRY
        FROM bronze.loc_a101;
        set @end_time=GETDATE();
        PRINT '>>TIME TAKEN TO LOAD loc_a101:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';

        -------------------------------------------------------------------------------------
        PRINT '>>TRUNCATING Table silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2 ;
        PRINT '>>LOADING silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
        SELECT
        ID,
        CAT,
        SUBCAT,
        CASE WHEN TRIM(UPPER(MAINTENANCE)) IN ('YES','NO') THEN TRIM(UPPER(MAINTENANCE))
        ELSE UPPER(SUBSTRING(MAINTENANCE,1,LEN(MAINTENANCE)-1)) 
        END AS MAINTENANCE
        FROM bronze.px_cat_g1v2;
        set @end_time=GETDATE();
        PRINT '>>TIME TAKEN TO LOAD px_cat_g1v2:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';


    --------------------------------------------------------------------------------
        end TRY
    begin CATCH
    PRINT '===================================================='
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' +CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===================================================='

    END CATCH
    set @batch_end=GETDATE()
PRINT '>>TIME TAKEN TO LOAD silver schema: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';
end


go
exec silver.load_silver;
