IF OBJECT_ID('golden.dim_customers', 'V') IS NOT NULL
    DROP view golden.dim_customers;
GO
create view golden.dim_customers as
    select 
        ROW_NUMBER() OVER (ORDER BY cst_id) as Customer_key, --creating a surrogate key in case no primary key is present
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        
        CASE WHEN ci.cst_gndr!='n/a' then ci.cst_gndr
            else coalesce(ca.gen,'n/a')
        end as gender,
        la.cntry as country,
        ci.cst_marital_status as marital_status,
        ca.bdate as birthdate,
        ci.cst_create_date as create_date
        
    from silver.crm_cust_info as ci
    left join silver.erp_cust_az12 as ca
    on ci.cst_key=ca.cid
    left join silver.erp_loc_a101 as la 
    on ci.cst_key=la.cid;

