USE DataWarehouse;

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL /*U=OBJECT TYPE USER DEFINED TABLE*/
    Drop TABLE bronze.crm_cust_info;
Create table bronze.crm_cust_info(
    cst_id INT,
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

IF OBJECT_ID('bronze.prd_info','U') IS NOT NULL 
    Drop TABLE bronze.prd_info;
Create table bronze.prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

IF OBJECT_ID('bronze.sales_details','U') IS NOT NULL 
    Drop TABLE bronze.sales_details;
Create table bronze.sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);


IF OBJECT_ID('bronze.cust_az12','U') IS NOT NULL 
    Drop TABLE bronze.cust_az12;
create table bronze.cust_az12(
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50)
);

IF OBJECT_ID('bronze.loc_a101','U') IS NOT NULL 
    Drop TABLE bronze.loc_a101;
create table bronze.loc_a101(
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50)
);

IF OBJECT_ID('bronze.px_cat_g1v2','U') IS NOT NULL 
    Drop TABLE bronze.px_cat_g1v2;
create table bronze.px_cat_g1v2(
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50)
);

