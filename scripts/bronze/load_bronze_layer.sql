Create or alter procedure bronze.load_bronze as
BEGIN
    DECLARE @batch_start DATETIME,@bath_end DATETIME;
    SET @batch_start=GETDATE();

    BEGIN TRY
        DECLARE @start_time DATETIME, @end_time DATETIME;

        PRINT '===================================================='
        PRINT 'LOADING BRONZE LAYER'
        PRINT '===================================================='

        PRINT '----------------------------------------------------'
        PRINT 'LOADING  CRM TABLES'
        PRINT '----------------------------------------------------'
        SET @start_time=GETDATE();
        PRINT '>>TRUNCATING TABLE:bronze.crm_cust_info'
        Truncate table bronze.crm_cust_info;
        PRINT '>>INSERTING DATA INTO: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/cust_info.csv'
        WITH (
            FIRSTROW=2, /*since data starts from second row in csv, first one contains only column names*/
            FIELDTERMINATOR=',',
            TABLOCK
        );
        SET @end_time=GETDATE();
        PRINT '>>LOAD DURATION '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT '-----------';

        SET @start_time=GETDATE();
        PRINT '>>TRUNCATING TABLE:bronze.prd_info'
        TRUNCATE TABLE bronze.prd_info;
        PRINT '>>INSERTING DATA INTO: bronze.prd_info'
        BULK INSERT bronze.prd_info
        FROM '/var/opt/mssql/prd_info.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        );
        SET @end_time=GETDATE();
        PRINT '>>LOAD DURATION '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT '-----------';

        SET @start_time=GETDATE();
        PRINT '>>TRUNCATING TABLE:bronze.sales_details'
        Truncate table bronze.sales_details;
        PRINT '>>INSERTING DATA INTO: bronze.sales_details'
        bulk insert bronze.sales_details
        FROM '/var/opt/mssql/sales_details.csv'
        with(
            firstrow=2,
            fieldterminator=',',
            tablock
        );
        SET @end_time=GETDATE();
        PRINT '>>LOAD DURATION '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT '-----------';
        
        PRINT '----------------------------------------------------'
        PRINT 'LOADING  ERP TABLES'
        PRINT '----------------------------------------------------'
        SET @start_time=GETDATE();
        PRINT '>>TRUNCATING TABLE:bronze.cust_az12'
        Truncate table bronze.cust_az12;
        PRINT '>>INSERTING DATA INTO: bronze.cust_az12'
        bulk insert bronze.cust_az12
        FROM '/var/opt/mssql/CUST_AZ12.csv'
        with(
            firstrow=2,
            fieldterminator=',',
            tablock
        );
        SET @end_time=GETDATE();
        PRINT '>>LOAD DURATION '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT '-----------';

        SET @start_time=GETDATE();
        PRINT '>>TRUNCATING TABLE:bronze.loc_a101'
        Truncate table bronze.loc_a101;
        PRINT '>>INSERTING DATA INTO: bronze.loc_a101'
        bulk insert bronze.loc_a101
        FROM '/var/opt/mssql/LOC_A101.csv'
        with(
            firstrow=2,
            fieldterminator=',',
            tablock
        );
        SET @end_time=GETDATE();
        PRINT '>>LOAD DURATION '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT '-----------';

        SET @start_time=GETDATE();
        PRINT '>>TRUNCATING TABLE:bronze.px_cat_g1v2'
        Truncate table bronze.px_cat_g1v2;
        PRINT '>>INSERTING DATA INTO: bronze.px_cat_g1v2'
        bulk insert bronze.px_cat_g1v2
        FROM '/var/opt/mssql/PX_CAT_G1V2.csv'
        with(
            firstrow=2,
            fieldterminator=',',
            tablock
        );
        SET @end_time=GETDATE();
        PRINT '>>LOAD DURATION '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT '-----------';

    END TRY
    BEGIN CATCH
        PRINT '===================================================='
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' +CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===================================================='

    END CATCH
    SET @bath_end=GETDATE();
    PRINT '>>TOTAL LOADING TIME: '+CAST(DATEDIFF(second,@batch_start,@bath_end) AS NVARCHAR)+'seconds';
END

/*1.40.00*/

EXEC bronze.load_bronze;