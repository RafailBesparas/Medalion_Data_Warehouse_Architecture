/*
================================================================================
Main Stored Procedure: LoadBronzeLayer
================================================================================
Script Purpose:
    This stored procedure orchestrates the loading of all data from source CSV files
    into the 'bronze' schema. It uses a helper procedure to handle the
    truncate-and-load logic for each table.

Usage Example:
    EXEC bronze.LoadBronzeLayer;
================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.LoadBronzeLayer AS
BEGIN
    DECLARE @batch_start_time DATETIME = GETDATE();

    BEGIN TRY
        PRINT '================================================';
        PRINT 'Starting Bronze Layer Load';
        PRINT '================================================';

        PRINT '--- Loading CRM Tables ---';
        EXEC bronze.bulk_insert_table 'bronze.crm_customer_info', 'C:\Users\user\Desktop\MedalionArchitecture\datasets\source_crm\cust_info.csv';
        EXEC bronze.bulk_insert_table 'bronze.crm_product_info', 'C:\Users\user\Desktop\MedalionArchitecture\datasets\source_crm\prd_info.csv';
        EXEC bronze.bulk_insert_table 'bronze.crm_sales_details', 'C:\Users\user\Desktop\MedalionArchitecture\datasets\source_crm\sales_details.csv';

        PRINT '--- Loading ERP Tables ---';
        EXEC bronze.bulk_insert_table 'bronze.erp_customer_location_a101', 'C:\Users\user\Desktop\MedalionArchitecture\datasets\source_erp\LOC_A101.csv';
        EXEC bronze.bulk_insert_table 'bronze.erp_customer_data_az12', 'C:\Users\user\Desktop\MedalionArchitecture\datasets\source_erp\CUST_AZ12.csv';
        EXEC bronze.bulk_insert_table 'bronze.erp_product_categories_g1v2', 'C:\Users\user\Desktop\MedalionArchitecture\datasets\source_erp\PX_CAT_G1V2.csv';

        DECLARE @batch_end_time DATETIME = GETDATE();
        PRINT '================================================';
        PRINT 'Bronze Layer Load Completed Successfully!';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR: An error occurred during the Bronze Layer load.';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '================================================';
    END CATCH
END;
GO