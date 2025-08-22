USE MedalionDatabase;
GO

/*
================================================================================
Stored Procedure: silver.QualityChecks
================================================================================
Script Purpose:
   This stored procedure performs a series of data quality checks on the
   'silver' schema tables to ensure data integrity and consistency.

Actions Performed:
   - Validates row counts against the Bronze layer.
   - Checks for NULL values in critical columns.
   - Verifies that data normalization was successful.
   - Reconciles total sales data between Bronze and Silver layers.

Usage Example:
   EXEC silver.QualityChecks;
================================================================================
*/
CREATE OR ALTER PROCEDURE silver.QualityChecks AS
BEGIN
    DECLARE @bronze_row_count INT, @silver_row_count INT;
    DECLARE @bronze_total_sales INT, @silver_total_sales INT;
    DECLARE @error_message NVARCHAR(4000);

    BEGIN TRY
        PRINT '================================================';
        PRINT 'BEGINNING SILVER LAYER QUALITY CHECKS';
        PRINT '================================================';

        -- Check 1: Row Count Validation
        PRINT '--- Validating Row Counts ---';
        SELECT @bronze_row_count = COUNT(*) FROM bronze.crm_sales_details;
        SELECT @silver_row_count = COUNT(*) FROM silver.crm_sales_details;
        IF @bronze_row_count = @silver_row_count
            PRINT '>> PASS: Row count for crm_sales_details is consistent. Bronze: ' + CAST(@bronze_row_count AS NVARCHAR) + ' | Silver: ' + CAST(@silver_row_count AS NVARCHAR);
        ELSE
            BEGIN
                SET @error_message = '>> FAIL: Row count mismatch for crm_sales_details. Bronze: ' + CAST(@bronze_row_count AS NVARCHAR) + ' | Silver: ' + CAST(@silver_row_count AS NVARCHAR);
                RAISERROR(@error_message, 16, 1);
            END

        SELECT @bronze_row_count = COUNT(*) FROM bronze.erp_customer_location_a101;
        SELECT @silver_row_count = COUNT(*) FROM silver.erp_loc_a101;
        IF @bronze_row_count = @silver_row_count
            PRINT '>> PASS: Row count for erp_loc_a101 is consistent.';
        ELSE
            BEGIN
                SET @error_message = '>> FAIL: Row count mismatch for erp_loc_a101. Bronze: ' + CAST(@bronze_row_count AS NVARCHAR) + ' | Silver: ' + CAST(@silver_row_count AS NVARCHAR);
                RAISERROR(@error_message, 16, 1);
            END

        -- Check 2: Null and Invalid Value Checks
        PRINT '--- Validating Critical Column Values ---';
        IF NOT EXISTS (SELECT 1 FROM silver.crm_cust_info WHERE cst_id IS NULL)
            PRINT '>> PASS: No NULL cst_id found in silver.crm_cust_info.';
        ELSE
            BEGIN
                SET @error_message = '>> FAIL: Found NULL cst_id in silver.crm_cust_info.';
                RAISERROR(@error_message, 16, 1);
            END

        IF NOT EXISTS (SELECT 1 FROM silver.crm_sales_details WHERE sls_ord_num IS NULL)
            PRINT '>> PASS: No NULL sls_ord_num found in silver.crm_sales_details.';
        ELSE
            BEGIN
                SET @error_message = '>> FAIL: Found NULL sls_ord_num in silver.crm_sales_details.';
                RAISERROR(@error_message, 16, 1);
            END

        -- Check 3: Value Normalization Validation
        PRINT '--- Validating Normalized Data ---';
        IF NOT EXISTS (SELECT 1 FROM silver.crm_cust_info WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a'))
            PRINT '>> PASS: Marital status values are properly normalized.';
        ELSE
            BEGIN
                SET @error_message = '>> FAIL: Un-normalized values found in silver.crm_cust_info.cst_marital_status.';
                RAISERROR(@error_message, 16, 1);
            END

        IF NOT EXISTS (SELECT 1 FROM silver.crm_cust_info WHERE cst_gndr NOT IN ('Female', 'Male', 'n/a'))
            PRINT '>> PASS: Gender values are properly normalized.';
        ELSE
            BEGIN
                SET @error_message = '>> FAIL: Un-normalized values found in silver.crm_cust_info.cst_gndr.';
                RAISERROR(@error_message, 16, 1);
            END

        -- Check 4: Data Reconciliation
        PRINT '--- Reconciling Sales Data ---';
        SELECT @bronze_total_sales = SUM(sls_sales) FROM bronze.crm_sales_details;
        SELECT @silver_total_sales = SUM(sls_sales) FROM silver.crm_sales_details;
        IF @bronze_total_sales = @silver_total_sales
            PRINT '>> PASS: Total sales reconciled successfully.';
        ELSE
            BEGIN
                SET @error_message = '>> FAIL: Sales data reconciliation failed. Bronze: ' + CAST(@bronze_total_sales AS NVARCHAR) + ' | Silver: ' + CAST(@silver_total_sales AS NVARCHAR);
                RAISERROR(@error_message, 16, 1);
            END

        PRINT '================================================';
        PRINT 'SILVER LAYER QUALITY CHECKS COMPLETED';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        SET @error_message = 'ERROR: An error occurred during the Silver Layer quality checks. ' +
            'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) +
            ' | Error State: ' + CAST(ERROR_STATE() AS NVARCHAR) +
            ' | Error Message: ' + ERROR_MESSAGE();
        PRINT '=========================================='
        PRINT @error_message;
        PRINT '=========================================='
    END CATCH
END;
GO