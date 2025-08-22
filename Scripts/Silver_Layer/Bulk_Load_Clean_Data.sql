USE MedalionDatabase;
GO

/*
================================================================================
Main Stored Procedure: LoadSilverLayer
================================================================================
Script Purpose:
   This stored procedure orchestrates the loading of all data from the bronze
   schema into the 'silver' schema. It cleans, refines, and validates the data during this process.

Usage Example:
   EXEC silver.LoadSilverLayer;
================================================================================
*/
CREATE OR ALTER PROCEDURE silver.LoadSilverLayer AS
BEGIN
    DECLARE @batch_start_time DATETIME = GETDATE();
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @bronze_row_count INT, @silver_row_count INT;
    DECLARE @bronze_total_sales INT, @silver_total_sales INT;
    DECLARE @error_message NVARCHAR(4000);

    BEGIN TRY
        PRINT '================================================';
        PRINT 'Starting Silver Layer Load';
        PRINT '================================================';

        -- Truncate all silver tables to ensure a clean load
        PRINT '--- Truncating Silver Tables ---';
        TRUNCATE TABLE silver.crm_cust_info;
        TRUNCATE TABLE silver.crm_prd_info;
        TRUNCATE TABLE silver.crm_sales_details;
        TRUNCATE TABLE silver.erp_loc_a101;
        TRUNCATE TABLE silver.erp_cust_az12;
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '--- Truncation Complete ---';

        PRINT '--- Loading CRM Tables ---';

        -- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_customer_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
                AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_product_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            -- FIX: Only recalculate sales if it's invalid, otherwise use the original value
            ISNULL(
                NULLIF(sls_sales, 0), -- Use NULLIF to handle both NULL and 0
                sls_quantity * ABS(sls_price)
            ) AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';


        PRINT '--- Loading ERP Tables ---';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid, cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_customer_location_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid, bdate, gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_customer_data_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        -- Loading erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id, cat, subcat, maintenance
        )
        SELECT
            id, cat, subcat, maintenance
        FROM bronze.erp_product_categories_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

        --
        -- BEGIN: QUALITY CHECKS
        --
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
        --
        -- END: QUALITY CHECKS
        --

        DECLARE @batch_end_time DATETIME = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH
        SET @error_message = 'ERROR: An error occurred during the Silver Layer load. ' +
            'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) +
            ' | Error State: ' + CAST(ERROR_STATE() AS NVARCHAR) +
            ' | Error Message: ' + ERROR_MESSAGE();
        PRINT '=========================================='
        PRINT @error_message;
        PRINT '=========================================='
    END CATCH
END;
GO