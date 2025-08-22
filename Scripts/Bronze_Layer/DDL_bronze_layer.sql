/*
===============================================================================
Database Setup Script - Create the ERP and CRM Tables
===============================================================================
Purpose:
  This script sets up the foundational tables to store our business data.
  It's designed to be run whenever we need to refresh or rebuild the structure
  of our main data tables for customer information, products, and sales.

  Running this script will first clear out any old versions of these tables
  before creating new, empty ones with the correct column headings.
===============================================================================
*/

-- This section sets up the table for customer relationship management (CRM) information.
IF OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_info;
GO

CREATE TABLE bronze.crm_customer_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE
);
GO

-- This section creates the table to store information about our products.
IF OBJECT_ID('bronze.crm_product_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_product_info;
GO

CREATE TABLE bronze.crm_product_info (
    prd_id             INT,
    prd_key            NVARCHAR(50),
    prd_nm             NVARCHAR(50),
    prd_cost           INT,
    prd_line           NVARCHAR(50),
    prd_start_dt       DATETIME,
    prd_end_dt         DATETIME
);
GO

-- This section creates the table to track sales and order details.
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num        NVARCHAR(50),
    sls_prd_key        NVARCHAR(50),
    sls_cust_id        INT,
    sls_order_dt       INT,
    sls_ship_dt        INT,
    sls_due_dt         INT,
    sls_sales          INT,
    sls_quantity       INT,
    sls_price          INT
);
GO

-- This section creates a table to store customer location data from the ERP system.
IF OBJECT_ID('bronze.erp_customer_location_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_customer_location_a101;
GO

CREATE TABLE bronze.erp_customer_location_a101 (
    cid                NVARCHAR(50),
    cntry              NVARCHAR(50)
);
GO

-- This section creates a table for additional customer data from the ERP system, including birth date and gender.
IF OBJECT_ID('bronze.erp_customer_data_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_customer_data_az12;
GO

CREATE TABLE bronze.erp_customer_data_az12 (
    cid                NVARCHAR(50),
    bdate              DATE,
    gen                NVARCHAR(50)
);
GO

-- This section creates a table for product categories and maintenance information.
IF OBJECT_ID('bronze.erp_product_categories_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_product_categories_g1v2;
GO

CREATE TABLE bronze.erp_product_categories_g1v2 (
    id                 NVARCHAR(50),
    cat                NVARCHAR(50),
    subcat             NVARCHAR(50),
    maintenance        NVARCHAR(50)
);
GO