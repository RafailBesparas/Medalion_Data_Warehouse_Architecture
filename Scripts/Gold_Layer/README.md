
# SQL Data Pipeline — Medallion Architecture

This README documents your warehouse setup and bronze-layer load in clear layers (bronze → silver → gold), with script-by-script, line-by-line commentary for the three SQL files you shared.

> Stack assumptions: Microsoft SQL Server (T-SQL). If you’re targeting another engine, call out differences (e.g., `OBJECT_ID`/`CREATE OR ALTER` availability, file import method).

---

## 1) Architecture Overview

**Medallion layers**

- **bronze** — raw/landing tables, schema-aligned to source files with minimal transformation (CSV → tables).
- **silver** — cleaned and typed data (dedupes, type casting, conforming columns). *(not in this drop, but schema reserved)*
- **gold** — business-ready marts (dimensions/facts, aggregates, semantic views). *(not in this drop, but schema reserved)*

**Data domains present in _bronze_**

- **CRM**: customer master, product master, sales order line items.
- **ERP**: customer location, customer demographics (birthdate/gender), product categories/maintenance flags.

**Execution order**

1. `DBInit.sql` — create database and schemas
2. `DDL_bronze_layer.sql` — (re)create bronze tables
3. `Bulk_Insert_Way_2.sql` — run stored procedure `bronze.LoadBronzeLayer` to bulk-load CSVs

---

## 2) Getting Started

### Prerequisites
- SQL Server instance with rights to create DB, schemas, procedures, and tables.
- Local access to the CSV files referenced in the bulk-load procedure (update file paths as needed).

### One-time initialization
```sql
-- 1) Create DB + schemas
:r .\DBInit.sql
-- 2) Create bronze tables
:r .\DDL_bronze_layer.sql
```

### Run the bronze load
```sql
-- 3) Create the loader procedure
:r .\Bulk_Insert_Way_2.sql

-- 4) Execute the end-to-end bronze load
EXEC bronze.LoadBronzeLayer;
```

> Tip: In SSMS, use `:r` to include files, or open and run each script in order.

---

## 3) Script-by-Script Documentation (Line-by-Line)

### A) `DBInit.sql` — Create database and schemas

**Purpose**: Create database `MedalionDatabase` (if missing) and three schemas: `bronze`, `silver`, `gold`.

**Listing with commentary**

```sql
/* Creates DW DB and medallion schemas */
```

1. `USE master;`  
   *Switch to the system DB to safely create a new database.*

2–7. `IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'MedalionDatabase') CREATE DATABASE MedalionDatabase;`  
   *Idempotent database creation.*

8–9. `USE MedalionDatabase;`  
   *Target the new (or existing) database for subsequent schema creation.*

10–15. `IF NOT EXISTS (...) CREATE SCHEMA bronze;`  
   *Create raw/landing layer schema if absent.*

16–21. `IF NOT EXISTS (...) CREATE SCHEMA silver;`  
   *Create cleaned/refined layer schema if absent.*

22–27. `IF NOT EXISTS (...) CREATE SCHEMA gold;`  
   *Create curated/business-ready layer schema if absent.*

**Notes**
- Uses dynamic `EXEC('CREATE SCHEMA ...')` for compatibility with the conditional check.
- All blocks are idempotent, so re-running is safe.

---

### B) `DDL_bronze_layer.sql` — Define bronze tables

**Purpose**: Drop-and-create six bronze tables aligned to the CSV headers. This keeps structures clean and repeatable for reloads.

#### 1) CRM Tables

**`bronze.crm_customer_info`**
```sql
IF OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL DROP TABLE bronze.crm_customer_info;
CREATE TABLE bronze.crm_customer_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE
);
```
- `IF OBJECT_ID(...) DROP` — *idempotent reset; ensures a clean table definition on each run.*
- Columns:
  - `cst_id` — business/customer numeric id (may be natural key from CRM).
  - `cst_key` — alternate key/surrogate reference (textual).
  - `cst_firstname`, `cst_lastname`, `cst_marital_status`, `cst_gndr` — personal attributes as provided.
  - `cst_create_date` — date the customer was created in source system.

**`bronze.crm_product_info`**
```sql
IF OBJECT_ID('bronze.crm_product_info', 'U') IS NOT NULL DROP TABLE bronze.crm_product_info;
CREATE TABLE bronze.crm_product_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
```
- Product master with validity window (`prd_start_dt`, `prd_end_dt`) for time-bounded attributes.

**`bronze.crm_sales_details`**
```sql
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
```
- Line-level sales details; dates arrive as **INT** (likely yyyymmdd) for raw fidelity—convert in silver.
- Monetary fields are **INT** in raw; cast to appropriate numeric/decimal types in silver to avoid precision issues.

#### 2) ERP Tables

**`bronze.erp_customer_location_a101`**
```sql
IF OBJECT_ID('bronze.erp_customer_location_a101', 'U') IS NOT NULL DROP TABLE bronze.erp_customer_location_a101;
CREATE TABLE bronze.erp_customer_location_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);
```
- Customer → country mapping (location snapshot/version `A101` is preserved in the name).

**`bronze.erp_customer_data_az12`**
```sql
IF OBJECT_ID('bronze.erp_customer_data_az12', 'U') IS NOT NULL DROP TABLE bronze.erp_customer_data_az12;
CREATE TABLE bronze.erp_customer_data_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);
```
- Birthdate and gender attributes keyed by `cid` (ERP id).

**`bronze.erp_product_categories_g1v2`**
```sql
IF OBJECT_ID('bronze.erp_product_categories_g1v2', 'U') IS NOT NULL DROP TABLE bronze.erp_product_categories_g1v2;
CREATE TABLE bronze.erp_product_categories_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);
```
- Product category taxonomy with additional `maintenance` flag (e.g., requires service schedule).

**Modeling notes**
- No PK/FK constraints in bronze—this is intentional to minimize ingestion friction. Enforce keys and types in silver/gold.
- Keep the source naming to ease reconciliation (late-arriving columns/records).

---

### C) `Bulk_Insert_Way_2.sql` — Orchestrate bronze bulk-load

**Purpose**: Define a stored procedure `bronze.LoadBronzeLayer` that batch-loads all CSVs into bronze tables via a helper `bronze.bulk_insert_table` (assumed to exist) and prints telemetry.

**Procedure body with commentary**

```sql
CREATE OR ALTER PROCEDURE bronze.LoadBronzeLayer AS
BEGIN
    DECLARE @batch_start_time DATETIME = GETDATE();
```

1–2. `CREATE OR ALTER PROCEDURE`  
   *Idempotent definition; updates if exists.*

3. `DECLARE @batch_start_time = GETDATE()`  
   *Capture start time for duration telemetry.*

```sql
    BEGIN TRY
        PRINT '================================================';
        PRINT 'Starting Bronze Layer Load';
        PRINT '================================================';
```
4–7. `BEGIN TRY` and banner `PRINT`s  
   *Human-friendly console/log markers.*

```sql
        PRINT '--- Loading CRM Tables ---';
        EXEC bronze.bulk_insert_table 'bronze.crm_customer_info', 'C:\...\source_crm\cust_info.csv';
        EXEC bronze.bulk_insert_table 'bronze.crm_product_info', 'C:\...\source_crm\prd_info.csv';
        EXEC bronze.bulk_insert_table 'bronze.crm_sales_details', 'C:\...\source_crm\sales_details.csv';
```
8–12. Load **CRM** CSVs  
   *Each call truncates and bulk-loads the target table via the helper.*

```sql
        PRINT '--- Loading ERP Tables ---';
        EXEC bronze.bulk_insert_table 'bronze.erp_customer_location_a101', 'C:\...\source_erp\LOC_A101.csv';
        EXEC bronze.bulk_insert_table 'bronze.erp_customer_data_az12', 'C:\...\source_erp\CUST_AZ12.csv';
        EXEC bronze.bulk_insert_table 'bronze.erp_product_categories_g1v2', 'C:\...\source_erp\PX_CAT_G1V2.csv';
```
13–17. Load **ERP** CSVs  
   *Same helper pattern for ERP domain.*

```sql
        DECLARE @batch_end_time DATETIME = GETDATE();
        PRINT '================================================';
        PRINT 'Bronze Layer Load Completed Successfully!';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
```
18–22. Wrap-up, duration, and success banner.

```sql
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
```
23–33. `BEGIN CATCH` logs error number, state, and message to console.  
*Production tip*: also raise the error (`THROW`) or insert into an audit table to fail upstream schedulers clearly.

**Assumed helper: `bronze.bulk_insert_table`**  
Expected behavior based on usage:  
- Accepts `(target_table_name NVARCHAR, file_path NVARCHAR)`  
- Truncates the target table (or loads into staging then swaps).  
- Calls `BULK INSERT` (or `OPENROWSET(BULK...)`) with appropriate CSV options: `FIELDTERMINATOR`, `ROWTERMINATOR`, `FIRSTROW = 2`, `CODEPAGE = '65001'` (if UTF‑8).  
- Validates row count and returns success/failure.

Update file paths to your environment (UNC path or local path on SQL Server host). Consider using an external table or a proxy/credential if loading from Azure/AWS storage.

---

## 4) Data Contracts (Bronze)

| Table | Grain | Key(s) | Notable Columns | Source CSV |
|---|---|---|---|---|
| `bronze.crm_customer_info` | 1 row per CRM customer | (`cst_id` or `cst_key`) | names, marital status, gender, create date | `cust_info.csv` |
| `bronze.crm_product_info` | 1 row per product version | (`prd_id` or `prd_key`) | cost, line, validity window | `prd_info.csv` |
| `bronze.crm_sales_details` | 1 row per order line | (`sls_ord_num`, `sls_prd_key`) | order/ship/due dates (INT), qty, price, sales | `sales_details.csv` |
| `bronze.erp_customer_location_a101` | 1 row per ERP customer | `cid` | `cntry` | `LOC_A101.csv` |
| `bronze.erp_customer_data_az12` | 1 row per ERP customer | `cid` | `bdate`, `gen` | `CUST_AZ12.csv` |
| `bronze.erp_product_categories_g1v2` | 1 row per product | `id` | `cat`, `subcat`, `maintenance` | `PX_CAT_G1V2.csv` |

> Keys are not enforced in bronze; do so in silver (e.g., dedupe by latest file load timestamp).

---

## 5) Next Steps (Silver/Gold design hints)

- **Type casting & conformance (silver):** cast date INTs → DATE, amounts → DECIMAL, normalize gender/marital status enums, dedupe on business keys.
- **Conformed dimensions (gold):** `dim_customer`, `dim_product` with SCD2 on descriptive attributes; `fact_sales` at order line grain.
- **Quality checks:** row counts per file, duplicate natural keys, referential integrity (e.g., every `sls_prd_key` must map to a product).

---

## 6) Operations & Troubleshooting

- **Idempotency:** All DDL scripts drop-and-create; loader procedure can be re-run safely.
- **File path failures:** Ensure SQL Server service account can read the CSV location (NTFS/SMB permissions).
- **Schema drift:** Add new columns to bronze tables before loading; keep names aligned with header row.
- **Error surfaces:** On failures, CATCH block prints details; consider adding `THROW;` after logging to fail jobs.

---

## 7) Appendix: Example Helper Procedure (optional)

> If you need a starting point for `bronze.bulk_insert_table`, adapt something like:

```sql
CREATE OR ALTER PROCEDURE bronze.bulk_insert_table
    @target NVARCHAR(256),
    @file   NVARCHAR(4000)
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'';
    SET @sql = N'TRUNCATE TABLE ' + QUOTENAME(PARSENAME(@target, 2)) + N'.' + QUOTENAME(PARSENAME(@target, 1)) + ';
BULK INSERT ' + @target + N'
FROM ''' + @file + N'''
WITH (FIRSTROW = 2, FIELDTERMINATOR='','', ROWTERMINATOR=''
'', TABLOCK);';
    EXEC sp_executesql @sql;
END;
```
*Adjust terminators, encoding, and error handling to your CSVs and SQL Server version.*
