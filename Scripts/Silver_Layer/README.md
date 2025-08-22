
# Medalion Data Warehouse Architecture — README

A compact, SQL Server–based data warehouse that follows the **Medallion Architecture**:

- **Bronze** — raw ingestion from CSV sources (minimal transformation).
- **Silver** — cleaned, conformed, and validated data ready for modeling.
- **Gold** — (reserved) curated marts for analytics and BI.

This repo includes scripts to initialize schemas, define bronze & silver tables, load data end‑to‑end, and run quality checks.

---

## Repository Contents

```
DBInit.sql                    # Create database + medallion schemas
DDL_bronze_layer.sql          # Bronze layer tables (raw structures)
Bulk_Insert_Way_2.sql         # Bronze loader procedure (CSV -> bronze tables)
DDL_Silver_Layer.sql          # Silver layer tables (cleaned structures)
Bulk_Load_Clean_Data.sql      # Silver loader procedure (bronze -> silver + transforms + checks)
Quality_Check.sql             # Standalone quality checks procedure for silver
README.md                     # This file
```

---

## What Each File Does

### `DBInit.sql`
Creates the foundational environment:
- Database **`MedalionDatabase`** (if not exists).
- Schemas **`bronze`**, **`silver`**, **`gold`** (idempotent creation).

### `DDL_bronze_layer.sql`
Defines all **bronze** (raw) tables that mirror incoming CSV headers:
- `bronze.crm_customer_info` — customer master (IDs, names, marital status, gender, create date)
- `bronze.crm_product_info` — product master (id/key, name, cost, line, start/end dates)
- `bronze.crm_sales_details` — sales order lines (order number, product key, customer id, order/ship/due dates as INT, sales, qty, price)
- `bronze.erp_customer_location_a101` — customer → country mapping
- `bronze.erp_customer_data_az12` — customer birthdate + gender
- `bronze.erp_product_categories_g1v2` — product category taxonomy (+ maintenance flag)

### `Bulk_Insert_Way_2.sql`
Implements **`bronze.LoadBronzeLayer`** to bulk-load CSVs into bronze tables. Prints progress and errors.  
> Update file paths to your environment. Assumes a helper procedure `bronze.bulk_insert_table` that wraps `BULK INSERT`/`OPENROWSET`.

### `DDL_Silver_Layer.sql`
Creates the **silver** tables (cleaned/typed) with a `dwh_create_date` audit column on each table.

### `Bulk_Load_Clean_Data.sql`
Defines **`silver.LoadSilverLayer`**, the end-to-end loader that **cleans**, **conforms**, and **validates** data from bronze to silver. Also prints timing per section and runs embedded quality checks.

### `Quality_Check.sql`
Defines **`silver.QualityChecks`**, a **standalone** procedure that re-runs silver-layer data quality checks (useful in orchestration after loads).

---

## Transformations (Bronze ➜ Silver)

### CRM — Customers → `silver.crm_cust_info`
- **Trim** first/last names.  
- **Normalize `cst_marital_status`**: `'S' → 'Single'`, `'M' → 'Married'`, others → `'n/a'`.  
- **Normalize `cst_gndr`**: `'F'/'M'` and variants → `'Female'/'Male'`, others → `'n/a'`.  
- **De-duplicate** by `cst_id`, keeping the **latest `cst_create_date`** record.  
- Preserve `cst_create_date` and add load audit timestamp (`dwh_create_date`).

### CRM — Products → `silver.crm_prd_info`
- **Derive `cat_id`** from the product key: take the first five chars and replace `-` with `_`.  
- **Normalize `prd_key`** to the **suffix** of the original key (drop leading category prefix).  
- **Map `prd_line`** codes to labels: `M→Mountain`, `R→Road`, `S→Other Sales`, `T→Touring`, else `'n/a'`.  
- **Type-cast dates**: `prd_start_dt` to `DATE`.  
- **Compute `prd_end_dt`** as **(next `prd_start_dt` per `prd_key`) − 1 day** (creating a valid-from/valid-to window).  
- **Default `prd_cost`** to `0` when missing.  
- Add load audit timestamp.

### CRM — Sales Details → `silver.crm_sales_details`
- **Convert date ints** (`YYYYMMDD`) to `DATE`; set to `NULL` when invalid (`0` or wrong length).  
- **Fix `sls_sales`**: if `NULL` or `0`, **recalculate as** `sls_quantity * ABS(sls_price)`; otherwise keep original.  
- **Fix `sls_price`**: when `NULL`/`<=0`, **derive as** `sls_sales / NULLIF(sls_quantity, 0)`.  
- Add load audit timestamp.

### ERP — Customer Location → `silver.erp_loc_a101`
- **Normalize `cid`**: remove hyphens (`-`).  
- **Expand country codes**: `DE→Germany`, `US/USA→United States`, empty/NULL → `'n/a'`, else pass-through trimmed value.  
- Add load audit timestamp.

### ERP — Customer Data → `silver.erp_cust_az12`
- **Normalize `cid`**: remove leading `'NAS'` if present.  
- **Validate `bdate`**: set to `NULL` if the birthdate is in the **future**.  
- **Normalize `gen`**: variants of `F`/`M` → `'Female'/'Male'`, else `'n/a'`.  
- Add load audit timestamp.

### ERP — Product Categories → `silver.erp_px_cat_g1v2`
- **Pass-through** of `id, cat, subcat, maintenance` (trim/typing handled by table defs).  
- Add load audit timestamp.

---

## Data Quality Checks (Silver)

Available in two forms:
1) **Embedded** at the end of `silver.LoadSilverLayer` (during the silver load run).  
2) **Standalone** via `silver.QualityChecks` (can be run any time).

**Checks performed:**
- **Row count parity** between Bronze and Silver for key tables (e.g., `crm_sales_details`, `erp_loc_a101`).  
- **Null checks** on critical keys (`silver.crm_cust_info.cst_id`, `silver.crm_sales_details.sls_ord_num`).  
- **Normalization assertions** (only allowed values after mapping):  
  - `silver.crm_cust_info.cst_marital_status ∈ {'Single','Married','n/a'}`  
  - `silver.crm_cust_info.cst_gndr ∈ {'Female','Male','n/a'}`  
- **Sales reconciliation**: compare `SUM(sls_sales)` in Bronze vs Silver for `crm_sales_details`.  
- On failure, **RAISERROR** with details; on success, print PASS summaries.

---

## How to Run (Typical Order)

1. **Initialize DB & Schemas**  
   Run `DBInit.sql`.

2. **Create Bronze Tables**  
   Run `DDL_bronze_layer.sql`.

3. **Load Bronze**  
   - Run `Bulk_Insert_Way_2.sql` to create `bronze.LoadBronzeLayer`.
   - Execute:
     ```sql
     EXEC bronze.LoadBronzeLayer;
     ```

4. **Create Silver Tables**  
   Run `DDL_Silver_Layer.sql`.

5. **Load Silver (with embedded checks)**  
   - Run `Bulk_Load_Clean_Data.sql` to create `silver.LoadSilverLayer`.
   - Execute:
     ```sql
     EXEC silver.LoadSilverLayer;
     ```

6. **(Optional) Re-run Quality Checks**  
   - Run `Quality_Check.sql` to create `silver.QualityChecks`.
   - Execute:
     ```sql
     EXEC silver.QualityChecks;
     ```

---

## Notes & Configuration

- **File paths**: Update CSV file locations inside `Bulk_Insert_Way_2.sql` to match your environment (SQL Server service account must have read access).  
- **Helper proc**: `bronze.bulk_insert_table` is expected by the bronze loader; implement it to wrap `BULK INSERT` with CSV options.  
- **Idempotency**: DDL scripts drop & recreate tables; loader procedures use `CREATE OR ALTER`; re-runs are safe.  
- **Auditability**: All silver tables include `dwh_create_date` set on insert.

---

## License
MIT (or your preferred license)
