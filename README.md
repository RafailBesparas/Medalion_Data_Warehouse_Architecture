
# Medalion Data Warehouse Architecture — README

This project implements a **Data Warehouse** using the **Medallion Architecture** (Bronze → Silver → Gold) in SQL Server.  
It demonstrates how raw CSV data can be ingested, cleaned, transformed, and modeled into a star schema for analytics.

---

## Architecture Overview

- **Bronze Layer**: Raw ingestion of CSV data into staging tables.  
- **Silver Layer**: Cleaned, conformed, and validated data with business rules applied.  
- **Gold Layer**: Curated star schema (fact and dimension tables) for analytics and BI.

### Schema Type (Gold Layer)
- **Star Schema**:  
  - Central fact table: `gold.fact_sales` (sales measures).  
  - Surrounding dimensions: `gold.dim_customers`, `gold.dim_products`.  
  - Enables slicing/dicing across customers, products, dates.

---

## Repository Contents

```
DBInit.sql                    # Create database + medallion schemas
DDL_bronze_layer.sql          # Bronze tables (raw CSV structures)
Bulk_Insert_Way_2.sql         # Bronze loader (CSV -> bronze tables)

DDL_Silver_Layer.sql          # Silver tables (cleaned structures)
Bulk_Load_Clean_Data.sql      # Silver loader (bronze -> silver, transformations, checks)
Quality_Check.sql             # Standalone quality check procedure

Customer_Dimensions.sql       # Gold dimension: customers
Product_Dimension.sql         # Gold dimension: products
Fact_Sales_Table.sql          # Gold fact: sales
FactCheck.sql                 # Gold fact referential integrity check

README.md                     # This documentation
```

---

## Layer by Layer

### 1. Initialization
**File:** `DBInit.sql`  
- Creates `MedalionDatabase`.  
- Defines schemas: `bronze`, `silver`, `gold`.

### 2. Bronze Layer
**Files:** `DDL_bronze_layer.sql`, `Bulk_Insert_Way_2.sql`  
- Tables mirror raw CSVs: customers, products, sales, ERP customer/location/product data.  
- Loader procedure `bronze.LoadBronzeLayer` bulk-inserts CSV files into bronze tables.  
- Paths configurable; assumes helper `bronze.bulk_insert_table`.

### 3. Silver Layer
**Files:** `DDL_Silver_Layer.sql`, `Bulk_Load_Clean_Data.sql`, `Quality_Check.sql`  
- Tables define cleaned, conformed structures with audit column `dwh_create_date`.  
- Loader `silver.LoadSilverLayer` transforms bronze → silver with business rules:  
  - **Customers**: Trim names, normalize marital status & gender, dedupe by latest record.  
  - **Products**: Extract category ID, normalize keys, map product line, compute end dates, default missing costs.  
  - **Sales**: Convert int dates → DATE, recalc sales & price if missing/invalid.  
  - **ERP data**: Normalize IDs, clean birthdates/gender, expand country codes.  
- Embedded quality checks + standalone `silver.QualityChecks`: row count parity, null checks, domain checks, sales reconciliation.

### 4. Gold Layer
**Files:** `Customer_Dimensions.sql`, `Product_Dimension.sql`, `Fact_Sales_Table.sql`, `FactCheck.sql`  
- **`gold.dim_customers`**: Combines CRM & ERP, surrogate key, enriches with country & gender fallback.  
- **`gold.dim_products`**: Current products only, enriched with category/subcategory, surrogate key.  
- **`gold.fact_sales`**: Central fact table joining sales details to product & customer dimensions, storing order dates, sales amount, quantity, price.  
- **`FactCheck.sql`**: Validates referential integrity (ensures fact rows map to valid dimension keys).

---

## Data Quality Checks

- **Row count parity** between bronze & silver for major tables.  
- **Null checks** on critical keys.  
- **Domain checks** (marital status, gender normalization).  
- **Sales reconciliation**: sums in bronze vs silver.  
- **Fact integrity checks**: fact rows must map to valid dimension keys (gold layer).

---

## How to Run

1. **Initialize database & schemas**  
   Run `DBInit.sql`.

2. **Create & load Bronze**  
   - Run `DDL_bronze_layer.sql`.  
   - Run `Bulk_Insert_Way_2.sql` to create loader.  
   - Execute:  
     ```sql
     EXEC bronze.LoadBronzeLayer;
     ```

3. **Create & load Silver**  
   - Run `DDL_Silver_Layer.sql`.  
   - Run `Bulk_Load_Clean_Data.sql` to create loader.  
   - Execute:  
     ```sql
     EXEC silver.LoadSilverLayer;
     ```

4. **Run Quality Checks (optional)**  
   ```sql
   EXEC silver.QualityChecks;
   ```

5. **Create Gold Layer**  
   - Run `Customer_Dimensions.sql`, `Product_Dimension.sql`, `Fact_Sales_Table.sql`.  
   - Validate with `FactCheck.sql`.

---

## Summary

- **Bronze**: Raw, schema-aligned tables.  
- **Silver**: Clean, deduplicated, business-conformed data.  
- **Gold**: Star schema with dimensions & fact tables.  
- End-to-end ETL pipeline with built-in quality checks and transformations.

---

## License
MIT (or your preferred license)
