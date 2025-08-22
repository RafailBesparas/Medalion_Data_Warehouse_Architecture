
# Gold Layer: Dimensions and Fact Tables

This stage represents the **Gold Layer** of the Medallion Architecture.  
It introduces **dimension tables** and a **fact table** to form a **Star Schema**, suitable for analytics and BI tools.

---

## Schema Type
- **Star Schema**  
  - Central **fact table** (`gold.fact_sales`) contains measurable business events (sales transactions).  
  - Surrounding **dimension tables** (`gold.dim_customers`, `gold.dim_products`) provide descriptive context for analysis.  
  - Optimized for slicing, dicing, and aggregating measures across multiple dimensions.

---

## Files & Their Purpose

### `Customer_Dimensions.sql`
- Builds **`gold.dim_customers`** as a dimension view enriched with CRM and ERP attributes.  
- **Transformations performed:**
  - Join **CRM customer info** with **ERP customer data** (birthdate, gender) and **ERP location** (country).  
  - Gender prioritization: take CRM gender if present, otherwise fall back to ERP gender.  
  - Adds a surrogate **`customer_key`** using `ROW_NUMBER()` for stable dimension referencing.  
- **Outputs:** View `gold.dim_customers` with keys, names, marital status, gender, birthdate, country, and create date.

### `Product_Dimension.sql`
- Builds **`gold.dim_products`** as a dimension view combining CRM product data with ERP product categories.  
- **Transformations performed:**
  - Filter to **current products only** (`prd_end_dt IS NULL`).  
  - Join CRM products with ERP product categories (cat, subcat, maintenance).  
  - Adds surrogate **`product_key`** using `ROW_NUMBER()`.  
- **Outputs:** View `gold.dim_products` with product id/number/name, category info, cost, line, and start date.

### `Fact_Sales_Table.sql`
- Builds **`gold.fact_sales`**, the central fact table of the schema.  
- **Transformations performed:**
  - Sales details (order number, dates, sales/quantity/price) are enriched with foreign keys:  
    - `product_key` from `gold.dim_products`.  
    - `customer_key` from `gold.dim_customers`.  
  - Preserves raw sales measures (`sales_amount`, `quantity`, `price`).  
- **Outputs:** View `gold.fact_sales` ready for BI/analytics.

### `FactCheck.sql`
- Provides a **quality check** for the fact table joins.  
- **Checks performed:**
  - Ensures all fact rows successfully join to corresponding dimension keys.  
  - Query flags any `fact_sales` rows where `product_key` is null (indicating orphaned fact records).  
- Used to validate referential integrity between the fact and dimensions.

---

## Summary
- **Star Schema** with `fact_sales` at the center, surrounded by `dim_customers` and `dim_products`.  
- Dimension tables include surrogate keys for stable relationships.  
- Fact table captures sales measures, linked to customer and product contexts.  
- Quality check script ensures completeness of the joins and prevents orphan fact records.

