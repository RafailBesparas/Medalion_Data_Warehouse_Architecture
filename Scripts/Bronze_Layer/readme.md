# Project: SQL Data Pipeline (Medallion Architecture)
- What this project is

A compact SQL-based data pipeline organized with a Medallion Architecture. It establishes a database and three schemas—bronze (raw ingestion), silver (cleaned/transformed), and gold (curated/business-ready)—to structure customer, product, and sales data sourced from CSV files.

# What each file does
- DBInit.sql
1. Creates the foundational environment.
2. Creates the database MedalionDatabase if it does not already exist.
3. Creates the schemas bronze, silver, and gold (idempotent checks so it’s safe to rerun).

-  DDL_bronze_layer.sql
1. Defines all bronze tables that mirror the incoming CSV structures.
- bronze.crm_customer_info — Customer master fields (IDs, names, marital status, gender, create date).
- bronze.crm_product_info — Product master (id/key, name, cost, line, start/end dates).
- bronze.crm_sales_details — Sales order lines (order number, product key, customer id, order/ship/due dates, sales, quantity, price).
- bronze.erp_customer_location_a101 — Customer location (country per customer id).
- bronze.erp_customer_data_az12 — Additional customer attributes (birthdate, gender).
- bronze.erp_product_categories_g1v2 — Product taxonomy and maintenance indicator.

- Bulk_Insert_Way_2.sql
1. Implements the loader that moves CSV data into the bronze tables.
2. Creates stored procedure bronze.LoadBronzeLayer.
3. Calls a helper (expected) bronze.bulk_insert_table to bulk-insert each CSV into its matching table.
4. Separates CRM and ERP loads and prints simple progress messages and errors.
5. File paths in the procedure should be adjusted to your environment.