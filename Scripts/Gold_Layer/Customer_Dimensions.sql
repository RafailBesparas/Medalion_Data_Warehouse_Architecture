use MedalionDatabase;

-- Creating the gold layer

-- Checking for duplicates
Select cst_id, COUNT(*) FROM 
(
-- Gold Layer
Select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
Left Join silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 la
ON ci.cst_key = la.cid
) t Group By cst_id
Having Count(*) > 1;

-- Create the golden layer ---------
Select 
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master of the data for gender info
ELSE Coalesce(ca.gen, 'n/a')
END AS new_gen,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
Left Join silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

-- Create two transformations
Select Distinct 
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master of the data for gender info
ELSE Coalesce(ca.gen, 'n/a')
END AS new_gen
from silver.crm_cust_info ci
Left Join silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1, 2;

-- Adding window function
Create View gold.dim_customers AS
Select 
ROW_NUMBER() Over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master of the data for gender info
ELSE Coalesce(ca.gen, 'n/a')
END AS new_gen,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
Left Join silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 la
ON ci.cst_key = la.cid;