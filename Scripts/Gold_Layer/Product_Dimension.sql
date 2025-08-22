use MedalionDatabase;

-- Create the product dimensions, check for duplicates
Select prd_key,
Count(*)
FROM(
Select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
Left Join silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
where prd_end_dt is null -- Filter our all historical data
)t Group By prd_key
Having Count(*) > 1;

-- Create product dimensions
Create view gold.dim_products As 
Select 
ROW_NUMBER() Over (Order By pn.prd_start_dt, pn.prd_key) AS product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt AS start_date
from silver.crm_prd_info pn
Left Join silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
where prd_end_dt is null ;