-- Create Fact Sales Table
Use MedalionDatabase;

Create View gold.fact_sales AS
Select 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price
from silver.crm_sales_details sd
Left Join gold.dim_products pr
On sd.sls_prd_key = pr.product_number
Left Join gold.dim_customers cu
On sd.sls_cust_id = cu.customer_id;