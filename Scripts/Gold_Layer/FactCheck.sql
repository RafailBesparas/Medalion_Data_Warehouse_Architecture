use MedalionDatabase;

-- Fact check, check if all tables can successfully join on a fact table
select * 
from gold.fact_sales f
Left Join gold.dim_customers c
ON c.customer_key = f.customer_key
Left join gold.dim_products p
On p.product_key = f.product_key
Where p.product_key Is Null;