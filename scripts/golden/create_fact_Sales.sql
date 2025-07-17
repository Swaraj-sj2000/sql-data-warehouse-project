create view golden.fact_sales as
SELECT
sl.sls_ord_num,
pr.product_key,
cu.customer_key,
sl.sls_order_dt as order_date,
sl.sls_ship_dt as shipping_date,
sl.sls_due_dt as due_date,
sl.sls_sales as sales_amount,
sl.sls_quantity as quantity,
sl.sls_price as price

from silver.crm_sales_details as sl
left join golden.dim_products as pr 
on sl.sls_prd_key=pr.product_number
left join golden.dim_customers as cu 
on sl.sls_cust_id=cu.customer_id ;