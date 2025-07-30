/*
==================================================================================
DDL Script: Create Gold Views
===================================================================================
Purpose:
  This script creates views for the Gold Layer in the Datawarehouse.
  the gold layer represents the final facts and dimension tables(Star Schema)

  Each view performs transformation and combine data from the silver layer
  to produce clean, enriched and business ready data set.

Usage:
  These views can be queried directly for analytics and reporting.
=====================================================================================
  
*/

create VIEW gold.dim_customers AS
select 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_fisrtname AS fist_name,
	ci.cst_lastname AS last_name,
	ci.cst_material_status AS marital_status,
	la.cntry AS country,
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE(ca.gen , 'n/a')
	END As gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid 
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

CREATE VIEW gold.dim_products AS
select
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat As category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null --filter out all historical data

CREATE VIEW gold.fact_sales AS
select 
	sd.sales_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sd.sales_order as sales_order_date,
	sd.sales_ship_dt AS shipping_date,
	sd.sales_due_dt As due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
from silver.crm_sales_details sd
left join gold.dim_products dp
on sd.sales_prd_key = dp.product_number
left join gold.dim_customers dc
on sd.sales_cust_id = dc.customer_id
