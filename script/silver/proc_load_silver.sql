/*
================================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze -> Silver)
================================================================================
Script Purpose: 
The strored procedure loads data into silver schema from Bronze layer.
It Performs the following actions:
  1) Truncates the Silver table before loading data.
  2) Inserts transformed ad cleansed data from bronze layer to the silver layer

Parameters:
  None.
  This stored procedure does not accepts any parameters or returns any value.

Usage Example:
  EXEC silver.silver;
==================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
			SET @batch_start_time = GETDATE()
			PRINT '===================================================================';
			PRINT 'Loading silver layer';
			PRINT '===================================================================';

			PRINT '-------------------------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '<<Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '<<Inserting Data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_fisrtname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date)
		select 
			cst_id,
			cst_key,
			TRIM(cst_fisrtname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END cst_material_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		from(
			select *,
			row_number() over(partition by cst_id order by cst_create_date) lat_occ
			from bronze.crm_cust_info where cst_id is not null) t
		where 
			lat_occ = 1;
		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';





		SET @start_time = GETDATE();

		PRINT '<<Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '<<Inserting Data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sales_ord_num,
			sales_prd_key,
			sales_cust_id,
			sales_order,
			sales_ship_dt,
			sales_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select
			sales_ord_num,
			sales_prd_key,
			sales_cust_id,
			CASE WHEN sales_order_dt = 0 OR LEN(sales_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sales_order_dt AS VARCHAR) AS DATE)
			END AS sales_order_dt,
			sales_ship_dt,
			sales_due_dt,
			CASE 
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales !=  sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,
				sls_quantity ,
			CASE
				WHEN sls_price <=0 OR sls_price IS NULL 
					THEN sls_sales/NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price	
		from bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';




		SET @start_time = GETDATE();
		PRINT '<<Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '<<Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) as prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line,
			prd_start_dt,
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS dt
		from bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';




		
		PRINT '-------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '-------------------------------------------------------------------'
		
		SET @start_time = GETDATE();

		PRINT '<<Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '<<Inserting Data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid, bdate,gen)
		SELECT 
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
				END cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
				END AS bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
				END AS gen
		from bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';





		SET @start_time = GETDATE();
		PRINT '<<Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '<<Inserting Data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry)
		select 
			REPLACE(cid, '-', '') AS cid,
			CASE	
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United Satates'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
				END AS cntry -- normalize and handle missing or blank country codes
		from bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';


		SET @start_time = GETDATE();
		PRINT '<<Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '<<Inserting Data into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(id, cat, subcat,maintenance)
		select * from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';
	END TRY
	BEGIN CATCH
		PRINT '============================================================';
		PRINT 'ERROR OCCURED DURING ADDING BRONZE LAYER';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message:' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================================';
	END CATCH
	SET @batch_end_time = GETDATE()
	PRINT '<<Entire Batch Load Duration:' +CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds' ;
END;

