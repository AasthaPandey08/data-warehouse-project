/*
================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================================
Script Purpose: 
The strored procedure loads data into bronze schema from external CSV files.
It Performs the following actions:
  1) Truncates the Bronze table before loading data.
  2) Uses "Bulk Insert" command to load data from CSV to bronze table.

Parameters:
  None.
  This stored procedure does not accepts any parameters or returns any value.

Usage Example:
  EXEC bronze.load_bronze;
==================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================================================';
		PRINT 'Loading bronze layer';
		PRINT '===================================================================';

		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '<<truncating the table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '<<inserting the data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\aship\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '<<truncating the table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '<<inserting the data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\aship\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '<<truncating the table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '<<inserting the data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\aship\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';

		PRINT '-------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '-------------------------------------------------------------------'
		
		SET @start_time = GETDATE();
		PRINT '<<truncating the table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '<<inserting the data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\aship\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '<<truncating the table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '<<inserting the data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\aship\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '<<truncating the table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '<<inserting the data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\aship\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '<<Load Duration:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '-------------------';
	SET @batch_end_time = GETDATE();
	PRINT '============================================================';
	print 'Loading Bronze Layer is completed!';
	print '<<Batch load duration:' +CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	PRINT '============================================================';
	END TRY
	BEGIN CATCH
		PRINT '============================================================';
		PRINT 'ERROR OCCURED DURING ADDING BRONZE LAYER';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message:' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================================';
	END CATCH
END;


