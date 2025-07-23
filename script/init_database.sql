/*
====================================================================================================================
Create database "Data Warehouse" and Schemas
====================================================================================================================
Script Purpose:
  this script creates a new database named "DataWarehouse" after checking if it exsists.
  If the database exisits, it is dropped and recreated. Additionally, the script sets up three schemas within the databse: 'bronze', 'silver', 'gold'.
*/

USE master;
GO
--drop and recreate the DataWarehouse database
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name = "DataWarehouse"
  BEGIN
  ALTER DATABASE "dataWarehouse" SET SINGLE_USER WITH ROLLBACK IMMIDIATE;
  DROP DATABASE "DataWarehouse"
  END:
    GO

create database DataWarehouse;
GO 
  
USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
