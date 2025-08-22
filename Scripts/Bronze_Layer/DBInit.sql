/*
This script creates a Data Warehouse database (if it does not exist),
initializes the MedalionDatabase, and sets up bronze, silver and gold schemas
to organize data layers for the Medallion Architecture.
*/

-- Create Datawarehouse Database
USE master;
GO

-- Check if database exists, if not create it
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'MedalionDatabase')
BEGIN
    CREATE DATABASE MedalionDatabase;
END
GO

-- Use the database
USE MedalionDatabase;
GO

-- Create the schema for bronze (only if it does not exist)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

-- Create schema silver
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- Create schema gold
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO
