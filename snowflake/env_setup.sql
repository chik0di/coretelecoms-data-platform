-- Use the AccountAdmin role for administrative privileges
USE ROLE accountadmin;

-- STEP 1: Create Warehouse
CREATE WAREHOUSE IF NOT EXISTS core_telecom_wh 
WITH WAREHOUSE_SIZE = 'X-SMALL';

-- STEP 2: Create Database
CREATE DATABASE IF NOT EXISTS core_telecom_db;

-- STEP 3: Create Role
CREATE ROLE IF NOT EXISTS data_engineer;

-- STEP 4: View Current Warehouse Permissions
SHOW GRANTS ON WAREHOUSE core_telecom_wh;

-- STEP 5: Grant Warehouse and Database Usage to Role
GRANT USAGE ON WAREHOUSE core_telecom_wh TO ROLE data_engineer;
GRANT USAGE ON DATABASE core_telecom_db TO ROLE data_engineer;

-- STEP 6: Assign Role to User
GRANT ROLE data_engineer TO USER k0di;

-- STEP 7: Grant Database Privileges 
GRANT ALL ON DATABASE core_telecom_db TO ROLE data_engineer;

-- STEP 8: Switch to data_engineer role
USE ROLE data_engineer;

-- STEP 9: Create Schemas in core_telecom_db Database
CREATE SCHEMA core_telecom_db.staging;
CREATE SCHEMA core_telecom_db.silver;
CREATE SCHEMA core_telecom_db.gold;
CREATE SCHEMA core_telecom_db.marts;

-- STEP 10: Grant Schema Ownership Usage to Role
GRANT USAGE ON SCHEMA core_telecom_db.staging TO ROLE data_engineer;
GRANT USAGE ON SCHEMA core_telecom_db.silver TO ROLE data_engineer;
GRANT USAGE ON SCHEMA core_telecom_db.gold TO ROLE data_engineer;
GRANT USAGE ON SCHEMA core_telecom_db.marts TO ROLE data_engineer;

GRANT OWNERSHIP ON SCHEMA core_telecom_db.staging TO ROLE data_engineer;
GRANT OWNERSHIP ON SCHEMA core_telecom_db.silver TO ROLE data_engineer;
GRANT OWNERSHIP ON SCHEMA core_telecom_db.gold TO ROLE data_engineer;
GRANT OWNERSHIP ON SCHEMA core_telecom_db.marts TO ROLE data_engineer;
