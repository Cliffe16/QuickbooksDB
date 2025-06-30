/*ADMINISTRATIVE RIGHTS*/
-- Create a login for the Python application
CREATE LOGIN QB_API WITH PASSWORD = 'DRUMbeats9876@FL!';

-- Create a user in your database for that login
CREATE USER Cliffe FOR LOGIN QB_API;

-- Grant permissions to the user
GRANT SELECT, UPDATE ON SCHEMA::etl TO Cliffe;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::qb_data TO Cliffe;
-- Grant the permission to a specific login
GRANT ADMINISTER BULK OPERATIONS TO Oscar1;
GO

/*DATABASE ENCRYPTION*//*
-- (This must be run on the master database first)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'a_very_strong_password_for_master_key';

-- (Now run this on your company database)
CREATE DATABASE ENCRYPTION KEY
WITH ALGORITHM = AES_256
ENCRYPTION BY SERVER MASTER KEY;

ALTER DATABASE QuickbooksDB SET ENCRYPTION ON;*/

