## **01_setup/warehouse_setup.sql**
sql
--Created a warehouse 
create warehouse UberRides_Warehouse;


## **01_setup/database_schema_setup.sql**
sql
-- created a database 
create database UberRideBooking;

--created a raw schema 
create schema UberRideBooking.UberRideBooking_Schema;

--created new refined schema 
create schema UberRideBooking.Refined_Schema;

CREATE SCHEMA UberRideBooking.Enriched_Schema;


## **01_setup/integration_setup.sql**
sql
-- created integrateion (integration is to connect snowflake to source[in this case aws s3] and provide access)
create or replace storage integration UberRide_Analysis
 type = external_stage
 storage_provider = s3
 enabled = true
 storage_aws_role_arn = 'arn:aws:iam::586206155985:role/UberRideRole'
 storage_allowed_locations = ('s3://myuberridesanalysis/');

 --described integration to cross check details 
desc integration UberRide_Analysis;


## **02_data_loading/file_format_setup.sql**
sql
-- created csv file format 
create or replace file format csv_format 
 type = csv 
 field_delimiter = ',' 
 skip_header = 1 
 null_if = ('NULL', 'null') 
 empty_field_as_null = true;


## **02_data_loading/stage_setup.sql**
sql
 --created internal stage 
create or replace stage UberRides
 storage_integration = UberRide_Analysis
 url = 's3://myuberridesanalysis/'
 file_format = csv_format;


## **02_data_loading/table_creation.sql**
sql
-- Drop and recreate table with larger VARCHAR sizes
DROP TABLE Rides;

--created raw table 
CREATE TABLE Rides (
  Date DATE,
  Time TIME,
  Booking_ID VARCHAR(50),           -- Increased from 15
  Booking_Status VARCHAR(50),       -- Increased from 30
  Customer_ID VARCHAR(50),          -- Increased from 15
  Vehicle_Type VARCHAR(50),         -- Increased from 30
  Pickup_Location VARCHAR(500),     -- Increased from 100
  Drop_Location VARCHAR(500),       -- Increased from 100
  Avg_VTAT DECIMAL(5,2),           -- Increased precision
  Avg_CTAT DECIMAL(5,2),           -- Increased precision
  Cancelled_Rides_by_Customer NUMBER,
  Reason_for_cancelling_by_Customer VARCHAR(500),  -- Increased from 200
  Cancelled_Rides_by_Driver NUMBER,
  Driver_Cancellation_Reason VARCHAR(500),         -- Increased from 200
  Incomplete_Rides NUMBER,
  Incomplete_Rides_Reason VARCHAR(500),            -- Increased from 200
  Booking_Value NUMBER,
  Ride_Distance NUMBER,
  Driver_Ratings INTEGER,
  Customer_Rating INTEGER,
  Payment_Method VARCHAR(100)      -- Increased from 30
);

-- Try the copy again
copy into Rides from @UberRides
ON_ERROR = 'CONTINUE';

--Ran query to check data 
SELECT * FROM  UberRideBooking.UberRideBooking_Schema.Rides;


## **03_data_transformation/refined_layer.sql**
sql
--created refind table 
CREATE OR REPLACE TABLE UberRideBooking.Refined_Schema.Refined_Rides (
  Date DATE,
  Time TIME,
  Booking_ID VARCHAR(50),           -- Increased from 15
  Booking_Status VARCHAR(50),       -- Increased from 30
  Customer_ID VARCHAR(50),          -- Increased from 15
  Reason_for_cancelling_by_Customer VARCHAR(500),  -- Increased from 200
  Driver_Cancellation_Reason VARCHAR(500),         -- Increased from 200
  Booking_Value NUMBER,
  Ride_Distance NUMBER,
  Driver_Ratings INTEGER,
  Customer_Rating INTEGER,
  Payment_Method VARCHAR(100)      -- Increased from 30
);

--ran query 
SELECT * FROM UberRideBooking.Refined_Schema.Refined_Rides;

--inserting data from raw table 
INSERT INTO UberRideBooking.Refined_Schema.Refined_Rides
(DATE,
  TIME ,
  Booking_ID, 
  Booking_Status, 
  Customer_ID, 
  Reason_for_cancelling_by_Customer, 
  Driver_Cancellation_Reason, 
  Booking_Value, 
  Ride_Distance, 
  Driver_Ratings, 
  Customer_Rating, 
  Payment_Method ) 
  SELECT DATE,
  TIME ,
  Booking_ID, 
  Booking_Status, 
  Customer_ID, 
  Reason_for_cancelling_by_Customer, 
  Driver_Cancellation_Reason, 
  Booking_Value, 
  Ride_Distance, 
  Driver_Ratings, 
  Customer_Rating, 
  Payment_Method FROM UberRideBooking.UberRideBooking_Schema.Rides ;


## **03_data_transformation/enriched_layer.sql**
sql
  CREATE VIEW UberRideBooking.Enriched_Schema.Enriched_Rides AS
SELECT DATE,
  TIME ,
  LTRIM(RTRIM(Booking_ID, '"'), '"') AS Booking_ID, 
  Booking_Status, 
  LTRIM(RTRIM(Customer_ID, '"'), '"') AS Customer_ID, 
  Reason_for_cancelling_by_Customer, 
  Driver_Cancellation_Reason AS Reason_for_cancelling_by_Driver, 
  Booking_Value, 
  Ride_Distance, 
  Driver_Ratings, 
  Customer_Rating, 
  Payment_Method
  FROM UberRideBooking.Refined_Schema.Refined_Rides ; 
  

SELECT * FROM UberRideBooking.Enriched_Schema.Enriched_Rides ;


## **04_data_export/export_for_tableau.sql**
sql
SELECT * FROM UberRideBooking.Enriched_Schema.Enriched_Rides ;
