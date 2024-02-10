  -- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `<my_project_id>.ny_taxi.external_green_taxi_data_2022` OPTIONS ( format = 'PARQUET',
    uris = ['gs://homework-03-data-warehouse/*.parquet'] );
  -- Quick data check
SELECT
  *
FROM
  <my_project_id>.ny_taxi.external_green_taxi_data_2022
LIMIT
  10;
  -- Create a non partitioned table from external table with datetime column
CREATE OR REPLACE TABLE
  <my_project_id>.ny_taxi.green_tripdata_2022_non_partitioned AS
SELECT
  *,
  PARSE_DATETIME('%Y/%m/%d %H:%M:%S', lpep_pickup_datetime) AS lpep_pickup_date,
  PARSE_DATETIME('%Y/%m/%d %H:%M:%S', lpep_dropoff_datetime) AS lpep_dropoff_date
FROM
  <my_project_id>.ny_taxi.external_green_taxi_data_2022;
  -- Count of records
SELECT
  COUNT(*)
FROM
  <my_project_id>.ny_taxi.green_tripdata_2022_non_partitioned;
  -- Count distinct PULocationIDs on both tables
  --- External
SELECT
  COUNT(DISTINCT PULocationID)
FROM
  <my_project_id>.ny_taxi.external_green_taxi_data_2022;
  --- Materialized
SELECT
  COUNT(DISTINCT PULocationID)
FROM
  <my_project_id>.ny_taxi.green_tripdata_2022_non_partitioned;
  -- How many records have a fare_amount of 0?
SELECT
  COUNT(*)
FROM
  <my_project_id>.ny_taxi.green_tripdata_2022_non_partitioned
WHERE
  fare_amount = 0;
  -- Create an optimised table to order the results by PUlocationID and filter based on lpep_pickup_datetime
CREATE OR REPLACE TABLE
  <my_project_id>.ny_taxi.green_tripdata_2022_partitioned_clustered
PARTITION BY
  DATE(lpep_pickup_date)
CLUSTER BY
  PUlocationID AS
SELECT
  *
FROM
  <my_project_id>.ny_taxi.green_tripdata_2022_non_partitioned;