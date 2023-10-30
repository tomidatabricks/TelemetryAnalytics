-- Databricks notebook source
-- MAGIC %md #Ingest and Cleanse DLT.sql
-- MAGIC
-- MAGIC TODO: add data quality constraints

-- COMMAND ----------

-- MAGIC %md ##Bronze

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_telemetry AS
SELECT 
  *
  ,_metadata.file_name AS landing_file_name
FROM
  cloud_files(
    "dbfs:/Volumes/telemetry_analytics_cat/main/landing/",
    "csv"
  );


-- COMMAND ----------

-- MAGIC %md ##Silver

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE clean_telemetry
SELECT
  REVERSE(SPLIT(landing_file_name,"[/_]"))[1] AS group_name, --extract the file timestamp as the gourp name
  CAST(unix_timestamp AS LONG) AS unix_timestamp,
  FROM_UNIXTIME(unix_timestamp/1000.0, 'yyyy-MM-dd HH:mm:ss.SSS') AS datetime_string,
  CAST(left_x AS DOUBLE) AS left_x,
  CAST(left_y AS DOUBLE) AS left_y,
  CAST(right_x AS DOUBLE) AS right_x,
  CAST(right_y AS DOUBLE) AS right_y,
  NOW() AS created_at
FROM
  LIVE.raw_telemetry;

-- COMMAND ----------

-- MAGIC %md ##Gold

-- COMMAND ----------

CREATE
  OR REPLACE LIVE TABLE telemetry_stats AS
SELECT
  group_name
  ,count(1) AS num_events
  ,min(unix_timestamp) AS min_unix_timestamp
  ,max(unix_timestamp) AS max_unix_timestamp
  ,max_unix_timestamp - min_unix_timestamp AS duration_millis
  ,IF(duration_millis > 0, COUNT(1)/duration_millis, 999999.9 ) as sampling_rate
  ,SLICE(COLLECT_LIST(left_x),1,10) AS sampling_left_x_list
  ,NOW() AS created_at
 FROM
  LIVE.clean_telemetry
GROUP BY
  group_name;
