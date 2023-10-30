-- Databricks notebook source
-- MAGIC %md #Queries

-- COMMAND ----------


USE CATALOG telemetry_analytics_cat;
USE DATABASE main;

-- COMMAND ----------

-- MAGIC %md ##Row Counts an last updated by for each table
-- MAGIC
-- MAGIC TODO rewrite in pyspark, make it dynamic, look up the tables that exist in the database and create the report for those tables
-- MAGIC TODO add info about models and feature stores and volumes in the same database as well

-- COMMAND ----------

          SELECT "raw_telemetry" AS name, count(1), NULL AS t FROM raw_telemetry
UNION ALL SELECT "clean_telemetry" AS name, count(1), max(created_at) AS t FROM clean_telemetry
UNION ALL SELECT "clean_telemetry_features" AS name, count(1), max(created_at) AS t FROM telemetry_features
UNION ALL SELECT "telemetry_labels_and_features" AS name, count(1), max(created_at) AS t FROM telemetry_labels_and_features
ORDER BY t ASC
