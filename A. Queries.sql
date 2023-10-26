-- Databricks notebook source
-- MAGIC %md #Queries

-- COMMAND ----------


USE CATALOG telemetry_analytics_cat;
USE DATABASE main;

-- COMMAND ----------

          SELECT "raw_telemetry" AS name, count(1), NULL AS t FROM raw_telemetry
UNION ALL SELECT "clean_telemetry" AS name, count(1), max(created_at) AS t FROM clean_telemetry
UNION ALL SELECT "clean_telemetry_features" AS name, count(1), max(created_at) AS t FROM telemetry_features
UNION ALL SELECT "telemetry_labels_and_features" AS name, count(1), max(created_at) AS t FROM telemetry_labels_and_features
ORDER BY t ASC

-- COMMAND ----------

df
