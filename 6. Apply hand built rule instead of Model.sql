-- Databricks notebook source
-- MAGIC %md #Apply Model

-- COMMAND ----------

USE CATALOG telemetry_analytics_cat;
USE DATABASE main;

-- COMMAND ----------

DROP FUNCTION IF EXISTS get_prediction;

--**********************************************************
--THIS IS THE SUPER SIMPLE MODEL
--**********************************************************
CREATE FUNCTION get_prediction(left_y_accel_stddev FLOAT) RETURNS STRING
RETURN
    CASE
      WHEN left_y_accel_stddev <= 0.0025 THEN 'human'
      ELSE 'machine'
    END;

SELECT get_prediction(0),get_prediction(1);

-- COMMAND ----------

DROP TABLE IF EXISTS telemetry_predictions;

CREATE TABLE telemetry_predictions AS
SELECT 
  group_name
  ,get_prediction(left_y_accel_stddev) AS prediction
  ,now() as created_at
FROM telemetry_features;

SELECT * FROM telemetry_predictions;

-- COMMAND ----------

DROP VIEW IF EXISTS telemetry_labels_and_predictions;

CREATE VIEW telemetry_labels_and_predictions AS
SELECT 
  l.group_name
  ,l.label
  ,p.prediction
  ,l.label = l.label AS correct_prediction
FROM telemetry_predictions AS p
  JOIN training_labels AS l
    ON p.group_name = l.group_name;

SELECT * FROM telemetry_labels_and_predictions



-- COMMAND ----------

SELECT 
  SUM(IF(correct_prediction,1,0)) AS num_correct_predictions
  ,COUNT(1) AS total_correct_predictions
  ,num_correct_predictions/total_correct_predictions as ratio_correct_preditions
FROM telemetry_labels_and_predictions

-- COMMAND ----------


