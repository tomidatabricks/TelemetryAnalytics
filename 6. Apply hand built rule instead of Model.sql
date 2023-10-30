-- Databricks notebook source
-- MAGIC %md #Apply hand built rule instead of Model

-- COMMAND ----------

USE CATALOG telemetry_analytics_cat;
USE DATABASE main;

-- COMMAND ----------

-- MAGIC %md ##a) SQL Function with a single parameter

-- COMMAND ----------

DROP FUNCTION IF EXISTS get_prediction;

--**********************************************************
--THIS IS THE SUPER SIMPLE MODEL
--**********************************************************
CREATE FUNCTION get_prediction(left_y_accel_stddev double) RETURNS STRING
RETURN IF(left_y_accel_stddev <= 0.0025, 'human', 'machine');

-- COMMAND ----------

-- MAGIC %md ###Tests

-- COMMAND ----------

-- TESTS
SELECT 'dsdsd_1' AS group_name, get_prediction(1) AS prediction
UNION ALL
SELECT 'dsdsd_0' AS group_name, get_prediction(0) AS prediction

-- COMMAND ----------

-- MAGIC %md ##b) SQL Function with a struct parameter

-- COMMAND ----------

DROP FUNCTION IF EXISTS get_prediction_struct;

--**********************************************************
--THIS IS THE SUPER SIMPLE MODEL
--**********************************************************
CREATE FUNCTION get_prediction_struct(features struct<
    group_name: string
    ,left_x_vel_median: double ,left_x_accel_median: double ,left_x_jerk_median: double
    ,left_y_vel_median: double ,left_y_accel_median: double ,left_y_jerk_median: double
    ,left_x_vel_stddev: double ,left_x_accel_stddev: double ,left_x_jerk_stddev: double
    ,left_y_vel_stddev: double ,left_y_accel_stddev: double ,left_y_jerk_stddev: double
    ,created_at: timestamp
>) RETURNS STRING
RETURN IF(features.left_y_accel_stddev <= 0.0025, 'human', 'machine');


-- COMMAND ----------

-- MAGIC %md ###Tests

-- COMMAND ----------


-- TESTS
SELECT group_name, get_prediction_struct(struct(*)) AS prediction, struct(*)
FROM (SELECT 
  'dsdsd' AS group_name
  ,57 AS left_x_vel_median,57 AS left_x_accel_median ,57 AS left_x_jerk_median
  ,57 AS left_y_vel_median,57 AS left_y_accel_median ,57 AS left_y_jerk_median
  ,57 AS left_x_vel_stddev,57 AS left_x_accel_stddev ,57 AS left_x_jerk_stddev
  ,57 AS left_y_vel_stddev,1 AS left_y_accel_stddev ,57 AS left_y_jerk_stddev
  ,NULL AS timestamp
)
UNION ALL
SELECT group_name, get_prediction_struct(struct(*)) AS prediction, struct(*)
FROM (SELECT 
  'dsdsd' AS group_name
  ,57 AS left_x_vel_median,57 AS left_x_accel_median ,57 AS left_x_jerk_median
  ,57 AS left_y_vel_median,57 AS left_y_accel_median ,57 AS left_y_jerk_median
  ,57 AS left_x_vel_stddev,57 AS left_x_accel_stddev ,57 AS left_x_jerk_stddev
  ,57 AS left_y_vel_stddev,0 AS left_y_accel_stddev ,57 AS left_y_jerk_stddev    
  ,NULL AS timestamp
)


-- COMMAND ----------

SELECT 
  *
  ,now() AS created_at
  ,get_prediction_struct(struct(
    group_name
    ,left_x_vel_median, left_x_accel_median, left_x_jerk_median
    ,left_y_vel_median, left_y_accel_median, left_y_jerk_median
    ,left_x_vel_stddev, left_x_accel_stddev, left_x_jerk_stddev
    ,left_y_vel_stddev, left_y_accel_stddev, left_y_jerk_stddev
    ,created_at)) AS prediction
FROM telemetry_features

-- COMMAND ----------

SELECT 
  *
  ,now() AS created_at
  ,get_prediction_struct(struct(*)) AS prediction
FROM telemetry_features

-- COMMAND ----------

DROP TABLE IF EXISTS telemetry_predictions;

CREATE TABLE telemetry_predictions AS
SELECT 
  * EXCEPT(created_at)
  ,now() as created_at
  ,get_prediction_struct(struct(*)) AS prediction
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
  ,l.created_at AS label_created_at
  ,p.created_at AS prediction_created_at
FROM telemetry_predictions AS p
  JOIN training_labels AS l
    ON p.group_name = l.group_name;

SELECT * FROM telemetry_labels_and_predictions



-- COMMAND ----------

-- TODO AFF CONFUSION MATRIX in SKlearn, SKlplsdo

SELECT 
  SUM(IF(correct_prediction,1,0)) AS num_correct_predictions
  ,COUNT(1) AS total_correct_predictions
  ,num_correct_predictions/total_correct_predictions as ratio_correct_preditions
FROM telemetry_labels_and_predictions

-- COMMAND ----------


