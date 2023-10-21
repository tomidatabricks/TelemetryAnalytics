# Databricks notebook source
# MAGIC %md #Overview
# MAGIC
# MAGIC The notebooks and code contained here, runs on Databricks DBR 14.x
# MAGIC
# MAGIC https://github.com/tomidatabricks/TelemetryAnalytics
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![Architecture Diagram](files/tomi_schumacher/ControllerTelemetrics/Architecture_Overview.png)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Telemetry Data
# MAGIC A zip file containing CSV files with the same schema is created, and lands on a GCS bucket.
# MAGIC
# MAGIC All CSV files have the same schema
# MAGIC - unix_timestamp: long  
# MAGIC - left_x: float 
# MAGIC - left_y: float 
# MAGIC - right_x: float 
# MAGIC - right_y: float 
# MAGIC
# MAGIC ##Comming up with a simple data generation algorithm
# MAGIC The range of the the x and y coordinates is [-1,1]
# MAGIC
# MAGIC We will simulate "drawing circles" with a wheel chair controller, a joy stick or a gaming controller. So we just use sine and cosine for the 
# MAGIC
# MAGIC - to simulate human motion, we add/substract a random error to make the circle a bit wobbly
# MAGIC - to simulate machine motion, we round x and y to a step size and add some error to get a jagged circle thingy
# MAGIC
# MAGIC The generator will randomly generate a CSV file, with either human or machine data, and store them under a unique name. Part of the name will be human or machine, so we got labled data as a side effect, but in the ingestion we will ignore this.
# MAGIC
# MAGIC The goal is for the model to determine, whether the input was generated by a machine or human.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TEMPORARY FUNCTION IF EXISTS digitize;
# MAGIC CREATE TEMPORARY FUNCTION digitize(x FLOAT) RETURNS FLOAT 
# MAGIC RETURN round(x, 1) + (rand()-0.5)/50; -- to 1 decimal
# MAGIC
# MAGIC --DROP TEMP VIEW IF EXISTS source;
# MAGIC CREATE OR REPLACE TEMPORARY VIEW source AS SELECT 
# MAGIC      round(id*1000/60) + (UNIX_TIMESTAMP(NOW()) * 1000)  AS unix_timestamp
# MAGIC      ,unix_timestamp/300 AS degree
# MAGIC      ,degree+(randn()-0.5)*0.1 AS noisy_degree1
# MAGIC      ,degree+(randn()-0.5)*0.1 AS noisy_degree2
# MAGIC      ,degree+(randn()-0.5)*0.1 AS noisy_degree3
# MAGIC      ,degree+(randn()-0.5)*0.1 AS noisy_degree4
# MAGIC     FROM range(1, 1800)
# MAGIC ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW human_telemetrics AS SELECT 
# MAGIC    unix_timestamp
# MAGIC    ,sin(noisy_degree1) AS left_x
# MAGIC    ,cos(noisy_degree2) AS left_y
# MAGIC    ,sin(noisy_degree3) AS right_x
# MAGIC    ,cos(noisy_degree4) AS right_y
# MAGIC FROM source;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW machine_telemetrics AS SELECT 
# MAGIC    unix_timestamp
# MAGIC    ,digitize(sin(noisy_degree1)) AS left_x
# MAGIC    ,digitize(cos(noisy_degree2)) AS left_y
# MAGIC    ,digitize(sin(noisy_degree3)) AS right_x
# MAGIC    ,digitize(cos(noisy_degree4)) AS right_y
# MAGIC FROM source;
# MAGIC
# MAGIC SELECT * FROM human_telemetrics

# COMMAND ----------

# MAGIC %sql SELECT * FROM human_telemetrics

# COMMAND ----------

# MAGIC %sql SELECT * FROM machine_telemetrics
# MAGIC
