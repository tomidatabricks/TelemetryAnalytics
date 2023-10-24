# Databricks notebook source
# MAGIC %md #Overview
# MAGIC
# MAGIC The notebooks and code contained here, runs on Databricks DBR >=14.x on a Unity Catalog enabled workspace
# MAGIC
# MAGIC Use a Databricks repo and check out the code from https://github.com/tomidatabricks/TelemetryAnalytics 
# MAGIC
# MAGIC Disclaimer: Use at your own risk for whatever you want, no warranty whatsever (TODO replace with lawyer blurb)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ##Architecture
# MAGIC
# MAGIC Telemetry analytics example:
# MAGIC - files containing sampled controller telemetry originate from a source server
# MAGIC - each file has a unique name, which will later be refered to as the group_name. (for a concrete example, a file could correspond to controller inputs of flight of a drone)
# MAGIC - data is ingested into the bronze layer, then cleansed into the siver layer, then enhanced and summarized for consumption in a gold layer
# MAGIC - feature engineering is performed on the sliver layer, and the features are used to train a model, the model scores are then added to the gold layer to be consumed by the end user or an operational system
# MAGIC
# MAGIC ![Architecture Diagram](https://github.com/tomidatabricks/TelemetryAnalytics/blob/183a6d9b257378c834c5e6735b2d876fc77e6c1e/images/TelemetryAnalyticsOverview.png?raw=true)
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Telemetry Data
# MAGIC
# MAGIC The telemetry data lands a gzipped csv file in a GCS bucket. 
# MAGIC The schema is the following:
# MAGIC - unix_timestamp: long  
# MAGIC - left_x: float 
# MAGIC - left_y: float 
# MAGIC - right_x: float 
# MAGIC - right_y: float 
# MAGIC
# MAGIC Every 60 milliseconds a new record is created, representing the current positions of the left and right controller buttons (name?) or two joy sticks.
# MAGIC
# MAGIC An example of what telemetry data looks like:
# MAGIC
# MAGIC ```
# MAGIC total 414
# MAGIC drwxrwxrwx 2 nobody                           nogroup  4096 Oct 24 12:19 .
# MAGIC drwxrwxrwx 2 nobody                           nogroup  4096 Oct 24 12:19 ..
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 40783 Oct 24 12:19 1698149956429_human.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 41285 Oct 24 12:19 1698149969804_machine.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 40442 Oct 24 12:19 1698149970470_machine.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 41382 Oct 24 12:19 1698149970963_human.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 42503 Oct 24 12:19 1698149971457_human.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 41177 Oct 24 12:19 1698149971879_human.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 41100 Oct 24 12:19 1698149972273_human.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 41528 Oct 24 12:19 1698149972757_human.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 40924 Oct 24 12:19 1698149973284_human.csv.gz
# MAGIC -rwxrwxrwx 1 spark-79db40d0-f371-4ab7-b32e-a3 nogroup 42120 Oct 24 12:19 1698149973697_machine.csv.gz
# MAGIC /Volumes/telemetry_analytics_cat/main/landing/1698149956429_human.csv.gz
# MAGIC unix_timestamp,left_x,left_y,right_x,right_y
# MAGIC 1698149956848.0,-0.6646773437437716,-0.9896596967493491,0.03445169249839354,-0.9981830518018697
# MAGIC 1698149956864.0,-0.5924185899143813,-0.9978658103651488,-0.344776653592767,-0.8089811731865921
# MAGIC 1698149956881.0,-0.4575046930527369,-0.8199662524652396,-0.1890744680274098,-0.9807185761749637
# MAGIC 1698149956898.0,-0.07156983967145507,-0.974038792286708,-0.6188568205703319,-0.999783742318304
# MAGIC 1698149956914.0,-0.7875262609269068,-0.9932800027644519,-0.6668605044621775,-0.8354202843861674
# MAGIC 1698149956931.0,-0.3382127528348022,-0.8656357931046494,-0.13296461456767406,-0.9731964315806784
# MAGIC 1698149956948.0,-0.6464909067213863,-0.4917229660583882,-0.24817787471110955,-0.6132649113933916
# MAGIC 1698149956964.0,-0.3575989867971946,-0.8857785293882315,-0.6117156833743076,-0.5839162190104631
# MAGIC 1698149956981.0,-0.9333719552546325,-0.7577270292248993,-0.41980029884223646,-0.44468947295631384
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generating Sample Data
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

# COMMAND ----------

# MAGIC %md ##Using generated Sample Data
# MAGIC
# MAGIC Generating sample data has both pros and cons
# MAGIC
# MAGIC Advantages:
# MAGIC - No source for real data is needed, and it is possible to create as much as needed
# MAGIC - Data can be produced very clean, so we don't have to deal with extensive preprocessing as we would have with real data: e.g. malfunctioning sensors emitting invalid data, data loss for a period of time, varing data latency. 
# MAGIC
# MAGIC Disadvantages:
# MAGIC - Generate sample data is not very usefull as training data for ML Model: E.g. in this example, machine controller data differs from human controller by having it align to a grid, and then adding some noise back in so it is not too obvious. But of course when a model is trained to determine if a controller input is from a human or a machine, the model will learn, if there is an underlying grid or not - so esentially the model will learn the assumption that was made in the first place, and be strongly bias to it
# MAGIC
# MAGIC Despite those disadvantges the techniques and architecture used can be applied to real data as a well.

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
