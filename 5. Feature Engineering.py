# Databricks notebook source
# MAGIC %md #Feature Engineering

# COMMAND ----------

# MAGIC %md ##Calculate speed and acceleration as additional features
# MAGIC
# MAGIC Speed, acceleration and jerk can be approximated using our coordinate data as:
# MAGIC
# MAGIC
# MAGIC $$ v_{x_i} = \frac{x_i-x_{i-1}}{\Delta{t}} $$
# MAGIC $$ a_{x_i} = \frac{v_{x_i}-v_{x_{i-1}}}{\Delta{t}}  $$
# MAGIC $$ j_{x_i} = \frac{a_{x_i}-a_{x_{i-1}}}{\Delta{t}} $$
# MAGIC
# MAGIC Or generic, the derivative
# MAGIC $$ \frac{d}{dt}col_i = \frac{col_i-col_{i-1}}{\Delta{t}} $$
# MAGIC
# MAGIC This is calulated in see module 
# MAGIC [feature_utils.py](feature_utils.py) (<-- TODO BROKEN LINK !)
# MAGIC
# MAGIC

# COMMAND ----------

DO_TEST = True

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG telemetry_analytics_cat;
# MAGIC USE DATABASE main;

# COMMAND ----------

import pyspark.sql.functions as sf
from pyspark.sql.window import Window

from feature_utils import TelemetryFeatureUtils

# COMMAND ----------

# MAGIC %md ##Testing the Formula to calculate dx/dt for speed and acceleration
# MAGIC

# COMMAND ----------

if (DO_TEST):
  df = (spark.range(120)
        .withColumn("group_id",sf.floor(sf.col("id")/60))
        .withColumn("group_name",sf.cast("String",sf.col("group_id")))
        .withColumn("id1",sf.col("id")%60)
        .withColumn("speed", 1 + 1.4 * sf.col("group_id"))
        .withColumn("millis", sf.col("id1")*1000.0/60.0)
        .withColumn("degree", sf.col("id1")/60*6.28*sf.col("speed") ) 
        .withColumn("x",sf.sin("degree"))
        .withColumn("y",sf.cos("degree"))
        .orderBy(["millis", "group_name"])
  )

  df = TelemetryFeatureUtils.add_d_col_dt(df, "x_speed", sf.col("x"), sf.col("millis"), sf.col("group_name"))
  df = TelemetryFeatureUtils.add_d_col_dt(df, "x_accel", sf.col("x_speed"), sf.col("millis"), sf.col("group_name"))

  df = TelemetryFeatureUtils.add_d_col_dt(df, "y_speed", sf.col("y"), sf.col("millis"), sf.col("group_name"))
  df = TelemetryFeatureUtils.add_d_col_dt(df, "y_accel", sf.col("y_speed"), sf.col("millis"), sf.col("group_name"))

  display(df)

# COMMAND ----------

feature_df = TelemetryFeatureUtils.calc_vel_accl_jerk_features(
  spark.table("telemetry_analytics_cat.main.clean_telemetry")
)

display(feature_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Looking at the graph above, left `left_y_accel_stddev = 0.0025` partitions the groups in two categories. 

# COMMAND ----------

# MAGIC %md ##Training labels

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS training_labels;

# COMMAND ----------


labels_df = (spark.read.format("csv").option("header", True)
      .load("dbfs:/Volumes/telemetry_analytics_cat/main/landing/")
      .withColumn("input_file_name", sf.col("_metadata.file_path"))
      .withColumn("label", sf.reverse(sf.split(sf.col("_metadata.file_path"),"[_.]")).getItem(2))
      .withColumn("group_name", sf.reverse(sf.split(sf.col("_metadata.file_path"),"[/_]")).getItem(1))
      .select("group_name", "label", sf.now().alias("created_at")).distinct()
)

(labels_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("telemetry_analytics_cat.main.training_labels")
)



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM training_labels

# COMMAND ----------

# MAGIC %md ##Show with lables

# COMMAND ----------

display(labels_df.join(feature_df, "group_name"))

# COMMAND ----------

# MAGIC %md ##Try out the rule we defined above

# COMMAND ----------

if (DO_TEST):

    joined_df = (labels_df
        .join(feature_df, "group_name")
        .withColumn("prediction",
            sf.when(feature_df["left_y_accel_stddev"]<=0.0025, "human")
            .otherwise("machine"))
        .select("group_name","left_y_accel_stddev","label","prediction")
    )

    display(joined_df)

# COMMAND ----------

# MAGIC %md ##Create Feature table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS telemetry_features;

# COMMAND ----------


#TODO use a merge for that, do we still use the feature store API for that or DLT merge
(feature_df.withColumn("created_at", sf.now()).write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("telemetry_analytics_cat.main.telemetry_features")
)

# COMMAND ----------

# MAGIC %md ##Add a primary key to the delta table to make it UC feature table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE telemetry_features 
# MAGIC   ALTER COLUMN group_name SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE telemetry_features
# MAGIC   ADD CONSTRAINT pk_group_name PRIMARY KEY(group_name);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM telemetry_features

# COMMAND ----------


