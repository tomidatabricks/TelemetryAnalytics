# Databricks notebook source
# MAGIC %md #Rewrite Feature Table
# MAGIC
# MAGIC LIMITATION: 
# MAGIC - Delta Live Tables cannot create feature tables
# MAGIC - ML clusters cannot read materialized views

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG telemetry_analytics_cat;
# MAGIC USE DATABASE main;

# COMMAND ----------

import pyspark.sql.functions as sf
from feature_utils import TelemetryFeatureUtils

feature_df = TelemetryFeatureUtils.calc_vel_accl_jerk_features(
  spark.table("telemetry_analytics_cat.main.clean_telemetry")
)

(feature_df.withColumn("created_at", sf.current_timestamp()
                       ).write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("telemetry_analytics_cat.main.telemetry_features")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Drop the existing constraint if it exists
# MAGIC ALTER TABLE telemetry_features DROP CONSTRAINT IF EXISTS pk_group_name;
# MAGIC
# MAGIC --make it a feature table by defining a primary key
# MAGIC ALTER TABLE telemetry_features ALTER COLUMN group_name SET NOT NULL;
# MAGIC ALTER TABLE telemetry_features ADD CONSTRAINT pk_group_name PRIMARY KEY(group_name)

# COMMAND ----------


