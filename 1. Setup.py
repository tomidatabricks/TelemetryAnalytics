# Databricks notebook source
# MAGIC %md #Setup
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ##Create catalog, schema and volume
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS  tomi_s_cat;
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS tomi_s_cat.controller_telemetrics;
# MAGIC
# MAGIC DROP VOLUME IF EXISTS tomi_s_cat.controller_telemetrics.landing;
# MAGIC
# MAGIC CREATE VOLUME tomi_s_cat.controller_telemetrics.landing;

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /Volumes/tomi_s_cat/controller_telemetrics/landing/

# COMMAND ----------

# MAGIC %md ## Copy images from github to /files for access in Overview notebook 

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls -dl images/*
# MAGIC
# MAGIC rm -rf /dbfs/FileStore/tomi_schumacher/ControllerTelemetrics
# MAGIC mkdir -p /dbfs/FileStore/tomi_schumacher/ControllerTelemetrics
# MAGIC
# MAGIC cp images/* /dbfs/FileStore/tomi_schumacher/ControllerTelemetrics/
# MAGIC
# MAGIC ls -dl /dbfs/FileStore/tomi_schumacher/ControllerTelemetrics/*

# COMMAND ----------

dbutils.fs.ls("/Volumes/tomi_s_cat/controller_telemetrics/landing/")

# COMMAND ----------


