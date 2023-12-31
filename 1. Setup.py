# Databricks notebook source
# MAGIC %md #Setup
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ##Create catalog and schema
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS  telemetry_analytics_cat;
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS telemetry_analytics_cat.main;

# COMMAND ----------

# MAGIC %md ##Droping and creating a volume on cloud storage

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP VOLUME IF EXISTS telemetry_analytics_cat.main.landing;
# MAGIC
# MAGIC CREATE VOLUME telemetry_analytics_cat.main.landing;

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la  /Volumes/telemetry_analytics_cat/main/landing/

# COMMAND ----------

dbutils.fs.ls("/Volumes/telemetry_analytics_cat/main/landing/")
