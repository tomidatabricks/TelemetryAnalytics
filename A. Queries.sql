-- Databricks notebook source
-- MAGIC %md #Queries

-- COMMAND ----------


USE CATALOG telemetry_analytics_cat;
USE DATABASE main;

-- COMMAND ----------

-- MAGIC %md ##Volumes
-- MAGIC

-- COMMAND ----------

DESCRIBE VOLUME telemetry_analytics_cat.main.landing;

-- COMMAND ----------

-- MAGIC %md ##Tables
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC tables_df = spark.sql("SELECT * FROM telemetry_analytics_cat.information_schema.tables WHERE table_schema='main'");
-- MAGIC
-- MAGIC display(tables_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sqls = [f"""
-- MAGIC   SELECT "{table['table_name']}" AS name
-- MAGIC    ,count(1) AS num_rows
-- MAGIC    ,"{table['last_altered']}" AS last_altered 
-- MAGIC   FROM telemetry_analytics_cat.main.{table['table_name']}""" for table in  tables_df.collect() ]
-- MAGIC
-- MAGIC query = " UNION ALL\n".join(sqls)+"\n ORDER BY last_altered ASC"
-- MAGIC
-- MAGIC display(spark.sql(query))

-- COMMAND ----------

-- MAGIC %md ##Models
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # I will need an MLRuntime for that
-- MAGIC # import mlflow
-- MAGIC # mlflow.set_registry_uri("databricks-uc")
-- MAGIC
-- MAGIC #client=MlflowClient()
-- MAGIC #client.search_registered_models()
-- MAGIC

-- COMMAND ----------


