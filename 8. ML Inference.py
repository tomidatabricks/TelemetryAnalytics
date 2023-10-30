# Databricks notebook source
# MAGIC %md #ML Inference
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as sf
import mlflow

mlflow.set_registry_uri("databricks-uc") #use unity catalog as the registery


# COMMAND ----------

features = spark.table("telemetry_analytics_cat.main.telemetry_features")

display(features)

# COMMAND ----------

model_udf = mlflow.pyfunc.spark_udf(
    spark, 
    model_uri="models:/telemetry_analytics_cat.main.automl_model@champion",
    result_type="string")

# COMMAND ----------

telemetry_predictions = features.withColumn("prediction", model_udf())

display(telemetry_predictions)

# COMMAND ----------

(telemetry_predictions.withColumn("created_at", sf.now()).write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("telemetry_analytics_cat.main.telemetry_predictions")
)

# COMMAND ----------

display(spark.table("telemetry_analytics_cat.main.telemetry_features"))

# COMMAND ----------


