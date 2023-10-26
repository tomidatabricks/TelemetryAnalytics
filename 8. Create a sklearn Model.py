# Databricks notebook source
# MAGIC %md #Create a Model
# MAGIC
# MAGIC TODO: figure out how to infer signature 
# MAGIC TODO: use scikit learn instead of sparkml 

# COMMAND ----------

import pyspark.sql.functions as sf
import mlflow

mlflow.set_registry_uri("databricks-uc") #use unity catalog as the registery

# COMMAND ----------

features_df = spark.table("telemetry_analytics_cat.main.telemetry_features")


# COMMAND ----------

features_df = spark.table("telemetry_analytics_cat.main.telemetry_features")
lables_df= spark.table("telemetry_analytics_cat.main.training_labels")

df_features_labled = features_df.join(lables_df, on="group_name")

df_features_labled = df_features_labled.withColumn("label", sf.when( sf.col("label") == "machine", 1.0).otherwise(0.0))

display(df_features_labled)

# COMMAND ----------


from pyspark.ml.feature import RFormula

formula = RFormula(formula="label ~ . - group_name", featuresCol="features", labelCol="label")

prep_df = formula.fit(df_features_labled).transform(df_features_labled)

display(prep_df)

# COMMAND ----------



# train_df, test_df = prep_df.randomSplit([0.7, 0.3], seed=42)

# lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# # Fit the model
# lrModel = lr.fit(train_df)

# print("Coefficients: " + str(lrModel.coefficients))
# print("Intercept: " + str(lrModel.intercept))

# display(lrModel.transform(test_df))

# COMMAND ----------

import mlflow.spark

with mlflow.start_run():
  lr = LogisticRegression()

  # Fit the model
  lrModel = lr.fit(prep_df)

  print("Coefficients: " + str(lrModel.coefficients))
  print("Intercept: " + str(lrModel.intercept))

  # Log the model with MLflow
  mlflow.spark.log_model(lrModel, "model")

  display(lrModel.transform(prep_df))

# COMMAND ----------

# MAGIC %md ##STUFF BELOW IS NOT WORKING YET

# COMMAND ----------

from mlflow.models import infer_signature, set_signature
from pyspark.ml.functions import vector_to_array

# Apply the model to the DataFrame to generate predictions
# predictions_df = lrModel.transform(prep_df).select("prediction")

# display(predictions_df)
# predictions_array = predictions_df.collect()


signature = infer_signature(prep_df, lrModel.transform(prep_df))

# Extract the vector of predictions from the DataFrame
#predictions_array = predictions_df.rdd.map(lambda x: vector_to_array(x[0])[0]).collect()

# Create a signature from the original DataFrame and the predictions
signature = infer_signature(prep_df, {"predictions": predictions_array})

# Set the signature for the registered model
#set_signature(model_uri, signature)

set_s

# COMMAND ----------


autolog_run = mlflow.last_active_run()
model_uri = "runs:/{}/model".format(autolog_run.info.run_id)
mlflow.register_model(model_uri, "telemetry_analytics_cat.main.simple_model")

print(model_uri)

# COMMAND ----------

mlflow.pyspark.ml.logging.

