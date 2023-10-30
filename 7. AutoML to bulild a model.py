# Databricks notebook source
# MAGIC %md #AutoML to bulild a model

# COMMAND ----------

# MAGIC %md ##Create a dataset with features and lables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG telemetry_analytics_cat;
# MAGIC USE DATABASE main;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP VIEW IF EXISTS telemetry_labels_and_features;
# MAGIC
# MAGIC CREATE VIEW telemetry_labels_and_features AS
# MAGIC SELECT 
# MAGIC   l.label
# MAGIC   ,f.*
# MAGIC FROM telemetry_features AS f
# MAGIC   JOIN training_labels AS l
# MAGIC     ON f.group_name = l.group_name;
# MAGIC
# MAGIC SELECT * FROM telemetry_labels_and_features;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ```
# MAGIC databricks.automl.classify(
# MAGIC   dataset: Union[pyspark.sql.DataFrame, pandas.DataFrame, pyspark.pandas.DataFrame, str],
# MAGIC   *,
# MAGIC   target_col: str,
# MAGIC   data_dir: Optional[str] = None,
# MAGIC   exclude_cols: Optional[List[str]] = None,                      # <DBR> 10.3 ML and above
# MAGIC   exclude_frameworks: Optional[List[str]] = None,                   # <DBR> 10.3 ML and above
# MAGIC   experiment_dir: Optional[str] = None,                             # <DBR> 10.4 LTS ML and above
# MAGIC   experiment_name: Optional[str] = None,                            # <DBR> 12.1 ML and above
# MAGIC   feature_store_lookups: Optional[List[Dict]] = None,               # <DBR> 11.3 LTS ML and above
# MAGIC   imputers: Optional[Dict[str, Union[str, Dict[str, Any]]]] = None, # <DBR> 10.4 LTS ML and above
# MAGIC   max_trials: Optional[int] = None,                                 # <DBR> 10.5 ML and below
# MAGIC   pos_label: Optional[Union[int, bool, str] = None,                 # <DBR> 11.1 ML and above
# MAGIC   primary_metric: str = "f1",
# MAGIC   time_col: Optional[str] = None,
# MAGIC   timeout_minutes: Optional[int] = None,
# MAGIC ) -> AutoMLSummary
# MAGIC ```

# COMMAND ----------

from databricks import automl

summary = automl.classify(
  dataset= "telemetry_analytics_cat.main.telemetry_labels_and_features"
  ,target_col="label"
  ,timeout_minutes=7
)

display(summary)

# COMMAND ----------

print(summary.best_trial.model_description)

print(summary.best_trial.model_path)

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc") #use unity catalog as the registery

mlflow.register_model(summary.best_trial.model_path, "telemetry_analytics_cat.main.automl_model")


# COMMAND ----------

# MAGIC %sql 
# MAGIC
