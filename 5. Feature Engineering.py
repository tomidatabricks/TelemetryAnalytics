# Databricks notebook source
# MAGIC %md #Feature Engineering

# COMMAND ----------

# MAGIC %md ##Calculate speed and acceleration as additional features
# MAGIC
# MAGIC Speed can be approximated using our coordinate data as:
# MAGIC
# MAGIC $$
# MAGIC vx_i = \frac{x_i-x_{i-1}}{t_i-t_{i-1}}
# MAGIC $$
# MAGIC $$
# MAGIC vx_i = \frac{vx_i-vx_{i-1}}{t_i-t_{i-1}}
# MAGIC $$
# MAGIC or generalized
# MAGIC $$
# MAGIC \frac{d}{dt}col_i = \frac{col_i-col_{i-1}}{t_i-t_{i-1}}
# MAGIC $$
# MAGIC
# MAGIC
# MAGIC
# MAGIC As a special case, we are dealing with a constant sampling rate so this can be simplified
# MAGIC
# MAGIC $$
# MAGIC vx_i = \frac{x_i-x_{i-1}}{\Delta{t}}
# MAGIC $$
# MAGIC $$
# MAGIC vx_i = \frac{vx_i-vx_{i-1}}{\Delta{t}}
# MAGIC $$
# MAGIC $$
# MAGIC \frac{d}{dt}col_i = \frac{col_i-col_{i-1}}{\Delta{t}}
# MAGIC $$
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

DO_TEST = False

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG telemetry_analytics_cat;
# MAGIC USE DATABASE main;

# COMMAND ----------

import pyspark.sql.functions as sf
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ##Formula to calculate dx/dt for speed and acceleration

# COMMAND ----------


def add_d_col_dt(df, new_col_name, col, time_col, partition_col):
  """calculates the derivative of <col> d<time_col>"""
  #temporary column names, to be removed later
  delta_t_name = "add_d_col_dt_23049582340958"
  delta_col_name = "add_d_col_dt_777777345344"

  # Add a column for the change in time and col 
  win = Window.partitionBy(partition_col).orderBy(time_col)
  df = df.withColumn(delta_t_name, (time_col - sf.lag(time_col, 1).over(win)))
  df = df.withColumn(delta_col_name, (col - sf.lag(col, 1).over(win)))

  # Compute d col dt
  df = df.withColumn(new_col_name, sf.col(delta_col_name)/sf.col(delta_t_name)) 

  # drop temporary cols before returning
  return df.drop(delta_t_name, delta_col_name)


def add_d_col_fixed_dt(df, new_col_name, col, time_col, part_col, dt=1000.0/60.0):
  """calculates the derivative of <col> d<time_col> using a constant time interval.
     As we are getting sampled data at a known rate, the time interval is known.
     This requires only one window and not two to calcuate the derivative
  """
  return df.withColumn(
    new_col_name,
    (col - sf.lag(col, 1).over(
      Window.partitionBy(part_col).orderBy(time_col)
      )
     )  / dt
  )


# COMMAND ----------

  

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

  # df = add_d_col_dt(df, "x_speed1", sf.col("x"), sf.col("millis"), sf.col("group_name"))
  # df = add_d_col_dt(df, "x_accel1", sf.col("x_speed1"), sf.col("millis"), sf.col("group_name"))

  # df = add_d_col_dt(df, "y_speed1", sf.col("y"), sf.col("millis"), sf.col("group_name"))
  # df = add_d_col_dt(df, "y_accel1", sf.col("y_speed1"), sf.col("millis"), sf.col("group_name"))

  df = add_d_col_fixed_dt(df, "x_speed", sf.col("x"), sf.col("millis"), sf.col("group_name"))
  df = add_d_col_fixed_dt(df, "x_accel", sf.col("x_speed"), sf.col("millis"), sf.col("group_name"))

  df = add_d_col_fixed_dt(df, "y_speed", sf.col("y"), sf.col("millis"), sf.col("group_name"))
  df = add_d_col_fixed_dt(df, "y_accel", sf.col("y_speed"), sf.col("millis"), sf.col("group_name"))

  display(df)

# COMMAND ----------

def calc_vel_accl_jerk_features(feature_df):

  #TODO: can probably use funtools to currie the below?  
  def add_derived_helper(feature_df ,new_col_name , col):
    return add_d_col_fixed_dt(feature_df, new_col_name, col, sf.col("unix_timestamp"), sf.col("group_name"))


  feature_df = add_derived_helper(feature_df, "left_x_vel", sf.col("left_x"))
  feature_df = add_derived_helper(feature_df, "left_y_vel", sf.col("left_y"))

  feature_df = add_derived_helper(feature_df, "left_x_accel", sf.col("left_x_vel"))
  feature_df = add_derived_helper(feature_df, "left_y_accel", sf.col("left_y_vel"))

  feature_df = add_derived_helper(feature_df, "left_x_jerk", sf.col("left_x_accel"))
  feature_df = add_derived_helper(feature_df, "left_y_jerk", sf.col("left_y_accel"))


  cols = [ f"{side}_{coord}_{measure}" 
      for side in ["left"] # add right 
      for coord in ["x","y"]
      for measure in ["vel","accel","jerk"]
      ]

  feature_df = (feature_df
    .groupBy("group_name").agg(
       *( [sf.median(col).alias(f"{col}_median") for col in cols] 
         +[sf.stddev(col).alias(f"{col}_stddev") for col in cols])
     ))
  return feature_df

# COMMAND ----------

feature_df = calc_vel_accl_jerk_features(
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
      .select("group_name","label").distinct()
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

# MAGIC %md ##Try out the rule we defined above

# COMMAND ----------

if (DO_TEST):

    from pyspark.sql.functions import when

    # Join labels_df and features_df on the "group_name" column
    joined_df = labels_df.join(features_df, "group_name")

    # Add new column "label_new" based on the condition of left_y_accel_stddev
    joined_df = (joined_df
        .withColumn("prediction",
            sf.when(joined_df["left_y_accel_stddev"]<=0.0025, "human")
            .otherwise("machine"))
        #.withColumn()
        .select("group_name","left_y_accel_stddev","label","prediction")
        
    )

    display(joined_df)

# COMMAND ----------

# MAGIC %md ##Create Feature table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS telemetry_features;

# COMMAND ----------

(feature_df.write
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


