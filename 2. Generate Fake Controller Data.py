# Databricks notebook source
# MAGIC %md #Generating Fake Controller Data

# COMMAND ----------

import pyspark.sql.functions as sf
from pyspark.sql import column
from datetime import datetime
import random 
import time

# COMMAND ----------

# TODO extract settings into a proper config file, to make it easier to find
num_files_to_generate = 100 #AutoML requires at least 5 samples per category
ratio_of_machine_files = 0.3
average_millis_between_file_generation = 0
landing_path = '/Volumes/telemetry_analytics_cat/main/landing/'
average_num_lines_per_file = 1000
noise_scale = 0.1

# COMMAND ----------

def digitize_sf(x: column) -> column:
  return sf.round(x, 1)

def noiser_sf(clean: column, scale: float) -> column:
  return clean + (sf.rand()-sf.lit(0.5)*sf.lit(scale))

file_num = 0

while (num_files_to_generate < 0 or file_num < num_files_to_generate):

    current_time = datetime.now()
    current_time_string = current_time.strftime("%Y-%m-%d_%H:%M:%S.%f")

    seconds_to_sleep = 0
    if (average_millis_between_file_generation > 0):
      seconds_to_sleep = random.gauss(
        average_millis_between_file_generation, 
        average_millis_between_file_generation/5) / 1000.0

    generate_a_machine_file = random.uniform(0, 1) < ratio_of_machine_files

    # Convert datetime object to Unix timestamp in milliseconds
    unix_timestamp = int(current_time.timestamp() * 1000)

    filename = f"{unix_timestamp}_{'machine' if generate_a_machine_file else 'human'}.csv.gz"
    file_path = f"{landing_path}{filename}"
    num_lines = average_num_lines_per_file+1+round( 
        (random.uniform(0, 1)-0.5)*0.1*average_num_lines_per_file 
    )

    print(f"{current_time_string} generating file {file_path} machine_generated={generate_a_machine_file}, then sleeping for {seconds_to_sleep} , num_lines:{num_lines}")

    timestamp_and_degree_df = (spark
      .range(1,num_lines+1)
      .withColumn("unix_timestamp", 
                  sf.round(sf.col("id")*1000/60 + sf.unix_millis(sf.now())) )
      .withColumn("degree", sf.col("unix_timestamp") / sf.lit(300))
      .select(["unix_timestamp", "degree"])
    )


    df = (timestamp_and_degree_df
          .withColumn("left_x",  sf.sin(noiser_sf(sf.col("degree"),noise_scale)))
          .withColumn("left_y",  sf.cos(noiser_sf(sf.col("degree"),noise_scale)))
          .withColumn("right_x", sf.sin(noiser_sf(sf.col("degree"),noise_scale)))
          .withColumn("right_y", sf.cos(noiser_sf(sf.col("degree"),noise_scale))) 
    )

    if generate_a_machine_file:
      df = (df
            .withColumn("left_x", noiser_sf(digitize_sf(sf.col("left_x")),noise_scale))
            .withColumn("left_y", noiser_sf(digitize_sf(sf.col("left_y")),noise_scale))
            .withColumn("right_x", noiser_sf(digitize_sf(sf.col("right_x")),noise_scale))
            .withColumn("right_y", noiser_sf(digitize_sf(sf.col("right_y")),noise_scale))
      )
  

    (df
     .select(["unix_timestamp","left_x","left_y","right_x","right_y"])
     .toPandas()
     .to_csv(file_path, index=False, compression='gzip'))
   
    # TODO calculate time remaining to sleep, after time it took to generate the file
    time.sleep(seconds_to_sleep)
    file_num += 1

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /Volumes/telemetry_analytics_cat/main/landing/
# MAGIC
# MAGIC FILE_PATH=$(ls /Volumes/telemetry_analytics_cat/main/landing/*.gz | head -n1)
# MAGIC
# MAGIC echo $FILE_PATH
# MAGIC gunzip -c $FILE_PATH | head

# COMMAND ----------


