import pyspark.sql.functions as sf
from pyspark.sql.window import Window


class TelemetryFeatureUtils:

  @staticmethod
  def add_d_col_dt(df, new_col_name, col, time_col, part_col, dt=1000.0/60.0):
    """calculates the derivative of <col> d<time_col> using a constant time interval.
      As we are getting sampled data at a known rate, the time interval is known.
      This requires only one window and not two to calcuate the derivative
    """

    win = Window.partitionBy(part_col).orderBy(time_col)

    return df.withColumn(
      new_col_name,
      (col - sf.lag(col, 1).over(win)) / dt
    )

  @staticmethod
  def calc_vel_accl_jerk_features(feature_df):

    #TODO: rewrite with list comprehension, splatter (*) and select  
    def add_derived_helper(feature_df ,new_col_name , col):
      return TelemetryFeatureUtils.add_d_col_dt(feature_df, new_col_name, col, sf.col("unix_timestamp"), sf.col("group_name"))

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
