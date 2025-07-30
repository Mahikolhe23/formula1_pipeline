# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

demo_df = race_results_df.filter('race_year in (2019,2020)')

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points'), countDistinct('race_name'))\
    .withColumnRenamed('sum(points)', 'total_points')\
    .withColumnRenamed('count(DISTINCT race_name)','number_of_races')
    

# COMMAND ----------

demo_group_df = demo_df\
    .groupBy('race_year','driver_name')\
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races'))

# COMMAND ----------

driver_rank_window = Window.partitionBy('race_year').orderBy(desc('total_points'))
demo_group_df.withColumn('rnk',rank().over(driver_rank_window))

# COMMAND ----------

