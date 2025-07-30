# Databricks notebook source
# MAGIC %run "../Includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

race_result_df = spark.read.format('delta').load(f'{presentation_folder_path}/race_results')\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

driver_standing_df = race_result_df\
    .groupBy('race_year', 'driver_name', 'driver_nationality')\
    .agg(sum('points').alias('total_points'),
         count(when(col('position') == 1, True)).alias('wins')
         )

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
driver_ranked_df = driver_standing_df.withColumn('rank', rank().over(driver_rank_spec))
# driver_ranked_df = driver_ranked_df.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# driver_ranked_df.write.mode('overwrite').format('delta').saveAsTable('f1_presentation.driver_rank')

# COMMAND ----------

merge_condition = 't.driver_name = s.driver_name AND t.race_year = s.race_year'
merge_delta_data(driver_ranked_df, 'f1_presentation', 'driver_rank', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_presentation.driver_rank

# COMMAND ----------

