# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_result_df = spark.read.format('delta').load(f'{presentation_folder_path}/race_results')\
    .filter('file_date = "{v_file_date}"')

# COMMAND ----------

constructor_standing_df = race_result_df\
    .groupBy('race_year', 'team')\
    .agg(sum('points').alias('total_points'),
         count(when(col('position') == 1, True)).alias('wins')
         )

# COMMAND ----------

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
constructor_standing_df = constructor_standing_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

# constructor_standing_df.write.mode('overwrite').format('parquet').saveAsTable("f1_presentation.constructor_standing")

# COMMAND ----------

merge_condition = 't.team = s.team AND t.race_year = s.race_year'
merge_delta_data(constructor_standing_df, 'f1_presentation', 'constructor_standing', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standing

# COMMAND ----------

