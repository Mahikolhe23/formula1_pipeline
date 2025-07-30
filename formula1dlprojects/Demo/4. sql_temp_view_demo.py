# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframe using SQL
# MAGIC #### Objectives
# MAGIC 1. Create temp views on df
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

race_result_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

display(spark.sql('SELECT * FROM v_race_results WHERE race_year = 2020'))

# COMMAND ----------

