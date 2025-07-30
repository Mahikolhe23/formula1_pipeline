# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_df.filter("race_year = 2019").show()

# COMMAND ----------

