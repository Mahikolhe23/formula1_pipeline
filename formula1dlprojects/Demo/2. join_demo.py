# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

circuites_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

races_circuites_df = circuites_df.join(races_df, circuites_df.circuit_id == races_df.circuit_id, 'inner')
display(races_circuites_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Joins

# COMMAND ----------

races_circuites_df = circuites_df.join(races_df, circuites_df.circuit_id == races_df.circuit_id, 'semi')
display(races_circuites_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Join

# COMMAND ----------

races_circuites_df = circuites_df.join(races_df, circuites_df.circuit_id == races_df.circuit_id, 'anti')
display(races_circuites_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Joins
# MAGIC

# COMMAND ----------

races_circuites_df = circuites_df.join(races_df, circuites_df.circuit_id == races_df.circuit_id, 'cross')
display(races_circuites_df)