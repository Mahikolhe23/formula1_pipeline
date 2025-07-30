# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dlprojects/demo'

# COMMAND ----------

result_df = spark.read\
    .option('inferSchema', True)\
    .json('/mnt/formula1dlprojects/raw/2021-03-28/results.json')

# COMMAND ----------

result_df.write.format('delta').mode('overwrite').saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed WHERE position > 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### History & versionning
# MAGIC #### Time travel
# MAGIC #### Vacuum
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed VERSION AS OF 1

# COMMAND ----------

# MAGIC   %sql
# MAGIC   MERGE INTO f1_demo.results_managed tgt
# MAGIC   USING f1_demo.results_managed VERSION AS OF 1 src
# MAGIC   ON tgt.resultId = src.resultId
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction Logs

# COMMAND ----------

