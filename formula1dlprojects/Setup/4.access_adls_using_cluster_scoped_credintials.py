# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using cluster scoped credentials
# MAGIC 1. Set the spark config fs.szure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuites.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC we have set access key and value in cluster advance option while creating cluster so it will automatically applyy all notebook of that cluster.

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlprojects.dfs.core.windows.net/"))

# COMMAND ----------

df = spark.read.csv('abfss://demo@formula1dlprojects.dfs.core.windows.net/circuits.csv')
df.display()