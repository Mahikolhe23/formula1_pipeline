# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using Access Keys
# MAGIC 1. Set the spark config fs.szure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuites.csv file

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")


# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get("formula1-scope", "formula1dl-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlprojects.dfs.core.windows.net",formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlprojects.dfs.core.windows.net/"))

# COMMAND ----------

df = spark.read.csv('abfss://demo@formula1dlprojects.dfs.core.windows.net/circuits.csv')

# COMMAND ----------

df.display()