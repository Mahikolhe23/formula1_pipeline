# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using SAS Token Key
# MAGIC 1. Set the spark config SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuites.csv file

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

formula1dl_sas_token = dbutils.secrets.get(scope='formula1-scope', key='formula1-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlprojects.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlprojects.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlprojects.dfs.core.windows.net", formula1dl_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlprojects.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load('abfss://demo@formula1dlprojects.dfs.core.windows.net/circuits.csv')
display(df)    

# COMMAND ----------

