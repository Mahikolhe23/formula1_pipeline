# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using Service Principal
# MAGIC 1. Register Azure AD Application/Service Principal
# MAGIC 2. Generate Secret/Password for the application
# MAGIC 3. Set Spark config with App/Cliend Id, Directory/Tennant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the data lake

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------



# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-id')
tennant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-tennant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlprojects.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlprojects.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlprojects.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlprojects.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlprojects.dfs.core.windows.net", f"https://login.microsoftonline.com/{tennant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlprojects.dfs.core.windows.net/"))

# COMMAND ----------

df = spark.read.csv('abfss://demo@formula1dlprojects.dfs.core.windows.net/circuits.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

