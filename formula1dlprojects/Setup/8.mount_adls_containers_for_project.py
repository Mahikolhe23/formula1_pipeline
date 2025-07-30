# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from key vault
    client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-id')
    tennant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-tennant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-client-secret')
    
    # Set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tennant_id}/oauth2/token"}
    
    # Unmount if already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
          dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")      

    # Mount the storage account
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    # Display all mounts  
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw Container
# MAGIC

# COMMAND ----------

mount_adls('formula1dlprojects', 'raw')

# COMMAND ----------

mount_adls('formula1dlprojects', 'processed')

# COMMAND ----------

mount_adls('formula1dlprojects', 'presentation')

# COMMAND ----------

mount_adls('formula1dlprojects', 'demo')

# COMMAND ----------

