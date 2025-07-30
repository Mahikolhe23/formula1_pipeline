-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create External Location required for this projects
-- MAGIC 1. Bronze
-- MAGIC 2. Silver
-- MAGIC 3. Gold

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricksadlsexternal_bronze
URL 'abfss://bronze@databricksadlsexternal.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickcource-ext-storage-cred`)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricksadlsexternal_silver
URL 'abfss://silver@databricksadlsexternal.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickcource-ext-storage-cred`)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricksadlsexternal_gold
URL 'abfss://gold@databricksadlsexternal.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickcource-ext-storage-cred`)