-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create Catalogs and Schemas required for the project
-- MAGIC   1. Catalog - formula1_dev (without managed location)
-- MAGIC   2. Schemas - bronze, silver, gold (with managed location)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_dev
MANAGED LOCATION 'abfss://meta-store@databricksucadlsdl.dfs.core.windows.net/'

-- COMMAND ----------

USE CATALOG formula1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze 
MANAGED LOCATION 'abfss://bronze@databricksadlsexternal.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver 
MANAGED LOCATION 'abfss://silver@databricksadlsexternal.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold 
MANAGED LOCATION 'abfss://gold@databricksadlsexternal.dfs.core.windows.net/'

-- COMMAND ----------

