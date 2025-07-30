-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Drop Databases

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed 
LOCATION '/mnt/formula1dlprojects/processed'

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/formula1dlprojects/presentation'

-- COMMAND ----------

show databases

-- COMMAND ----------

