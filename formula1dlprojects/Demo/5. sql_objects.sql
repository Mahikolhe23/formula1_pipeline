-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lession Objectives
-- MAGIC 1. Spark SQL docs
-- MAGIC 2. Create DB demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW cmd
-- MAGIC 5. DESCRIBE cmd
-- MAGIC 6. Find the current db

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in demo


-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT current_database()