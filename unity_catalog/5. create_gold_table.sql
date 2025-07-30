-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create manages table in the gold table
-- MAGIC join drivers and result to identify the number of wins per driver

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.gold.driver_wins;

CREATE TABLE IF NOT EXISTS formula1_dev.gold.driver_wins
AS 
SELECT d.full_name, COUNT(1) AS wins
FROM formula1_dev.silver.drivers d
INNER JOIN formula1_dev.silver.results r ON d.driver_id = r.driver_id
WHERE position = 1
GROUP BY d.full_name


-- COMMAND ----------

SELECT * 
FROM formula1_dev.gold.driver_wins

-- COMMAND ----------

SELECT * FROM system.INFORMATION_SCHEMA.TABLES

-- COMMAND ----------

