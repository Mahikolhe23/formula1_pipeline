-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dominant Driver

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) as total_races, 
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC


-- COMMAND ----------

WITH CTE AS (
  SELECT driver_name,
        COUNT(1) as total_races, 
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points,
        RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC
)
SELECT race_year,
       driver_name,
       COUNT(1) as total_races, 
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM CTE WHERE driver_rank <= 10)
GROUP BY driver_name, race_year
ORDER BY race_year, total_points DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dominant Team

-- COMMAND ----------

SELECT team_name,
       COUNT(1) as total_races, 
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC


-- COMMAND ----------

WITH CTE AS (
  SELECT team_name,
        COUNT(1) as total_races, 
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points,
        RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC
)
SELECT race_year,
       team_name,
       COUNT(1) as total_races, 
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM CTE WHERE team_rank <= 10)
GROUP BY team_name, race_year
ORDER BY race_year, total_points DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:black;text-align:center; font-family:Arial">Report on Dominant Formula 1 Drivers!</h1>"""
-- MAGIC displayHTML(html)