# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results(
        race_year INT,
        team_name STRING,
        driver_id INT,
        driver_name STRING,
        race_id INT,
        position INT,
        points INT,
        calculated_point INT,
        created_date TIMESTAMP,
        updated_date TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW race_results_updated
AS
SELECT rc.race_year, 
       c.name as team_name, 
       d.driver_id as driver_id,
       d.name AS driver_name,
       rc.race_id,
       r.position, 
       r.points,
       11 - r.position as calculated_points
  FROM f1_processed.results r
  JOIN f1_processed.driver d ON r.driver_id = d.driver_id
  JOIN f1_processed.constructors c ON r.constructor_id = c.constructor_id
  JOIN f1_processed.races rc ON rc.race_id = r.race_id
WHERE r.position <= 10
  AND r.file_date = '{v_file_date}'
""")  

# COMMAND ----------

# MAGIC   %sql
# MAGIC   MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC   USING race_results_updated upd
# MAGIC   ON (tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id)
# MAGIC   WHEN MATCHED THEN
# MAGIC     UPDATE SET tgt.position = upd.position,
# MAGIC                tgt.points = upd.points,
# MAGIC                tgt.calculated_point = upd.calculated_points,
# MAGIC                tgt.updated_date = current_timestamp
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT(race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_point, created_date)
# MAGIC          VALUES(upd.race_year, upd.team_name, upd.driver_id, upd.driver_name, upd.race_id, upd.position, upd.points, upd.calculated_points, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results