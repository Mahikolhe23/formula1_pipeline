# Databricks notebook source
v_result = dbutils.notebook.run("1. ingest_circuits.csv", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("2. ingest_races.csv", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("3. ingest_constructor.json", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("4. ingest_driver.json", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("5. ingest_result.json", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("6. ingest_pitstops.json", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("7. ingest_laptimes_multiple.csv", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("8. ingest_qualifying_multiple.csv", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC