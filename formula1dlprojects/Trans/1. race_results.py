# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_df = spark.read.format('delta').load(f"{processed_folder_path}/races")\
    .select('race_id', 'race_year', 'name', 'race_timestamp','circuit_id')\
    .withColumnRenamed('name', 'race_name')\
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f"{processed_folder_path}/circuits")\
    .select('location','circuit_id')\
    .withColumnRenamed('location', 'circuit_location')

                                                                             

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f"{processed_folder_path}/driver")\
    .select('name','number','nationality','driver_id')\
    .withColumnRenamed('name', 'driver_name')\
    .withColumnRenamed('number', 'driver_number')\
    .withColumnRenamed('nationality', 'driver_nationality')


# COMMAND ----------

constructors_df = spark.read.format('delta').load(f"{processed_folder_path}/constructors")\
    .select('constructor_id', 'name')\
    .withColumnRenamed('name', 'team')

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder_path}/results")\
    .filter(f"file_date = '{v_file_date}'")\
    .select('result_id', 'race_id', 'driver_id', 'constructor_id', 'grid','fastest_lap','time','points','position','file_date')\
    .withColumnRenamed('time', 'race_time')\
    .withColumnRenamed('file_date', 'result_file_date')


# COMMAND ----------

race_cicuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'inner')\
    .withColumnRenamed('race_id', 'result_race_id')


# COMMAND ----------

race_results_df = results_df.join(race_cicuit_df, results_df.race_id == race_cicuit_df.result_race_id, 'inner')\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, 'inner')\
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, 'inner')

race_results_df = add_insert_date(race_results_df)    

# COMMAND ----------

race_results_df = race_results_df.select(col('result_race_id').alias('race_id'),'race_year','race_name','race_date','circuit_location','driver_name','driver_number','driver_nationality','team','grid','fastest_lap','race_time','points','position','insert_date', col('result_file_date').alias('file_date'))

# COMMAND ----------

# write_to_partition_table(race_results_df, 'f1_presentation', 'race_results', 'result_race_id')

# COMMAND ----------

merge_condition = 't.driver_name = s.driver_name AND t.race_id = s.race_id'
merge_delta_data(race_results_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

