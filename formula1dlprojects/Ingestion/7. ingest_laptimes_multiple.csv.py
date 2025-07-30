# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read Multiple CSV file From Folder

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_time_df = spark.read\
    .schema(lap_times_schema)\
    .option("multiline", True)\
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop colummn and add new insert_date column

# COMMAND ----------

lap_time_df = lap_time_df\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))        

lap_time_df = add_insert_date(lap_time_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write CSV file to processed container

# COMMAND ----------

# Write data to table
# write_to_partition_table(lap_time_df,'f1_processed','laptimes')

# COMMAND ----------

merge_condition = 't.race_id = s.race_id AND t.driver_id = s.driver_id AND t.lap = s.lap'
merge_delta_data(lap_time_df, 'f1_processed', 'laptimes', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.laptimes
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
# MAGIC
# MAGIC --Let's