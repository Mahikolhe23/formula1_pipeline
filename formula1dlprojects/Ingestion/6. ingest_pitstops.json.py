# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read the multiline JSON file

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

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.format('json')\
    .option('multiLine', True)\
    .schema(pit_stops_schema)\
    .load(f'{raw_folder_path}/{v_file_date}/pit_stops.json')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename column and add new insert_date column
# MAGIC

# COMMAND ----------

pit_stops_df = pit_stops_df\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))

pit_stops_df = add_insert_date(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write clean data to processed container 

# COMMAND ----------

# Write to table 
# write_to_partition_table(pit_stops_df, 'f1_processed', 'pit_stops')

# COMMAND ----------

merge_condition = 't.race_id = s.race_id AND t.driver_id = s.driver_id AND t.stop = s.stop'
merge_delta_data(pit_stops_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
# MAGIC
# MAGIC --Let's

# COMMAND ----------

