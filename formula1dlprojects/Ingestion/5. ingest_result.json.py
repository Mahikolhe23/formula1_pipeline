# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using Dataframe API

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

result_schema = StructType(fields=[
    StructField('resultId', StringType(), True),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('laps', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', FloatType(), True),
    StructField('statusId', StringType(), True)
])

# COMMAND ----------

result_df = spark.read.format('json')\
    .schema(result_schema)\
    .load(f'{raw_folder_path}/{v_file_date}/results.json')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename column and add new insert_date column

# COMMAND ----------

result_df = result_df.withColumnRenamed('resultId','result_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('positionText', 'position_text')\
    .withColumnRenamed('positionOrder', 'position_order')\
    .withColumnRenamed('fastestLap', 'fastest_lap')\
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))\
    .withColumn('insert_date', current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted column

# COMMAND ----------

result_df = result_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC Drop duplicates records

# COMMAND ----------

result_df = result_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to parquet file
# MAGIC ##### Method 1 for incremental Load
# MAGIC In this method first we are finding the partition number and drop that partition and then insert the fresh data for that partition in append mode

# COMMAND ----------

# for race_id_list in result_df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# result_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2 for increamental Load
# MAGIC In this incremental mode first we are creating table if not exists and make it as partition by column provided, and for next onward incremental load we are just inserting data directly overwrite to that partition wihout droppping, so first we have to check and then insert append data. along with data we also need to provide partition column at last in df, means that partition column must be last while creating table using df.
# MAGIC

# COMMAND ----------

# Getting partition table and overwrite only partition
# write_to_partition_table(result_df, 'f1_processed', 'results', 'race_id')


# COMMAND ----------

merge_condition = 't.result_id = s.result_id AND t.race_id = s.race_id'
merge_delta_data(result_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

