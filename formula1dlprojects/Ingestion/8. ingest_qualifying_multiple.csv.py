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

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option("multiline", True)\
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop colummn and add new insert_date column

# COMMAND ----------

qualifying_df = qualifying_df\
    .withColumnRenamed('qualifyId', 'qualify_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))

qualifying_df = add_insert_date(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write CSV file to processed container

# COMMAND ----------

# write_to_partition_table(qualifying_df, 'f1_processed', 'qualifying')

# COMMAND ----------

merge_condition = 't.qualify_id = s.qualify_id'
merge_delta_data(qualifying_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

