# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1 Read Data using DataFrameReader API

# COMMAND ----------

# MAGIC %run "../Includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('year', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('circuitId', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('date', DateType(), True),
    StructField('time', StringType(), True),
    StructField('url', StringType(), True)
])


# COMMAND ----------

races_df = spark.read.format('csv')\
    .schema(races_schema)\
    .option('header','true')\
    .load(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 Merge Date and time to make it timestamp and also add insert_date column

# COMMAND ----------

races_df = add_insert_date(races_df)
races_df = races_df\
    .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 Select only Required Column
# MAGIC

# COMMAND ----------

races_df = races_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('insert_date'), col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write data to Processed Layer

# COMMAND ----------

races_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

