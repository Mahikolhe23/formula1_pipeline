# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

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

name_schema = StructType(fields=[
  StructField("forename", StringType(), True),
  StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True)
])

# COMMAND ----------

driver_df = spark.read\
    .schema(driver_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename column and add new column

# COMMAND ----------

driver_df = driver_df\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('driverRef', 'driver_ref')\
    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))    

driver_df = add_insert_date(driver_df)
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted column

# COMMAND ----------

driver_df = driver_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 Write clean data to process container

# COMMAND ----------

driver_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.driver')