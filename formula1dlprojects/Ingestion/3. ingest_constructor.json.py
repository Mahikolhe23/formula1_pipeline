# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1 Read JSON File using DF Reader API
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

contructor_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructor_df = spark.read.format('json')\
    .schema(contructor_schema)\
    .load(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

constructor_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 Rename column and insert date column add

# COMMAND ----------

constructor_df = constructor_df\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef', 'constructor_ref')\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))\

constructor_df = add_insert_date(constructor_df)    

# COMMAND ----------

constructor_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')