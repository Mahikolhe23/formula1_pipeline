# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using dataframe Reader API

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

from pyspark.sql.types import *
from pyspark.sql.functions import * 

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField('circuitId', IntegerType(), True),
    StructField('circuitRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('location', StringType(), True),
    StructField('country', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('lng', DoubleType(), True),
    StructField('alt', IntegerType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.format('csv')\
    .option('header', True)\
    .schema(circuits_schema)\
    .load(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 Select only requied column and rename column as required

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed('circuitId', 'circuit_id')\
    .withColumnRenamed('circuitRef','circuit_reference')\
    .withColumnRenamed('lat','latitude')\
    .withColumnRenamed('lng','longitude')\
    .withColumnRenamed('alt','altitude')\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('data_source', lit(v_file_date))

circuits_df = circuits_df.drop('url')

# COMMAND ----------

circuits_df = add_insert_date(circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write data to parquet

# COMMAND ----------

circuits_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')