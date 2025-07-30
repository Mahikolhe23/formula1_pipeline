# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def add_insert_date(input_df):
    output_df = input_df.withColumn("insert_date", current_timestamp())
    return output_df

# COMMAND ----------

def set_spark_config():
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

def get_df_column_list(df, partition_column=None):
    if partition_column is None:
        return df.columns
    else:
        column_list = df.columns
        column_list.remove(partition_column)
        column_list.append(partition_column)
        final_df = df.select(column_list)
    return final_df

# COMMAND ----------

def write_to_partition_table(df_name, db_name, table_name, partition_column=None):
    set_spark_config()
    
    if partition_column is not None:
        df_name = get_df_column_list(df_name, partition_column)
        if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
            df_name = df_name.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
        else:
            df_name = df_name.write.mode('overwrite')\
                .partitionBy(f'{partition_column}').format('parquet').saveAsTable(f'{db_name}.{table_name}')
    else:
        if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
            df_name = df_name.write.mode('append').insertInto(f'{db_name}.{table_name}')
        else:
            df_name = df_name.write.mode('overwrite')\
            .format('parquet').saveAsTable(f'{db_name}.{table_name}')
    return df_name             

# COMMAND ----------

def merge_delta_data(df_name, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        delta_table = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
        delta_table.alias("t")\
            .merge(df_name.alias("s"), merge_condition)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()        
    else:
        df_name = df_name.write.mode('overwrite').partitionBy(partition_column)\
            .format('delta').saveAsTable(f'{db_name}.{table_name}')




# COMMAND ----------

