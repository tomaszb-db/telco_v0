# Databricks notebook source
# MAGIC %md
# MAGIC ## Telecommunications Reliability Metrics
# MAGIC 
# MAGIC **Use Case Overview**
# MAGIC 
# MAGIC Different types of metrics are collected by telecommunications companies to ultimately determine the quality of service for their customers as well as to assess the health of their network. The metrics that can be used are Call Detail Records (CDR), Per Call Measurement Data (PCMD), and Signal Strength measurements (RSSI). 
# MAGIC 
# MAGIC ![test](files/Users/tomasz.bacewicz@databricks.com/Screen_Shot_2022_06_12_at_2_09_00_PM.png)

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

#table location definitions
db_name = "geospatial_tomasz"
cell_tower_table = "cell_tower_geojson"

#locations of data streams
RSSI_dir = "dbfs:/tmp/tomasz.bacewicz@databricks.com/telco_signal_strength"
CDR_dir = "dbfs:/tmp/tomasz.bacewicz@databricks.com/telco_CDR"

RSSI_schema = "dbfs:/tmp/tomasz.bacewicz@databricks.com/RSSI_schema/"
CDR_schema = "dbfs:/tmp/tomasz.bacewicz@databricks.com/CDR_schema/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Using Autoloader and Creating the Bronze Layer
# MAGIC 
# MAGIC ![test](files/Users/tomasz.bacewicz@databricks.com/Screen_Shot_2022_06_12_at_2_09_07_PM.png)

# COMMAND ----------

@dlt.table(comment="RSSI Stream - Bronze")
def rssi_stream_bronze():
  return spark.readStream.format("cloudFiles")  \
          .option("cloudFiles.format", 'json')  \
          .option('header', 'false') \
          .option("cloudFiles.maxFilesPerTrigger", 1)  \
          .option("mergeSchema", "true")         \
          .option("cloudFiles.inferColumnTypes", "true") \
          .option("cloudFiles.schemaLocation", RSSI_schema) \
          .load(RSSI_dir)

# COMMAND ----------

@dlt.table(comment="CDR Stream - Bronze")
def cdr_stream_bronze():
  return spark.readStream.format("cloudFiles")  \
                          .option("cloudFiles.format", 'json') \
                          .option('header', 'false')  \
                          .option("cloudFiles.maxFilesPerTrigger", 1) \
                          .option("mergeSchema", "true")         \
                          .option("cloudFiles.inferColumnTypes", "true") \
                          .option("cloudFiles.schemaLocation", CDR_schema) \
                          .load(CDR_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining with Tower Data and Creating the Silver Layer 
# MAGIC 
# MAGIC ![test](files/Users/tomasz.bacewicz@databricks.com/Screen_Shot_2022_06_12_at_2_09_23_PM.png)

# COMMAND ----------

@dlt.table(comment="RSSI Stream - Silver (Tower Info Added)")
def rssi_stream_silver():
  #get static tower data
  df_towers = spark.sql("select * from {0}.{1}".format(db_name, cell_tower_table))
  
  df_rssi_bronze = dlt.read_stream("rssi_stream_bronze")
  
  df_rssi_bronze_w_tower = df_rssi_bronze.join(df_towers, df_rssi_bronze.towerId == df_towers.properties.GlobalID)
  
  return df_rssi_bronze.join(df_towers, df_rssi_bronze.towerId == df_towers.properties.GlobalID)

# COMMAND ----------

@dlt.table(comment="CDR Stream - Silver (Tower Info Added)")
def cdr_stream_silver():
  #get static tower data
  df_towers = spark.sql("select * from {0}.{1}".format(db_name, cell_tower_table))
  
  df_cdr_bronze = dlt.read_stream("cdr_stream_bronze")
  df_cdr_bronze_col_rename = df_cdr_bronze.withColumn("typeC", F.col("type")).drop("type")
  return df_cdr_bronze_col_rename.join(df_towers, df_cdr_bronze_col_rename.towerId == df_towers.properties.GlobalID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregating on Daily and Monthly Time Periods by Tower to Create the Gold Layer
# MAGIC 
# MAGIC ![test](files/Users/tomasz.bacewicz@databricks.com/Screen_Shot_2022_06_12_at_2_09_38_PM.png)

# COMMAND ----------

@dlt.view
def static_tower_data():
  df_towers = spark.sql("select * from {0}.{1}".format(db_name, cell_tower_table))
  
  return df_towers.select(df_towers.properties.GlobalID.alias("GlobalID"), df_towers.properties.LocCity.alias("City"), df_towers.properties.LocCounty.alias("County"), df_towers.properties.LocState.alias("State"), df_towers.geometry.coordinates[0].alias("Longitude"), df_towers.geometry.coordinates[1].alias("Latitude"))

# COMMAND ----------

@dlt.table(comment="Aggregate Tower Stream - Gold (by Day)")
def tower_stream_day_gold():
  #df_rssi_silver = dlt.read_stream("rssi_stream_silver")
  df_cdr_silver = dlt.read_stream("cdr_stream_silver")
  
  #re-add tower data
  df_towers = dlt.read("static_tower_data")
  
  #df_cdr_pivot_on_status = df_cdr_silver.groupBy(F.window("event_ts", "1 day"), "towerId", "status", "type").count()
  df_cdr_pivot_on_status_grouped_tower = df_cdr_silver.groupBy(F.window("event_ts", "1 day"), "towerId").agg(F.count(F.when(F.col("status") == "dropped", True)).alias("dropped"), F.count(F.when(F.col("status") == "answered", True)).alias("answered"), F.count(F.when(F.col("status") == "missed", True)).alias("missed"), F.count(F.when(F.col("type") == "text", True)).alias("text"), F.count(F.when(F.col("type") == "call", True)).alias("call"), F.count(F.lit(1)).alias("totalRecords_CDR"), F.first("window.start").alias("window_start")).withColumn("date", F.to_date("window_start")).drop("window_start").drop("window")
  
  
  #df_full_cdr = df_cdr_pivot_on_status.join(df_cdr_pivot_on_rtype.select("towerId", "call", "text"), ["towerId"]).withColumn("date", F.date("event_ts")).drop("event_ts")
  df_full_cdr_w_tower_properties = df_cdr_pivot_on_status_grouped_tower.join(df_towers, df_cdr_pivot_on_status_grouped_tower.towerId == df_towers.GlobalID).drop("GlobalID")
  #df_full_cdr_w_tower_properties = df_cdr_pivot_on_status.join(df_towers, df_cdr_pivot_on_status.towerId == df_towers.GlobalID).drop("GlobalID")
  df_full_cdr_w_tower_properties_with_null = df_full_cdr_w_tower_properties.withColumn("avg_RSRP", F.lit(None).cast(IntegerType())) \
                                                                                        .withColumn("avg_RSRQ", F.lit(None).cast(IntegerType())) \
                                                                                        .withColumn("avg_SINR", F.lit(None).cast(IntegerType()))
  
  df_cdr_pivot_on_status_grouped_tower_ordered = df_full_cdr_w_tower_properties_with_null.select("date", "towerId", "answered", "dropped", "missed", "call", "text", "totalRecords_CDR", "avg_RSRP", "avg_RSRQ", "avg_SINR", "Latitude", "Longitude", "City", "County", "State")
  
  return df_cdr_pivot_on_status_grouped_tower_ordered
  
  #df_rssi_silver_agg_day = df_rssi_silver.groupBy(F.to_date("event_ts").alias("date"), "towerId").agg(F.avg("RSRP").alias("avg_RSRP"), F.avg("RSRQ").alias("avg_RSRQ"), F.avg("SINR").alias("avg_SINR"))
  
  #df_daily_tower_kpi = df_cdr_silver.join(df_rssi_silver, ["towerId", "date"], "full")
  
  #df_daily_tower_kpi_w_properties = df_daily_tower_kpi.join(df_towers, "towerId" == "properties.GlobalID")
  
  #return df_cdr_silver

# COMMAND ----------

tower_kpis_daily_gold_schema = StructType([ 
                                           StructField("date", DateType(), False),
                                           StructField("towerId", StringType(), False),
                                           StructField("answered", LongType(), True),
                                           StructField("dropped", LongType(), True),
                                           StructField("missed", LongType(), True),
                                           StructField("call", LongType(), True),
                                           StructField("text", LongType(), True),
                                           StructField("totalRecords_CDR", LongType(), True),
                                         #  StructField("dayofweek", IntegerType(), True),
                                           StructField("avg_RSRP", IntegerType(), True),
                                           StructField("avg_RSRQ", IntegerType(), True),
                                           StructField("avg_SINR", IntegerType(), True),
                                           StructField("Latitude", DoubleType(), False),
                                           StructField("Longitude", DoubleType(), False),
                                           StructField("City", StringType(), True),
                                           StructField("County", StringType(), True),
                                           StructField("State", StringType(), True)
])

# COMMAND ----------

dlt.create_streaming_live_table("tower_kpis_daily_gold_test", comment="Aggregate Tower Stream - Gold (by Day)", partition_cols=["date"], schema=tower_kpis_daily_gold_schema)

# COMMAND ----------

dlt.apply_changes(target="tower_kpis_daily_gold_test", source="tower_stream_day_gold", keys=["date", "towerId"], sequence_by="date")

# COMMAND ----------


