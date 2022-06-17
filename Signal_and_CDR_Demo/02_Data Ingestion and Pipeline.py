# Databricks notebook source
#imports
import pyspark.sql.functions as F

#table location definitions
db_name = "geospatial_tomasz"
cell_tower_table = "cell_tower_geojson"

# COMMAND ----------

#locations of data streams
RSSI_dir = "/tmp/tomasz.bacewicz@databricks.com/telco_signal_strength"
CDR_dir = "/tmp/tomasz.bacewicz@databricks.com/telco_CDR"

RSSI_schema = "/tmp/tomasz.bacewicz@databricks.com/RSSI_schema/"
CDR_schema = "/tmp/tomasz.bacewicz@databricks.com/CDR_schema/"

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/tomasz.bacewicz@databricks.com/telco_signal_strength/0.json/

# COMMAND ----------

#ingest RSSI data
df_rssi = spark.readStream.format("cloudFiles") \
                          .option("cloudFiles.format", 'json') \
                          .option('header', 'false') \
                          .option("cloudFiles.maxFilesPerTrigger", 1) \
                          .option("mergeSchema", "true")         \
                          .option("cloudFiles.inferColumnTypes", "true") \
                          .option("cloudFiles.schemaLocation", RSSI_schema) \
                          .load(RSSI_dir)

# COMMAND ----------

display(df_rssi)

# COMMAND ----------

#ingest CDR Data
df_cdr = spark.readStream.format("cloudFiles") \
                          .option("cloudFiles.format", 'json') \
                          .option('header', 'false') \
                          .option("cloudFiles.maxFilesPerTrigger", 1) \
                          .option("mergeSchema", "true")         \
                          .option("cloudFiles.inferColumnTypes", "true") \
                          .option("cloudFiles.schemaLocation", CDR_schema) \
                          .load(CDR_dir)

# COMMAND ----------

display(df_cdr)

# COMMAND ----------

#get static tower data
df_towers = spark.sql("select * from {0}.{1}".format(db_name, cell_tower_table))

# COMMAND ----------

df_rssi_with_tower = df_rssi.join(df_towers, df_rssi.towerId == df_towers.properties.GlobalID)

# COMMAND ----------

display(df_rssi_with_tower)

# COMMAND ----------

df_cdr_with_tower = df_cdr.join(df_towers, df_cdr.towerId == df_towers.properties.GlobalID)

# COMMAND ----------


