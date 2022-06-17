# Databricks notebook source
#table definitions from setup
db_name = "geospatial_tomasz"
cell_tower_table = "cell_tower_geojson"
area_codes_table = "area_codes"
phone_numbers_table = "phone_numbers"

#historical tables
RSSI_table = "telco_signal_strength_hist"
CDR_table = "telco_CDR_hist"
PCMD_table = "telco_PCMD_hist"

#silver tables
RSSI_silver_table = "telco_signal_strength_silver_hist"
CDR_silver_table = "telco_CDR_silver_hist"
PCMD_silver_table = "telco_PCMD_silver_hist"

#gold tables
RSSI_gold_table_daily = "telco_signal_strength_gold_hist_daily"
RSSI_gold_table_monthly = "telco_signal_strength_gold_hist_monthly"
CDR_gold_table_daily = "telco_CDR_gold_hist_daily"
CDR_gold_table_monthly = "telco_CDR_gold_hist_monthly"
PCMD_gold_table_daily = "telco_PCMD_gold_hist_daily"
PCMD_gold_table_monthly = "telco_PCMD_gold_hist_monthly"

# COMMAND ----------

df_test = spark.sql("select * from geospatial_tomasz.telco_CDR_gold_hist_daily")

# COMMAND ----------

df_test.schema

# COMMAND ----------

#get static tower data
df_towers = spark.sql("select * from {0}.{1}".format(db_name, cell_tower_table))

# COMMAND ----------

#RSSI data silver layer creation
#read bronze RSSI table
df_rssi_bronze = spark.sql("select * from {}.{}".format(db_name, RSSI_table))

#join table with tower data
df_rssi_w_towers = df_rssi_bronze.join(df_towers, df_rssi_bronze.towerId == df_towers.properties.GlobalID)

#save to silver table
df_rssi_w_towers.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, RSSI_silver_table))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from geospatial_tomasz.telco_CDR_silver_hist

# COMMAND ----------

import pyspark.sql.functions as F
#CDR data silver layer creation
#read bronze RSSI table
df_cdr_bronze = spark.sql("select * from {}.{}".format(db_name, CDR_table))

#rename type column
df_cdr_bronze_type_change = df_cdr_bronze.withColumnRenamed("type", "rtype")

#join table with tower data
df_cdr_w_towers = df_cdr_bronze_type_change.join(df_towers, df_cdr_bronze_type_change.towerId == df_towers.properties.GlobalID)

#save to silver table
df_cdr_w_towers.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, CDR_silver_table))

# COMMAND ----------

#PCMD data silver layer creation
#read bronze RSSI table
df_pcmd_bronze = spark.sql("select * from {}.{}".format(db_name, PCMD_table))

#join table with tower data
df_pcmd_w_towers = df_pcmd_bronze.join(df_towers, df_pcmd_bronze.towerId == df_towers.properties.GlobalID)

#save to silver table
df_pcmd_w_towers.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, PCMD_silver_table))

# COMMAND ----------

#create gold table as a summary of metrics by tower
#daily table
df_cdr_pivot_on_status = df_cdr_w_towers.groupBy(F.to_date("event_ts").alias("date"), "towerId").pivot("status").count()
df_cdr_pivot_on_rtype = df_cdr_w_towers.groupBy(F.to_date("event_ts").alias("date"), "towerId").pivot("rtype").count()

df_cdr_full_pivot = df_cdr_pivot_on_status.join(df_cdr_pivot_on_rtype.alias("df2"), (df_cdr_pivot_on_status.towerId == df_cdr_pivot_on_rtype.towerId) & (df_cdr_pivot_on_status.date == df_cdr_pivot_on_rtype.date)).drop(F.col("df2.date")).drop(F.col("df2.towerId"))
#monthly table
df_cdr_full_pivot_clean = df_cdr_full_pivot.na.fill(0).withColumn("totalRecords", (F.col("null") + F.col("answered") + F.col("dropped") + F.col("missed"))).drop("null").withColumn("dayofweek", F.dayofweek(F.col("date")))

#join back tower info
df_cdr_full_pivot_clean_w_tower = df_cdr_full_pivot_clean.join(df_towers, df_cdr_full_pivot_clean.towerId == df_towers.properties.GlobalID)
display(df_cdr_full_pivot_clean_w_tower)

#create hourly gold CDR table
df_cdr_full_pivot_clean_w_tower.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, CDR_gold_table_daily))

# COMMAND ----------

#monthly table
df_cdr_full_pivot_clean_monthly = df_cdr_full_pivot_clean.groupBy("towerId", F.trunc(F.col("date"), "month").alias("month")).agg(F.sum("answered").alias("answered"), F.sum("dropped").alias("dropped"), F.sum("missed").alias("missed"), F.sum("call").alias("call"), F.sum("text").alias("text"), F.sum("totalRecords").alias("totalRecords"))
print(df_cdr_full_pivot_clean_monthly.count())

#join back tower info
df_cdr_full_pivot_clean_monthly_w_tower = df_cdr_full_pivot_clean_monthly.join(df_towers, df_cdr_full_pivot_clean_monthly.towerId == df_towers.properties.GlobalID)
print(df_cdr_full_pivot_clean_monthly_w_tower.count())

df_cdr_full_pivot_clean_monthly_w_tower.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, CDR_gold_table_monthly))
