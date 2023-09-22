# Databricks notebook source
spark.conf.set(
    f"fs.azure.account.key.spaicker.dfs.core.windows.net", 
    "z7/JXE3a7KBWMKgkxbkD4SRVg2Teb2BuGoEao866FtWuzgFSTQ3SIIs1p71dQBLeMCkTzKVsdOFN+AStgBP1Kg=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://spaickerlake@spaicker.dfs.core.windows.net/public_transport_data/raw/")

# COMMAND ----------

file_location = "abfss://spaickerlake@spaicker.dfs.core.windows.net/public_transport_data/raw/public-transport-data.csv"

df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)

display(df)
