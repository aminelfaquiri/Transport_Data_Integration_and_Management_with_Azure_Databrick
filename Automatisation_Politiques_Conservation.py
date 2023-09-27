# Databricks notebook source
from pyspark.sql.functions import year, month, dayofmonth,\
    dayofweek,col,date_format,regexp_extract,when,expr,\
    unix_timestamp, from_unixtime,avg,to_timestamp,col, sum, count

from datetime import datetime

# COMMAND ----------

account_name = "elfaquiriacount"
container_name = "mydata"
Access_keys = "fKOvOOt/O/dwhHzRQADXl0AG5/4NjinJ9fB4bEWVOn7xf8CbhAhLr/AbQySNsLIzRFUGcPivt/Vs+AStpFyXqw=="

spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net", 
    f"{Access_keys}"
)

# COMMAND ----------

# get all fishies in processed :
processed_data = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/processed/")

# get all fishies in row :
row_data = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/raw/")

# show information for les fishies csv dans row :
row_data_info =[(info.name, info.modificationTime) for info in row_data]

# show information for les fishies csv dans processed :
processed_data_info =[(info.name,info.modificationTime) for info in processed_data]


# COMMAND ----------

# archive the file in row : 

for i in row_data_info  :
    timestamp_datetime = datetime.fromtimestamp(i[1] / 1000)

    # Calculate the duration between the two datetime objects
    duration = datetime.now() - timestamp_datetime
    duration_day = duration.days
    print(duration_day)

    if duration_day == 0 :
        # file name :
        filenam = i[0]

        # file path :
        fishier_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/raw/{filenam}"

        #archive folder :
        archive_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/Archive/{filenam}"

        dbutils.fs.cp(fishier_path,archive_path,recurse=True)
