# Databricks notebook source
from pyspark.sql.functions import year, month, dayofmonth,\
    dayofweek,col,date_format,regexp_extract,when,expr,\
    unix_timestamp, from_unixtime,avg,to_timestamp,col, sum, count

# COMMAND ----------

account_name = "elfaquiriacount"
container_name = "mydata"
Access_keys = "fKOvOOt/O/dwhHzRQADXl0AG5/4NjinJ9fB4bEWVOn7xf8CbhAhLr/AbQySNsLIzRFUGcPivt/Vs+AStpFyXqw=="

spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net", 
    f"{Access_keys}"
)

# COMMAND ----------

# get all fishier in processed file :
processed_data = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/processed/")

processed_data = [file.name for file in processed_data]

# csv file in row :
file_list = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/raw/")

# create list of csv :
file_names = [file.name for file in file_list]

# COMMAND ----------

def Cleaning(i,container_name,account_name) :

    # select curent file :
    file_location = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/raw/{file_names[i]}"

    # load data :
    df = spark.read.format("csv").option("inferSchema", "True").option("header",
    "True").option("delimeter",",").load(file_location)

    # Fix data DepartureTime and ArrivalTime:
    df = df.withColumn("DepartureTime", date_format(col("DepartureTime"), "HH:mm"))
    df = df.withColumn("ArrivalTime", date_format(col("ArrivalTime"), "HH:mm"))

    # Fix invalid time values in ArrivalTime column :
    time_pattern = r'^([01][0-9]|2[0-3]):[0-5][0-9]$'

    df = df.withColumn("ArrivalTime", when(~col("ArrivalTime").rlike(time_pattern), "00:00").otherwise(col("ArrivalTime")))

    # Add column day,month,year,day_of_week :
    df = df.withColumn("year", year("Date"))
    df = df.withColumn("month", month("Date"))
    df = df.withColumn("day", dayofmonth("Date"))
    df = df.withColumn("day_of_week", dayofweek("Date"))
    df = df.drop("date")

    # caluculer la duration of time :
    df = df.withColumn("Duration", expr(
        "from_unixtime(unix_timestamp(ArrivalTime, 'HH:mm') - unix_timestamp(DepartureTime, 'HH:mm'), 'HH:mm')"
    ))

    # Catégoriser les retards en fonction de la colonne "Delay" :
    df = df.withColumn("DelayCategory", 
                    when(col("Delay") <= 0, "No Delay")
                    .when((col("Delay") > 0) & (col("Delay") <= 10), "Short Delay")
                    .when((col("Delay") > 10) & (col("Delay") <= 20), "Medium Delay")
                    .otherwise("Long Delay"))

    # Identifier peak and off-peak :
    average_passengers = df.select(avg("Passengers")).first()[0]

    df = df.withColumn("HeureDePointe", when(col("Passengers") > average_passengers, True).otherwise(False))

    # Define the path to save the CSV :
    output_file_location = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/processed/{file_names[i]}"

    # Save the DataFrame as a CSV file to the specified location :
    df.write.csv(output_file_location, header=True, mode="overwrite") 

# COMMAND ----------

i = 0
for fishie_csv in file_names :

    if len(processed_data) == 0 :
        print("prossess")
        Cleaning(i,container_name,account_name)
        break;
    
    if fishie_csv+'/' not in processed_data :
        i = file_names.index(fishie_csv)
        Cleaning(i,container_name,account_name)
        break;

# COMMAND ----------


