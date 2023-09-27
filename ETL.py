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

processed_data = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/processed/")

processed_data = [file.name for file in processed_data]

# COMMAND ----------

# csv file :
file_list = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/raw/")

# create list of csv :
file_names = [file.name for file in file_list]

# select curent file :
file_location = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/raw/{file_names[0]}"

# COMMAND ----------

print(file_names)

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)
display(df)

# COMMAND ----------

# MAGIC %md ## Cleanning Data :

# COMMAND ----------

# Calculate the total number of null values in the DataFrame :
null_count = df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).collect()[0]

# Print the null count for each column :
for column, count in null_count.asDict().items():
    print(f"Column '{column}': {count} null values")


# COMMAND ----------

# Fix data DepartureTime :
df = df.withColumn("DepartureTime", date_format(col("DepartureTime"), "HH:mm"))
df = df.withColumn("ArrivalTime", date_format(col("ArrivalTime"), "HH:mm"))
display(df)

# COMMAND ----------

## check value of time :

# Define a regular expression pattern to match valid time values (HH:mm)
time_pattern = r'^([01][0-9]|2[0-3]):[0-5][0-9]$'

# Filter rows with invalid time values in DepartureTime or ArrivalTime columns
invalid_time_rows = df.filter(~(col("DepartureTime").rlike(time_pattern)) | ~(col("ArrivalTime").rlike(time_pattern)))

# Show the DataFrame with rows containing invalid time values
display(invalid_time_rows)


# COMMAND ----------

# MAGIC %md ### Fixe value in ArrivalTime :

# COMMAND ----------

# Fix invalid time values in ArrivalTime column :
df = df.withColumn("ArrivalTime", when(~col("ArrivalTime").rlike(time_pattern), "00:00").otherwise(col("ArrivalTime")))
display(df)

# COMMAND ----------

# MAGIC %md ### ADD Column nesseser :

# COMMAND ----------

### Add column day,month,year,day_of_week :
df = df.withColumn("year", year("Date"))
df = df.withColumn("month", month("Date"))
df = df.withColumn("day", dayofmonth("Date"))
df = df.withColumn("day_of_week", dayofweek("Date"))
df = df.drop("date")
display(df)

# COMMAND ----------

# caluculer la duration of time :
df = df.withColumn("Duration", expr(
    "from_unixtime(unix_timestamp(ArrivalTime, 'HH:mm') - unix_timestamp(DepartureTime, 'HH:mm'), 'HH:mm')"
))
display(df)

# COMMAND ----------

# Catégoriser les retards en fonction de la colonne "Delay"

df = df.withColumn("DelayCategory", 
                   when(col("Delay") <= 0, "No Delay")
                   .when((col("Delay") > 0) & (col("Delay") <= 10), "Short Delay")
                   .when((col("Delay") > 10) & (col("Delay") <= 20), "Medium Delay")
                   .otherwise("Long Delay"))
display(df)


# COMMAND ----------

# MAGIC %md #### Analyse des Passagers :

# COMMAND ----------

# Identifier les heures de pointe et heures hors pointe en fonction du nombre de passagers :
average_passengers = df.select(avg("Passengers")).first()[0]

df = df.withColumn("HeureDePointe", when(col("Passengers") > average_passengers, "peak").otherwise("off-peak"))

# Afficher le DataFrame avec les heures de pointe identifiées :
display(df)

# COMMAND ----------

# MAGIC %md #### Analyse des Itinéraires :

# COMMAND ----------

from pyspark.sql import functions as F

result_df = df.groupBy("Route").agg(
    F.avg("Delay").alias("RetardMoyen"),
    F.avg("Passengers").alias("NombrePassagersMoyen"),
    F.count("*").alias("NombreTotalVoyages")
)

# Afficher le DataFrame résultant :
display(result_df)

# COMMAND ----------

# MAGIC %md #### Save File :

# COMMAND ----------

# Define the path where you want to save the CSV file in Azure Data Lake Storage :
output_file_location = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/processed/{file_names[i]}"

# Save the DataFrame as a CSV file to the specified location :
df.write.csv(output_file_location, header=True, mode="overwrite")

# COMMAND ----------


