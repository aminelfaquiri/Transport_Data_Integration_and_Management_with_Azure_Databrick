# Databricks notebook source
from pyspark.sql.functions import year, month, dayofmonth,\
 dayofweek,col,date_format,regexp_extract,when,expr,\
 unix_timestamp, from_unixtime,avg,to_timestamp,col, sum, count

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.elfaquirlake.dfs.core.windows.net", 
    "8NIr8rOoeJMTSeKCs+rNqt7rstck9ktX7dmFwpne12GEefhb+AzeC7OVzmLeJbZrw4sm8aMgotEk+AStKTmlRg=="
)

# COMMAND ----------

file_list = dbutils.fs.ls("abfss://transport-con@elfaquirlake.dfs.core.windows.net/public_transport_data/raw")

# Extract only the file names
file_names = [file.name for file in file_list]

# Print the list of file names
for name in file_names:
    print(name)

# COMMAND ----------

file_name = file_names[1]

# COMMAND ----------

file_location = f"abfss://transport-con@elfaquirlake.dfs.core.windows.net/public_transport_data/raw/{file_name}"

df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)

display(df)

# COMMAND ----------

## Cleanning Data :

# COMMAND ----------


# Calculate the total number of null values in the DataFrame
null_count = df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).collect()[0]

# Print the null count for each column
for column, count in null_count.asDict().items():
    print(f"Column '{column}': {count} null values")


# COMMAND ----------

# Fix data DepartureTime :
df = df.withColumn("DepartureTime", date_format(col("DepartureTime"), "HH:mm"))
display(df)

# COMMAND ----------

# MAGIC %md ### check value of time :

# COMMAND ----------

## check value of time :

# Define a regular expression pattern to match valid time values (HH:mm)
time_pattern = r'^([01][0-9]|2[0-3]):[0-5][0-9]$'

# Filter rows with invalid time values in DepartureTime or ArrivalTime columns
invalid_time_rows = df.filter(~(col("DepartureTime").rlike(time_pattern)) | ~(col("ArrivalTime").rlike(time_pattern)))

# Show the DataFrame with rows containing invalid time values
invalid_time_rows.show(100)


# COMMAND ----------

# MAGIC %md ### Fixe vlue in ArrivalTime :

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

average_passengers = df.select(avg("Passengers")).first()[0]

# Identifier les heures de pointe et heures hors pointe en fonction du nombre de passagers :

df = df.withColumn("HeureDePointe", when(col("Passengers") > average_passengers, "peak").otherwise("off-peak"))

# Afficher le DataFrame avec les heures de pointe identifiées :
display(df)

# COMMAND ----------

# MAGIC %md #### Analyse des Itinéraires :

# COMMAND ----------

result_df = df.groupBy("Route").agg(
    avg("Delay").alias("RetardMoyen"),
    avg("Passengers").alias("NombrePassagersMoyen"),
    count("*").alias("NombreTotalVoyages")
)

# Afficher le DataFrame résultant :
display(result_df)


# COMMAND ----------

# MAGIC %md #### Save File :

# COMMAND ----------

display(df)

# COMMAND ----------

# Define the path where you want to save the CSV file in Azure Data Lake Storage :
output_file_location = f"abfss://transport-con@elfaquirlake.dfs.core.windows.net/public_transport_data/processed/{file_name}"

# Save the DataFrame as a CSV file to the specified location :
df.write.csv(output_file_location, header=True, mode="overwrite")

# Display a message indicating the save operation is complete :
print("DataFrame saved as CSV to Azure Data Lake Storage")
