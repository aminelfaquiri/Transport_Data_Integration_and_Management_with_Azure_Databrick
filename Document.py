# Databricks notebook source
# DBTITLE 1,introduction :


# COMMAND ----------

# DBTITLE 1,Description des Données :
# MAGIC %md Les données utilisées dans le cadre de ce projet de gestion des données de transport sont gérées à l'aide d'un script Python.
# MAGIC
# MAGIC ######TransportType :
# MAGIC Cette colonne indique le type de transport, par exemple, "Tram". Elle permet de catégoriser les véhicules de transport en fonction de leur mode.
# MAGIC
# MAGIC ######Route :
# MAGIC La colonne "Route" spécifie le numéro ou le nom de la route ou de la ligne de transport. Elle identifie un itinéraire spécifique suivi par le véhicule.
# MAGIC
# MAGIC ######DepartureTime :
# MAGIC Il s'agit de l'heure de départ du véhicule. Cette information est importante pour suivre les horaires de départ.
# MAGIC
# MAGIC ######ArrivalTime : 
# MAGIC L'heure d'arrivée représente l'heure à laquelle le véhicule atteint sa destination. Elle est essentielle pour calculer la durée du voyage.
# MAGIC
# MAGIC ######Passengers : 
# MAGIC Cette colonne indique le nombre de passagers à bord du véhicule à un moment donné. Elle peut être utilisée pour analyser les niveaux de fréquentation.
# MAGIC
# MAGIC ######DepartureStation : 
# MAGIC La station de départ est le point de départ du voyage. Elle est généralement identifiée par un nom ou un code.
# MAGIC
# MAGIC ######ArrivalStation : 
# MAGIC La station d'arrivée est la destination finale du voyage. Elle est également identifiée par un nom ou un code.
# MAGIC
# MAGIC ######Delay : 
# MAGIC Le retard représente la différence entre l'heure prévue d'arrivée et l'heure réelle d'arrivée du véhicule. Il peut être exprimé en minutes.
# MAGIC
# MAGIC ######Date : 
# MAGIC Cette colonne indique la date du voyage.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Transformations :
# MAGIC %md 
# MAGIC Ces transformations sont effectuées à l'aide de PySpark dans un environnement Databricks.
# MAGIC
# MAGIC ##### Importation des Bibliothèques :
# MAGIC Nous commençons par importer les bibliothèques PySpark,datetime essentielles qui nous permettront d'effectuer une gamme d'opérations sur les données.

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth,\
    dayofweek,col,date_format,regexp_extract,when,expr,\
    unix_timestamp, from_unixtime,avg,to_timestamp,col, sum, count

from datetime import datetime

# COMMAND ----------

# MAGIC %md #####Configuration du Stockage Azure :
# MAGIC Pour faciliter l'accès aux données brutes et sauvegarder les données transformées, nous configurons notre compte de stockage Azure en utilisant les clés d'accès, le nom du compte et le nom du conteneur.

# COMMAND ----------

account_name = "elfaquiriacount"
container_name = "mydata"
Access_keys = "fKOvOOt/O/dwhHzRQADXl0AG5/4NjinJ9fB4bEWVOn7xf8CbhAhLr/AbQySNsLIzRFUGcPivt/Vs+AStpFyXqw=="

spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net", 
    f"{Access_keys}"
)

# COMMAND ----------

# MAGIC %md #### la récupération des fichiers  :
# MAGIC
# MAGIC Dans cette étape, le code effectue la récupération des fichiers dans deux répertoires distincts du système de stockage Azure Blob Storage, puis stocke les noms de ces fichiers dans des listes distinctes.

# COMMAND ----------

# get all fishier in processed file :
processed_data = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/processed/")

processed_data = [file.name for file in processed_data]

# csv file in row :
file_list = dbutils.fs.ls(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/public_transport_data/raw/")

# create list of csv :
file_names = [file.name for file in file_list]

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Lignage des Données : 


# COMMAND ----------

# DBTITLE 1,Directives d'Utilisation :

