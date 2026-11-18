# Databricks notebook source
# MAGIC %md
# MAGIC ##init

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_root_path = os.path.join(os.getcwd(),'..','..')

sys.path.append(project_root_path)
from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimUser

# COMMAND ----------

df_user = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimUser/checkpoint")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@mithranstorageaccount.dfs.core.windows.net/DimUser")
)

# COMMAND ----------

obj = reusable()
df_user=obj.dropcolumns(df_user,['_rescued_data'])

# COMMAND ----------

query = (df_user.writeStream
    .format("delta")
    .outputMode("append")
    .option("path",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimUser/data")
    .option("checkpointLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimUser/checkpoint")
    .trigger(once=True)
    .toTable("spotify_catalog.silver.dim_user")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimDate

# COMMAND ----------

df_date = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimDate/checkpoint")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@mithranstorageaccount.dfs.core.windows.net/DimDate")
)

# COMMAND ----------

obj = reusable()
df_date=obj.dropcolumns(df_date,['_rescued_data'])

# COMMAND ----------

query = (df_date.writeStream
    .format("delta")
    .outputMode("append")
    .option("path",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimDate/data")
    .option("checkpointLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimDate/checkpoint")
    .trigger(once=True)
    .toTable("spotify_catalog.silver.dim_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimArtist

# COMMAND ----------

df_artist = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimArtist/checkpoint")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@mithranstorageaccount.dfs.core.windows.net/DimArtist")
)

# COMMAND ----------

obj = reusable()
df_artist=obj.dropcolumns(df_artist,['_rescued_data'])

# COMMAND ----------

query = (df_artist.writeStream
    .format("delta")
    .outputMode("append")
    .option("path",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimArtist/data")
    .option("checkpointLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimArtist/checkpoint")
    .trigger(once=True)
    .toTable("spotify_catalog.silver.dim_artist")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimTrack

# COMMAND ----------

df_track = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimTrack/checkpoint")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@mithranstorageaccount.dfs.core.windows.net/DimTrack")
)

# COMMAND ----------

obj = reusable()
df_track=obj.dropcolumns(df_track,['_rescued_data'])

# COMMAND ----------

df_track = df_track.withColumn(
    "duration_sec",
    when(col("duration_sec") <= 150, "Low")
    .when(col("duration_sec") <= 250, "Medium")
    .otherwise("High")
)

# COMMAND ----------

df_track = df_track.withColumn(
    "track_name",
    regexp_replace(col("track_name"), "-", " ")
)

# COMMAND ----------

query = (df_track.writeStream
    .format("delta")
    .outputMode("append")
    .option("path",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimTrack/data")
    .option("checkpointLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/DimTrack/checkpoint")
    .trigger(once=True)
    .toTable("spotify_catalog.silver.dim_track")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_catalog.silver.dim_track

# COMMAND ----------

# MAGIC %md
# MAGIC ###Factstream

# COMMAND ----------

fact_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/FactStream/checkpoint")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@mithranstorageaccount.dfs.core.windows.net/FactStream")
)

# COMMAND ----------

query = (fact_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("path",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/FactStream/data")
    .option("checkpointLocation",
            "abfss://silver@mithranstorageaccount.dfs.core.windows.net/FactStream/checkpoint")
    .trigger(once=True)
    .toTable("spotify_catalog.silver.fact_stream")
)