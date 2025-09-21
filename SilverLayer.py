# Databricks notebook source
from datetime import date, timedelta
current_date = date.today() 
past_date = current_date - timedelta(days=1)
raw_data_file_path = f'/Volumes/catalog_earthquakes/schema_earthquakes/volume_earthquakes/raw_layer/raw_data_{past_date}.json'
staging_data_file_path = '/Volumes/catalog_earthquakes/schema_earthquakes/volume_earthquakes/staging_layer/'

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, LongType, StringType, DecimalType

# COMMAND ----------

df = spark.read.json(raw_data_file_path, multiLine=True) 

# COMMAND ----------

if 'features' not in df.columns:
  raise ValueError("Features Node is not Found!")
if df.select(df.features).collect()[0]['features'] == []:
  raise ValueError("No data in the dataframe!")

# COMMAND ----------

df = df.withColumn('features', f.explode('features')).select('features')

# COMMAND ----------

df = df.select(f.col('features.properties').alias('props'),f.col('features.geometry').alias('geometry'))

# COMMAND ----------

prop_columns = {
"longitude": f.col("geometry.coordinates").getItem(0).cast(DecimalType(8,4)),
"latitude": f.col("geometry.coordinates").getItem(1).cast(DecimalType(8,4)),
"alert": f.col("props.alert").cast(StringType()),
"felt": f.col("props.felt").cast(IntegerType()),
"mag": f.col("props.mag").cast(DecimalType(3,2)),
"net": f.col("props.net").cast(StringType()),
"place": f.col("props.place").cast(StringType()),
"rms": f.col("props.rms").cast(DecimalType(3,2)),
"sig": f.col("props.sig").cast(IntegerType()),
"time": f.col("props.time").cast(LongType()),
"title": f.col("props.title").cast(StringType()),
} 
df = df.withColumns(prop_columns).drop('props','geometry')

# COMMAND ----------

df = df.filter(' time is not NULL')

# COMMAND ----------

df = df.withColumn('logged_date',f.date_format(f.timestamp_millis(df.time), 'yyyy-MM-dd')) \
       .withColumn('logged_datetime',f.date_format(f.timestamp_millis(df.time), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

df = df.drop('time')

# COMMAND ----------

regex_pattern = "([0-9]*) km ([A-Z]*) of (.*), (.*)$"
place_seperated_cols = {
    'distance': f.regexp_extract("place",regex_pattern,1),
    'direction': f.regexp_extract("place",regex_pattern,2),
    'nearest_city': f.regexp_extract("place",regex_pattern,3),
    'nearest_state': f.regexp_extract("place",regex_pattern,4),
}
df = df.withColumns(place_seperated_cols)
df = df.drop('place','title')

# COMMAND ----------

df = df.select('logged_date','logged_datetime','mag','net','rms','sig','distance','direction','nearest_city','nearest_state','latitude','longitude','felt','alert')

# COMMAND ----------

df.write.format('delta')\
    .partitionBy('logged_date')\
    .mode('overwrite') \
    .save(staging_data_file_path)