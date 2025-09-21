# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import reverse_geocode
from datetime import date, timedelta

# COMMAND ----------

past_date = date.today() - timedelta(days=1)
staging_data_file_path = '/Volumes/catalog_earthquakes/schema_earthquakes/volume_earthquakes/staging_layer/'
curated_data_file_path = '/Volumes/catalog_earthquakes/schema_earthquakes/volume_earthquakes/curated_layer/'

# COMMAND ----------

df = spark.read \
    .format('delta') \
    .load(staging_data_file_path) \
    .where(f"logged_date = '{past_date}'")

# COMMAND ----------

compass_directions = {
    "N":   "North",
    "NNE": "North-Northeast",
    "NE":  "Northeast",
    "ENE": "East-Northeast",
    "E":   "East",
    "ESE": "East-Southeast",
    "SE":  "Southeast",
    "SSE": "South-Southeast",
    "S":   "South",
    "SSW": "South-Southwest",
    "SW":  "Southwest",
    "WSW": "West-Southwest",
    "W":   "West",
    "WNW": "West-Northwest",
    "NW":  "Northwest",
    "NNW": "North-Northwest"
}
df_directions = spark.createDataFrame(compass_directions.items(), ['direction', 'direction_description'])
df = df.join(df_directions, "direction", "left")

# COMMAND ----------

df = df.withColumn('magnitude_band',
     when((col("mag") > 0)   & (col("mag") <= 2.5), "Very Minor") \
    .when((col("mag") > 2.5) & (col("mag") <= 5.5), "Minor") \
    .when((col("mag") > 5.5) & (col("mag") <= 6.0), "Moderate") \
    .when((col("mag") > 6.0) & (col("mag") <= 7.0), "Strong") \
    .when((col("mag") > 7.0) & (col("mag") <= 8.0), "Major") \
    .when((col("mag") >= 8.0), "Great") \
    .otherwise("Unknown")
)


# COMMAND ----------

schema = StructType([
  StructField('city', StringType(), True),
  StructField('population', IntegerType(), True),
  StructField('county', StringType(), True),
  StructField('state', StringType(), True),
  StructField('country', StringType(), True)
])

def check_node(node, data):
  if node in data:
    return data[node]
  else:
    return None 

@udf(schema)
def reverse_geocode_lookup(longitude, latitude):
    x_y = (latitude, longitude)
    data = reverse_geocode.search([x_y])[0]
    city = check_node('city', data)
    population = check_node('population', data)
    county = check_node('county', data)
    state = check_node('state', data)
    country = check_node('country', data)
    
    return  city, population, county, state, country


# COMMAND ----------

df = df.withColumn('location',
                   reverse_geocode_lookup(
                       col('longitude').cast(FloatType()), 
                       col('latitude').cast(FloatType()))
                   )

# COMMAND ----------

df = df.withColumn('affected_city', col('location.city'))\
    .withColumn('affected_population', col('location.population'))\
    .withColumn('affected_county', col('location.county'))\
    .withColumn('affected_state', col('location.state'))\
    .withColumn('affected_country', col('location.country'))


# COMMAND ----------

df = df.drop('location')

# COMMAND ----------

df.write \
    .format('delta')  \
    .partitionBy('logged_date')\
    .mode('overwrite') \
    .save(curated_data_file_path)