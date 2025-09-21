# Databricks notebook source
import requests
import json
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %sql
# MAGIC Create schema if not exists catalog_earthquakes.schema_earthquakes;

# COMMAND ----------

current_date = date.today()
past_date = current_date - timedelta(days=1)
raw_bucket_path = f's3://raw-data-bucket-5f593a/raw_layer/raw_data_{past_date}.json'


# COMMAND ----------

current_date = date.today()
past_date = current_date - timedelta(days=1)
raw_file_path = f'/Volumes/catalog_earthquakes/schema_earthquakes/volume_earthquakes/raw_layer/raw_data_{past_date}.json'

# COMMAND ----------

try:
    response = requests.get(f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={past_date}&endtime={current_date}&orderby=time')
    response.raise_for_status()
    data = response.json()
    if data:
        reponse_json = json.loads(response.content.decode('utf-8'))
    else:
        response_json = []
    dbutils.fs.put(raw_bucket_path, json.dumps(reponse_json, indent=4), True)
    print(f'JSON Data is Loaded to file - raw_data_{current_date}.json')
except requests.exceptions.RequestException as e:
    print(f"Error in Request: {str(e)}")
except json.JSONDecodeError as e:
    print(f"Unable to Parse JSON - Decode Error: {e}")
except requests.exceptions.HTTPError as e:
    print(f"HTTP error: {str(e)}")
except Exception as e:
    print(e)