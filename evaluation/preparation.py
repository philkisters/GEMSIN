from dotenv import load_dotenv

import os
import pandas as pd

from db import SensorDB, DBConfig
from db.dwd_inserter import DWDInserter
from models import MeasurementType

from functools import reduce

def load_dwd_sensor_data(sensorid: str) -> pd.DataFrame:
  load_dotenv()

  db = SensorDB(DBConfig(
      dbname=os.getenv("DB_NAME"),
      user=os.getenv("DB_USER"),
      password=os.getenv("DB_PASSWORD"),
      host=os.getenv("DB_HOST"),
      port=os.getenv("DB_PORT")
  ))

  inserter = DWDInserter(db)

  sensor = inserter.get_sensor_by_id(sensorid)
  
  temp_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.TEMPERATURE_24H.value, from_timestamp="1980-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
  humi_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.HUMIDITY_24H.value, from_timestamp="1980-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
  pres_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.PRESSURE_24H.value, from_timestamp="1980-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
  rain_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.RAIN_24H.value, from_timestamp="1980-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
  wind_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.WIND_STRENGTH_24H.value, from_timestamp="1980-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
  sun_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.SUN_24H.value, from_timestamp="1980-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
  cloud_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.CLOUD_COVERAGE_24H.value, from_timestamp="1980-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")

  # Create DataFrames for each measurement series with timestamp as key
  # Each DataFrame contains one variable and its timestamps
  df_temp = pd.DataFrame({'timestamp': [m['timestamp'] for m in temp_measurements], 'temperature': [m['value'] for m in temp_measurements]})
  df_humi = pd.DataFrame({'timestamp': [m['timestamp'] for m in humi_measurements], 'humidity': [m['value'] for m in humi_measurements]})
  df_pres = pd.DataFrame({'timestamp': [m['timestamp'] for m in pres_measurements], 'pressure': [m['value'] for m in pres_measurements]})
  df_rain = pd.DataFrame({'timestamp': [m['timestamp'] for m in rain_measurements], 'rain': [m['value'] for m in rain_measurements]})
  df_wind = pd.DataFrame({'timestamp': [m['timestamp'] for m in wind_measurements], 'wind': [m['value'] for m in wind_measurements]})
  df_sun = pd.DataFrame({'timestamp': [m['timestamp'] for m in sun_measurements], 'sun': [m['value'] for m in sun_measurements]})
  df_cloud = pd.DataFrame({'timestamp': [m['timestamp'] for m in cloud_measurements], 'cloud': [m['value'] for m in cloud_measurements]})

  # Merge all DataFrames on timestamp (inner join, keeps only common timestamps)
  dfs = [df_temp, df_humi, df_pres, df_rain, df_wind, df_sun, df_cloud]
  df_merged = reduce(lambda left, right: pd.merge(left, right, on='timestamp', how='inner'), dfs)

  # Remove all rows with NaN values (should be none after inner join, but just in case)
  df_clean = df_merged.dropna()

  # Add month and year as separate columns for imputation
  df_clean['month'] = pd.to_datetime(df_clean['timestamp']).dt.month
  df_clean['year'] = pd.to_datetime(df_clean['timestamp']).dt.year
  
  print(f"Returning DataFrame {len(df_clean)} with data points")
  return df_clean