from dotenv import load_dotenv
import os

import pandas as pd

from functools import reduce
from pypots.optim import Adam
from pypots.imputation import CSDI

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db import SensorDB, DBConfig
from db.dwd_inserter import DWDInserter
from models.measurement_type import MeasurementType
from data_preprocessing.dwd import preprocess_dwd

load_dotenv()

db = SensorDB(DBConfig(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
))

inserter = DWDInserter(db)

sensor = inserter.get_sensor_by_id("01975")

temp_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.TEMPERATURE_24H.value, from_timestamp="2015-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
humi_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.HUMIDITY_24H.value, from_timestamp="2015-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
pres_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.PRESSURE_24H.value, from_timestamp="2015-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
rain_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.RAIN_24H.value, from_timestamp="2015-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
wind_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.WIND_STRENGTH_24H.value, from_timestamp="2015-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
sun_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.SUN_24H.value, from_timestamp="2015-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")
cloud_measurements = db.get_measurements_for_sensor(sensor.sensor_id, measurement_type=MeasurementType.CLOUD_COVERAGE_24H.value, from_timestamp="2015-04-30 12:00:00", to_timestamp="2025-05-01 12:00:00")

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
print(f"Remaining data points: {len(df_clean)}")

# Preprocess for model input
dataset = preprocess_dwd(df_clean)

dataset_for_training = {
    "X": dataset['train_X'],
}

dataset_for_validating = {
    "X": dataset['val_X'],
    "X_ori": dataset['val_X_ori'],
}

dataset_for_testing = {
    "X": dataset['test_X'],
}

# Initialize and train CSDI model
csdi = CSDI(
    n_steps=dataset['n_steps'],
    n_features=dataset['n_features'],
    n_layers=6,
    n_heads=2,
    n_channels=128,
    d_time_embedding=64,
    d_feature_embedding=32,
    d_diffusion_embedding=128,
    target_strategy="random",
    n_diffusion_steps=50,
    batch_size=32,
    epochs=100,
    patience=3,
    optimizer=Adam(lr=1e-3),
    num_workers=0,
    device=None,
    saving_path=".data/imputation/csdi",
    model_saving_strategy="best",
)

csdi.fit(train_set=dataset_for_training, val_set=dataset_for_validating)