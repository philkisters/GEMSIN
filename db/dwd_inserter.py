import os
import pandas as pd

from db import SensorDB
from models import Position, MeasurementType, AggregatedMeasurement, Sensor
from typing import List

class DWDInserter:
  IDENTIFIER = "DWD"
  TIME_COLUMN = "MESS_DATUM"
  AGGREGATION_INTERVAL = 60*60*24 # 1 day in seconds
  DWD_TYPE_MAPPING = {
    "TMK": MeasurementType.TEMPERATURE,
    "FM": MeasurementType.WIND_STRENGTH,
    "FX": MeasurementType.GUST_STRENGTH,
    "NM": MeasurementType.CLOUD_COVERAGE,
    "PM": MeasurementType.PRESSURE,
    "RSK": MeasurementType.RAIN,
    "SDK": MeasurementType.SUN,
    "UPM": MeasurementType.HUMIDITY
  }
  AGR_METHOD_MAPPING = {
      MeasurementType.TEMPERATURE: "AVERAGE",
      MeasurementType.WIND_STRENGTH: "AVERAGE",
      MeasurementType.GUST_STRENGTH: "MAX",
      MeasurementType.CLOUD_COVERAGE: "AVERAGE",
      MeasurementType.PRESSURE: "AVERAGE",
      MeasurementType.RAIN: "AVERAGE",
      MeasurementType.SUN: "AVERAGE",
      MeasurementType.HUMIDITY: "AVERAGE"
  }
  
  def __init__(self, db: SensorDB):
    self.db = db
    
  def get_sensor_by_id(self, originial_id) -> Sensor:
    return self.db.get_sensor_by_original_id_and_source(original_id=originial_id, source=self.IDENTIFIER)
  
  def get_measurement_type(self, dwd_definition: str) -> MeasurementType:
    
    if dwd_definition not in self.DWD_TYPE_MAPPING: return MeasurementType.UNKNOWN
    return self.DWD_TYPE_MAPPING.get(dwd_definition)
  
  def insert_measurement_types_for_sensor(self, sensor: Sensor, measurement_types: List[str]):
    for type in measurement_types:
      measurement_type = self.get_measurement_type(type)
      
      if measurement_type == MeasurementType.UNKNOWN: continue
  
      self.db.add_measurment_type_for_sensor(sensor, measurement_type)
  
  def store_measurement(self, sensor: Sensor, column, timestamp, value) -> AggregatedMeasurement:
    measurement_type = self.get_measurement_type(column)
    if measurement_type == MeasurementType.UNKNOWN: 
      raise Exception(f"Invalid column to store: {column}")
    if value == -999:
      print(f"Ignoring row with timestampt {timestamp} since value is -999")
      return None
    
    measurement = AggregatedMeasurement(measurement_type=measurement_type.value, 
                              position=sensor.position, 
                              timestamp=timestamp, 
                              unit=MeasurementType.get_unit_for_type(measurement_type), 
                              value=value, 
                              sensor_id=sensor.sensor_id,
                              interval_in_seconds=self.AGGREGATION_INTERVAL,
                              aggregation_method=self.AGR_METHOD_MAPPING[measurement_type])
    
    measurement_id = self.db.insert_agr_measurement(measurement)
    if measurement_id != -1:
      measurement.set_measurement_id(measurement_id)
      return measurement
    return None
  
  def store_sensor(self, original_id, position: Position, additional_information="") -> Sensor:
    existing_sensor = self.get_sensor_by_id(original_id)
    if existing_sensor is not None:
      print(f"Sensor with the original_id {original_id} already exists.")
      return existing_sensor
    
    new_sensor = Sensor(additional_information=additional_information, original_id=original_id, position=position, sensor_type="", source=self.IDENTIFIER)
    return self.db.insert_sensor(new_sensor)
  
  def clear_sensor_measurements(self, sensor_id) -> int:
    sensor = self.get_sensor_by_id(sensor_id)
    if sensor is None:
      raise Exception(f"Sensor with original ID {sensor_id} does not exist.")
    
    if sensor.source != self.IDENTIFIER:
      raise Exception(f"Sensor with original ID {sensor_id} does not belong to source {self.IDENTIFIER}.")
    
    deleted_count = self.db.clear_measurements_for_sensor(sensor.sensor_id)
    print(f"All measurements for sensor with ID {sensor.sensor_id} have been cleared.")
    return deleted_count
    
  
  def store_csv(self, filename, file_path, create_sensor = False, position: Position = None) -> int:
    original_id = filename.split("_")[5]
    
    if create_sensor:
      if position is None:
        raise Exception(f"Can't store csv since the position of the sensor f{original_id} is unknown")
      sensor = self.store_sensor(original_id, position)
      
    sensor = self.get_sensor_by_id(original_id)
    if sensor == None:
      raise Exception("To store csv for an unknown sensor, set 'create_sensor' to True and provide the position of the sensor.")      
    
    # Construct the full path to the CSV file
    file_path = os.path.join(file_path, filename + ".csv")

    # Load the CSV file into a DataFrame and use the first row as column names
    df = pd.read_csv(file_path, sep=';', header=0)

    df.columns = df.columns.str.strip()
    columns_of_interest = self.DWD_TYPE_MAPPING.keys()
    
    columns_of_interest = list(columns_of_interest) + [self.TIME_COLUMN]
    print(f"Found columns of interest: {columns_of_interest}")
    
    # Reduce the DataFrame to the defined columns of interest
    df = df[columns_of_interest]
    
    if create_sensor:
      self.insert_measurement_types_for_sensor(sensor=sensor, measurement_types=df.columns.to_list())

    # Convert the TIME_COLUMN to a datetime format
    df[self.TIME_COLUMN] = pd.to_datetime(df[self.TIME_COLUMN], format='%Y%m%d')

    for column in df.columns:
      if column == self.TIME_COLUMN:
        continue
      measurement_type = self.get_measurement_type(column)
      
      last_measurement_str = self.db.get_latest_measurement_timestamp(sensor_id=sensor.sensor_id, measurement_type=measurement_type.value)
      last_measurement = None
      if last_measurement_str is not None:
        last_measurement = pd.to_datetime(last_measurement_str)
      
      if measurement_type == MeasurementType.UNKNOWN:
        print(f"Skipping column {column} because of unknown measurement type")
        continue
      
      measurements = []
      for index, row in df.iterrows(): 
        if row[column] == -999:
          print(f"Ignoring row with timestamp {row[self.TIME_COLUMN]} since value is -999")
          continue
        if last_measurement is not None and row[self.TIME_COLUMN] <= last_measurement:
          continue
        measurement = AggregatedMeasurement(measurement_type=measurement_type.value, 
                              position=sensor.position, 
                              timestamp=row[self.TIME_COLUMN], 
                              unit=MeasurementType.get_unit_for_type(measurement_type), 
                              value=row[column], 
                              sensor_id=sensor.sensor_id,
                              interval_in_seconds=86400,
                              aggregation_method=self.AGR_METHOD_MAPPING[measurement_type])
        measurements.append(measurement)
      
      self.db.insert_batch_aggregated_measurements(measurements)
    
    