import requests
import time
import json
import datetime

from db import SensorDB
from models import Sensor, Rectangle, Position, Measurement, MeasurementType

class NetAtmoFetcher():
  def __init__(self):
    self.token = self.fetch_token()
    
  def fetch_token(self):
    token_res = requests.get("https://auth.netatmo.com/weathermap/token")
    return token_res.json()['body']
  
  def fetch_weather_data(self, device_id, module_id, types, scale="1day", date_begin = None, date_end = None):
    url = "https://app.netatmo.net/api/getmeasure"

    headers = {
      "accept": "application/json, text/plain, */*",
      "accept-language": "en-US,en;q=0.9,de;q=0.8,de-DE;q=0.7,zh-TW;q=0.6,zh-CN;q=0.5,zh;q=0.4,sv;q=0.3",
      "authorization": "Bearer " + self.token,
      "content-type": "application/json"
    }

    data = {
      "device_id": device_id,
      "module_id": module_id,
      "scale": scale,
      "type": types,
      "optimize": False
    }

    if date_begin is not None:
      data['date_begin'] = str(date_begin)

    if date_end is not None:
      data['date_end'] = str(date_end)
      
    result = []

    last_timestamp = None

    while True:
      if last_timestamp is None:
        print("Fetching data starting from begin")
      else:
        print(f"Fetching data starting from {datetime.datetime.fromtimestamp(last_timestamp).isoformat()}")
        data['date_begin'] = str(last_timestamp)
      
      response = requests.post(url, headers=headers, json=data)
      
      if response.status_code != 200:
        print(f"Request failed -- StatusCode Expected: 200 -- Actual: {response.status_code}")
        print(data)
        print(response.json())
        return result
      
      response = response.json()['body']
      
      if len(response) <= 1:
        print("No data points found")
        return result
      
      
      timestamps = sorted(list([int(timestamp) for timestamp in response.keys()]))
      for timestamp in timestamps:
        values = response[str(timestamp)]
        measurement = {
          "timestamp": timestamp 
        }
        for index, type in enumerate(types):
          measurement[type] = values[index]
        
        result.append(measurement)
        
      last_timestamp = timestamps[-1] + 1
      
      if len(timestamps) < 1024:
        break
      
    return result
  
  def fetch_sensors_for_area(self, area: Rectangle):
    url = "https://app.netatmo.net/api/getpublicmeasures"
    params = {
      "limit": 1,
      "divider": 7,
      "quality": 7,
      "zoom": 12,
      "lat_ne": area.north_east.latitude,
      "lon_ne": area.north_east.longitude,
      "lat_sw": area.south_west.latitude,
      "lon_sw": area.south_west.longitude,
      "date_end": "last",
      "access_token": self.token
    }

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,de;q=0.8,de-DE;q=0.7,zh-TW;q=0.6,zh-CN;q=0.5,zh;q=0.4,sv;q=0.3",
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
      print(f"Request failed -- StatusCode Expected: 200 -- Actual: {response.status_code}")
      print(params)
      print(response.json())
      return []
    
    return params, response.json()
    

class NetAtmoInserter():
  IDENTIFIER = "Netatmo" 
  NETATMO_TYPE_MAPPING = {
    "latest": {
      "temperature": MeasurementType.TEMPERATURE,
      "pressure": MeasurementType.PRESSURE,
      "humidity": MeasurementType.HUMIDITY,
      "rain_60min": MeasurementType.RAIN_60MIN,
      "rain_24h": MeasurementType.RAIN_24H,
      "rain_live": MeasurementType.RAIN_LIVE,
      "wind_strength": MeasurementType.WIND_STRENGTH,
      "wind_angle": MeasurementType.WIND_ANGLE,
      "gust_strength": MeasurementType.GUST_STRENGTH,
      "gust_angle": MeasurementType.GUST_ANGLE,
    },
    "1day": {
      "temperature": MeasurementType.TEMPERATURE_24H,
      "pressure": MeasurementType.PRESSURE_24H,
      "humidity": MeasurementType.HUMIDITY_24H,
      "min_temp": MeasurementType.TEMPERATURE_24H_MIN,
      "min_pressure": MeasurementType.PRESSURE_24H_MIN,
      "min_hum": MeasurementType.HUMIDITY_24H_MIN,
      "max_temp": MeasurementType.TEMPERATURE_24H_MAX,
      "max_pressure": MeasurementType.PRESSURE_24H_MAX,
      "max_hum": MeasurementType.HUMIDITY_24H_MAX,
      "rain": MeasurementType.RAIN_24H,
      "wind_strength": MeasurementType.WIND_STRENGTH_24H,
      "wind_angle": MeasurementType.WIND_ANGLE_24H,
      "gust_strength": MeasurementType.GUST_STRENGTH_24H,
      "gust_angle": MeasurementType.GUST_ANGLE_24H,
    }
  }
  
  def __init__(self, db: SensorDB):
    self.db = db
    self.netatmo_fetcher = NetAtmoFetcher()
    
  def get_sensor_by_id(self, originial_id) -> Sensor:
    """
    Retrieves a Sensor object from the database using the provided original sensor ID and the current source identifier.

    Args:
      originial_id (str): The original identifier of the sensor.

    Returns:
      Sensor: The Sensor object corresponding to the given original ID and source, or None if not found.
    """
    return self.db.get_sensor_by_original_id_and_source(original_id=originial_id, source=self.IDENTIFIER)
  
  def fetch_sensors_in_area(self, area_of_interest: Rectangle, store_sensors: bool = False, square_size_m = 1000, request_delay = 2) -> list[Sensor]:   
    """
    Fetches sensors located within a specified rectangular area by subdividing the area into smaller squares
    and querying each square for sensors. Ensures that only unique sensors are collected based on their original IDs.
    Args:
      area_of_interest (Rectangle): The rectangular area to search for sensors.
      store_sensors (bool, optional): If True, stores the fetched sensors using the store_sensors method and returns the result.
                  If False, returns the list of unique sensors. Defaults to False
      square_size_m (int, optional): Determines the number of squares the area is divided into. 
                  Leads to more sensors found, but increases the amount of requests agains the NetAtmo API.
                  Defaults to 1000
      request_delay (int, optional): Delay in seconds between requests for each square. Defaults to 2.
    Returns:
      list of sensors: If store_sensors is False, returns a list of unique sensors found in the area.
             If store_sensors is True, returns the result of the store_sensors method.
    """
    squares = area_of_interest.subdivide(square_size_m)
    sensor_ids = set()
    sensors = []
    
    for index, square in enumerate(squares):
      req, res = self.netatmo_fetcher.fetch_sensors_for_area(square)
      
      for item in res['body']:
        sensor = self.sensor_from_response_item(item)
        if sensor.original_id != "" and sensor.original_id not in sensor_ids:
          sensor_ids.add(sensor.original_id)
          sensors.append(sensor)
      
      print(f"Square {index}/{len(squares)}: Found {len(res['body'])} sensors. Unique sensors: {len(sensors)}")
      time.sleep(request_delay)
    
    if (store_sensors):
      return self.store_sensors(sensors)
    
    return sensors
  
  def store_sensors(self, sensors) -> list[Sensor]:
    """
    Stores a list of sensor objects in the database.
    For each sensor in the provided list, attempts to insert or update the sensor
    in the database using the `upsert_sensor` method. Collects and returns the list
    of stored sensor objects as returned by the database operation.
    Args:
      sensors (list): A list of sensor objects to be stored or updated.
    Returns:
      list[Sensor]: A list of sensor objects as stored in the database.
    """
    stored_sensors = []
    
    for sensor in sensors:
      stored_sensor = self.db.upsert_sensor(sensor)
      stored_sensors.append(stored_sensor)
      
    return stored_sensors
  
  def store_measurements(self, sensor: Sensor, received_measurements, scale):
    assert scale in ["latest", "1day"], f"Currently we can only story daily or latest measurements in the database."
    
    print(f"Starting to store {len(received_measurements)} received measurements for sensor {sensor.original_id}")
    measurements = []
    for received_measurement in received_measurements:
      for type in received_measurement:
        if type == "timestamp":
          continue
        
        measurement_type = self.NETATMO_TYPE_MAPPING[scale][type]
        unit = MeasurementType.get_unit_for_type(measurement_type)

        timestamp  = datetime.datetime.fromtimestamp(received_measurement['timestamp'])
        
        measurement = Measurement(measurement_type.value, sensor.position, timestamp, unit, received_measurement[type], sensor.sensor_id)
        measurements.append(measurement)
    
    print(f"Storing {len(measurements)} for sensor {sensor.original_id}")
    self.db.insert_batch_measurements(measurements)


  def sensor_from_response_item(self, item):
    """
    Extracts sensor information from a Netatmo API response item and constructs a Sensor object.
    The method processes the 'measures' field in the response item to extract module IDs and their associated types.
    It handles special cases for rain and wind modules, assigning appropriate measurement types.
    The resulting modules information is serialized as a JSON string and stored in the sensor_type field of the Sensor object.
    Args:
      item (dict): A dictionary representing a single station or device from the Netatmo API response.
    Returns:
      Sensor: An instance of the Sensor class populated with information extracted from the response item.
          If extraction fails due to missing keys, returns a Sensor object with default/empty values.
    """
    position = Position(item['place']['location'][1], item['place']['location'][0])
    try:
      modules = []
      for module in item['measures']:
        sensor_module = {
          'module_id': module
        }
        types = []
        if 'type' in item['measures'][module]:
          types = item['measures'][module]['type']
        elif 'rain_60min' in item['measures'][module]:
          types = ['rain_60min', 'rain_24h', 'rain_live']
        elif 'wind_strength' in item['measures'][module]:
          types = ['wind_strength', 'wind_angle', 'gust_strength', 'gust_angle']
        
        sensor_module['types'] = types
        modules.append(sensor_module)
    except KeyError:
      print(f"Couldn't extract sensor from item: {item}")
      return Sensor("", "", position, "", self.IDENTIFIER)
    
    modules = json.dumps(modules)
    
    return Sensor(additional_information="", original_id=item['_id'], position=position, sensor_type=modules, source=self.IDENTIFIER)
  
  def fetch_data_from_sensor(self, sensor: Sensor, types, scale="1day", date_begin = None, date_end = None):
    assert sensor.source == self.IDENTIFIER, f"Must be a '{self.IDENTIFIER}' sensor to receive measurements, got '{sensor.source}' instead"
    assert scale in ["latest", "30min", "1hour", "3hours", "1day", "1week", "1month"], f"Scale must be one of the following: (latest, 30min, 1hour, 3hours, 1day, 1week, 1month), got {scale} instead"
    assert sensor.sensor_type != "", f"The sensor_type field must contain module information, but is empty for this sensor."
    
    modules = json.loads(sensor.sensor_type)
    get_all = types == 'all'
    
    measurements = []
    
    print(f"Get all: {get_all}")
    
    for module in modules:
      if get_all:
        selected_types = module['types']
      else:
        selected_types = self._select_types_with_subtypes(module['types'], types)
      
      if selected_types:
        print(f"{module['module_id']}: Found selected types: {selected_types}")
        
        data = self.netatmo_fetcher.fetch_weather_data(sensor.original_id, module['module_id'], selected_types, scale, date_begin, date_end)
        print(f"Received {len(data)} measurements for {selected_types} on sensor {sensor.original_id}")
        measurements.append({
          'module_id': module['module_id'],
          'measurements': data
        })
    
    return measurements
    
  def _select_types_with_subtypes(self, module_types, type_filter):
    selected_types = []
    for type in module_types:
      if type in type_filter:
        selected_types.append(type)
        # Add related subtypes if present
        if type == "temperature":
          for sub in ["min_temp", "max_temp"]:
            if sub in type_filter:
              selected_types.append(sub)
        elif type == "humidity":
          for sub in ["min_hum", "max_hum"]:
            if sub in type_filter:
              selected_types.append(sub)
        elif type == "pressure":
          for sub in ["min_pressure", "max_pressure"]:
            if sub in type_filter:
              selected_types.append(sub)
        elif type == "co2":
          for sub in ["min_co2", "max_co2"]:
            if sub in type_filter:
              selected_types.append(sub)
        elif type == "noise":
          for sub in ["min_noise", "max_noise"]:
            if sub in type_filter:
              selected_types.append(sub)
    return selected_types  
