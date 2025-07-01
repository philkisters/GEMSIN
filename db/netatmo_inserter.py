import requests
import time
import json

from db import SensorDB
from models import Sensor, Rectangle, Position

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

    response = requests.post(url, headers=headers, json=data)
    return data, response
  
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
  
  def fetch_sensors_in_area(self, area_of_interest: Rectangle, store_sensors: bool, request_delay = 2) -> list[Sensor]:   
    """
    Fetches sensors located within a specified rectangular area by subdividing the area into smaller squares
    and querying each square for sensors. Ensures that only unique sensors are collected based on their original IDs.
    Args:
      area_of_interest (Rectangle): The rectangular area to search for sensors.
      store_sensors (bool): If True, stores the fetched sensors using the store_sensors method and returns the result.
                  If False, returns the list of unique sensors.
      request_delay (int, optional): Delay in seconds between requests for each square. Defaults to 2.
    Returns:
      list of sensors: If store_sensors is False, returns a list of unique sensors found in the area.
             If store_sensors is True, returns the result of the store_sensors method.
    """
    squares = area_of_interest.subdivide()
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
    
  
  def fetch_data_from_sensor_module(self, device_id, module_id, types, scale="1day", date_begin = None, date_end = None):
    """
    Fetches weather data from a specific Netatmo sensor module.
    Args:
      device_id (str): The ID of the Netatmo device.
      module_id (str): The ID of the sensor module within the device.
      types (list of str): List of measurement types to fetch (e.g., ["Temperature", "Humidity"]).
      scale (str, optional): The time scale for the data aggregation (default is "1day", other options are "latest", "30min", "1hour", "3hours", "1day", "1week", "1month").
      date_begin (int or None, optional): The start timestamp (UNIX epoch) for the data range. If None, fetches from earliest available.
      date_end (int or None, optional): The end timestamp (UNIX epoch) for the data range. If None, fetches up to latest available.
    Returns:
      list of dict: A list of measurements, each represented as a dictionary with a "timestamp" key and keys for each requested type.
              Returns an empty list if no data is found or if the request fails.
    """
    req, res = self.netatmo_fetcher.fetch_weather_data(device_id=device_id, module_id=module_id, types=types, scale=scale, date_begin=date_begin, date_end=date_end)
    
    if res.status_code != 200:
      print(f"Got status code != 200 {res.status_code}")
      print(res.json())
      return []
      
    res = res.json()
    
    if len(res['body']) <= 1:
      print("No data points found")
      return []
    
    
    result = []
    timestamps = sorted(list([int(timestamp) for timestamp in res['body'].keys()]))
    print(f"{len(timestamps)} measurements found for sensor {device_id}")
    
    for timestamp in timestamps:
      values = res['body'][str(timestamp)]
      measurement = {
        "timestamp": timestamp 
      }
      for index, type in enumerate(types):
        measurement[type] = values[index]
      
      result.append(measurement)
      
    return result
    
    
    
    