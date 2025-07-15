from models.position import Position 

class Measurement:
  def __init__(self, measurement_type, position: Position, timestamp, unit, value, sensor_id, measurement_id=-1):
    self.measurement_id = measurement_id
    self.measurement_type = measurement_type
    self.position = position
    self.timestamp = timestamp
    self.unit = unit
    self.value = value
    self.sensor_id = sensor_id

  def set_measurement_id(self, measurement_id):
    if self.measurement_id != -1 and self.measurement_id != measurement_id:
      raise Exception(f"Measurement already has id set: {self.measurement_id}")
    self.measurement_id = measurement_id
      
  
  def __repr__(self):
    return (f"Measurement(measurement_id={self.measurement_id}, "
      f"measurement_type={self.measurement_type}, position={self.position}, "
      f"timestamp={self.timestamp}, unit='{self.unit}', value={self.value}, "
      f"sensor_id={self.sensor_id})")
    

class AggregatedMeasurement(Measurement):
  def __init__(self, measurement_type, position: Position, timestamp, unit, value, sensor_id, interval_in_seconds, aggregation_method, measurement_id=-1):
    super().__init__(measurement_type, position, timestamp, unit, value, sensor_id, measurement_id)
    self.interval_in_seconds = interval_in_seconds
    self.aggregation_method = aggregation_method

  def __repr__(self):
    base_repr = super().__repr__()[:-1]  # remove closing parenthesis
    return f"{base_repr}, interval_in_seconds={self.interval_in_seconds}, aggregation_method={self.aggregation_method})"
