from models.position import Position 

class Sensor:
    def __init__(self, additional_information, original_id, position: Position, sensor_type, source, sensor_id = -1):
        """
        Initializes a Sensor object.

        :param additional_information: Additional information about the sensor (string)
        :param original_id: Original ID of the sensor (string)
        :param position: Position of the sensor as a dictionary with 'lon' and 'lat' (e.g., {'lon': 9.9881, 'lat': 53.6332})
        :param sensor_type: Type of the sensor (string)
        :param source: Source of the sensor (string)
        """
        self.additional_information = additional_information
        self.original_id = original_id
        self.position = position
        self.sensor_type = sensor_type
        self.source = source
        self.sensor_id = sensor_id
        
    def set_sensor_id(self, sensor_id):
      if not self.sensor_id == -1:
        raise Exception("Sensor already has a valid id.")
      
      self.sensor_id = sensor_id

    def to_dict(self):
        """
        Converts the Sensor object into a dictionary for database operations.

        :return: Dictionary containing sensor data
        """
        return {
            'additional_information': self.additional_information,
            'original_id': self.original_id,
            'position': self.position,
            'sensor_type': self.sensor_type,
            'source': self.source
        }