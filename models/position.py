
class Position:
  def __init__(self, latitude, longitude):
    self.latitude = latitude
    self.longitude = longitude
  
  def __str__(self):
    return f'Position(latitude={self.latitude}, longitude={self.longitude})'
  
  @staticmethod
  def from_wkt_position(position_wkt:str) -> 'Position':
    """
    Creates a Position object from a WKT (Well-Known Text) representation of a point.
    Args:
      position_wkt (str): A string in WKT format representing a point, e.g., "POINT (longitude latitude)".
    Returns:
      Position: A Position object with latitude and longitude extracted from the WKT string.
    Raises:
      Exception: If the WKT string does not start with 'POINT' or is improperly formatted.
    """
    if not position_wkt.startswith('POINT'):
      raise Exception(f"Invalid position string: {position_wkt}")

    coords = position_wkt[6:-1].split()
    longitude, latitude = map(float, coords)
    return Position(latitude=latitude, longitude=longitude)