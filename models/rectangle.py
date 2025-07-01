from geopy import Point
from geopy.distance import distance

from models import Position

class Rectangle: 
  def __init__(self, north_east: Position, south_west: Position):
    self.north_east = north_east
    self.south_west = south_west
    
  def subdivide(self, square_size_m=1000):
    """
    Generates a list of squares covering this rectangular geographic area.

    Args:
        square_size_m (int): Size of each square in meters (default 1000m).

    Returns:
        list of rectangles: Each tuple is ((sw_lat, sw_lon), (ne_lat, ne_lon)) of a square.
    """
    squares = []
    lat = self.south_west.latitude

    while lat < self.north_east.latitude:
      # Move north by square_size_m to get the next latitude
      next_lat = distance(meters=square_size_m).destination(Point(lat, self.south_west.longitude), bearing=0).latitude
      lon = self.south_west.longitude

      while lon < self.north_east.longitude:
        # Move east by square_size_m to get the next longitude
        next_lon = distance(meters=square_size_m).destination(Point(lat, lon), bearing=90).longitude

        sw = Position(lat, lon)
        ne = Position(next_lat, next_lon)
        squares.append(Rectangle(sw, ne))

        lon = next_lon
      lat = next_lat

    return squares
  
  def size(self):
    width = distance((self.north_east.latitude, self.north_east.longitude), (self.north_east.latitude, self.south_west.longitude)).kilometers
    height = distance((self.north_east.latitude, self.north_east.longitude), (self.south_west.latitude, self.north_east.longitude)).kilometers
    
    return (width, height)