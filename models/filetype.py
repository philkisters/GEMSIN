from enum import Enum, verify, UNIQUE

@verify(UNIQUE)
class FileType(Enum):
  NETCDF = "NetCDF"
  GEOTIFF = "GeoTIFF"
  
  @staticmethod
  def get_content_type(filetype: 'FileType') -> str:
    types = {
      FileType.NETCDF: "application/x-netcdf",
      FileType.GEOTIFF: "image/tiff"
    }
    return types.get(filetype, "Unknown")
  
  @staticmethod
  def get_upload_url_ending(filetype: 'FileType') -> str:
    types = {
      FileType.NETCDF: "netcdf",
      FileType.GEOTIFF: "geotiff"
    }
    return types.get(filetype, "Unknown")
    