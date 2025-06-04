import requests
from models.filetype import FileType

class GeoServerConfig:
  """
  A configuration class for GeoServer that holds the connection details.
  Attributes:
    url (str): The base URL of the GeoServer instance.
    username (str): The username for authenticating with GeoServer.
    password (str): The password for authenticating with GeoServer.
  """
  def __init__(self, url: str, username: str, password: str):
    self.url = url
    self.username = username
    self.password = password

class GeoServer:
  """
  A class to interact with a GeoServer instance using the provided configuration.
  Attributes:
    config (GeoServerConfig): The configuration object containing connection details.
  """
  def __init__(self, config: GeoServerConfig):
    self.config = config
    
  def upload_store(self, workspace, store_name, file_path, filetype: FileType) -> bool:
    headers = {"Content-type": FileType.get_content_type(filetype)}
    upload_url = f"{self.config.url}/rest/workspaces/{workspace}/coveragestores/{store_name}/file.{FileType.get_upload_url_ending(filetype)}?configure=none"
    with open(file_path, 'rb') as file_data:
      response = requests.put(upload_url, data=file_data, headers=headers, auth=(self.config.username, self.config.password))

    if response.status_code not in [200, 201]:
      print(f"Failed to upload NetCDF for {file_path}: {response.text}")
      return False

    return True
  
  def publish_layer(self, workspace, store_name, layer_name, native_layer="") -> bool:
    # Publish the layer
    publish_url = f"{self.config.url}/rest/workspaces/{workspace}/coveragestores/{store_name}/coverages"
    headers = {"Content-type": "application/xml"}
    
    if native_layer == "":
      native_layer = layer_name
    
    layer_xml = f"""
    <coverage>
      <name>{layer_name}</name>
      <nativeCoverageName>{native_layer}</nativeCoverageName>
      <enabled>true</enabled>
    </coverage>
    """
    response = requests.post(publish_url, data=layer_xml, headers=headers, auth=(self.config.username, self.config.password))
    if response.status_code not in [200, 201]:
      print(f"Failed to publish '{layer_name}' layer for {store_name}: {response.text}")
      return False

    return True

    