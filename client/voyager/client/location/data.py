import os
from getpass import getuser
from voyager.client.location.location import Location

class DataStore(Location):

  def __init__(self, name, factory, params, layer=None):
    Location.__init__(self, type='DATA_STORE', name=name, factory=factory)
    self.connection = map(lambda (k,v) : {'key':k, 'val':v}, params.items())
    if layer:
      self.layer = layer;

class PostGIS(DataStore):

  def __init__(self, db, host='localhost', port=5432, user=getuser(), passwd=None, layer=None, **kwargs):
    params = {
      'dbtype': 'postgis',
      'host': host,
      'port': port,
      'user': user,
      'database': db
    }
    params.update(kwargs)

    if passwd:
      params['passwd'] = passwd

    DataStore.__init__(self, db, 'org.geotools.data.postgis.PostgisNGDataStoreFactory', params, layer )

class Shapefile(DataStore):

  def __init__(self, shp, layer=None):
    params = {
      'url': 'file://' + os.path.abspath(shp)
    }

    name = os.path.splitext(os.path.basename(shp))[0]
    DataStore.__init__(self, name, 'org.geotools.data.shapefile.ShapefileDataStoreFactory', params, layer)