import requests, json
from location import Location
from spatial import Spatial

class Client(object):

  def __init__(self, url='http://localhost:8888', user='admin', passwd='admin'):
    """
    Voyager client object.

    :param url: Base url to voyager server.
    :param user: User to connect as.
    :param passwd: User password.
    """
    self.url = url;
    self.auth = (user, passwd)

  def locations(self):
    """
    Returns list of all locations.
    """
    return map(lambda obj: Location(self, **obj),
      self.get('discovery/location')['locations'])

  def get_location(self, id):
    """
    Get a location by identifier.

    :param location: The location id.
    """
    return Location(self, **self.get('discovery/location/%s' % id))

  def add_location(self, location):
    """
    Adds a new location.

    :param location: The location to add, an instance of of :class:`Location`.
    """
    return Location(self, **self.post('discovery/location', location.json()))

  def remove_location(self, id):
    """
    Removes a location.

    :param id: Location id.
    """
    self.delete('discovery/location/%s' % id)

  def remove_locations(self):
    """
    Removes all locations.
    """
    for l in self.locations():
      self.remove_location(l.id)

  def index_location(self, id, delta):
    """
    Indexes contents of a location.

    :param id: Location id.
    :param delta: Whether to index only changes or build from scratch.
    """
    self.post('discovery/location/%s/scan?delta=%s' % (id,delta))

  def wipe_location(self, id):
    """
    Clears contents of a location.

    :param id: Location id.
    """
    self.delete('discovery/location/%s/index' % (id))

  def wipe_index(self):
    """
    Wipes the solr index, removing all documents.
    """
    self.delete('index')

  def spatial(self):
    """
    Returns object for performing spatial operations via the rest api.
    """
    return Spatial(self)

  def get(self, path, params=None):
    """
    Makes an api call with the GET method.

    :param path: The relative api path, without the '/api/rest/' prefix.
    :param params: Optional dict of query string parameters.

    :returns: dict|array -- The request response body parsed as json.
    """
    r = requests.get(self._path(path), auth=self.auth, params=params)
    r.raise_for_status()
    return r.json()

  def post(self, path, body=None, params=None):
    """
    Makes an api call with the POST method.

    :param path: The relative api path, without the '/api/rest/' prefix.
    :param body: The optional request body as a json object.
    :param params: Optional dict of query string parameters.

    :returns: dict|array -- The request response body parsed as json.
    """
    args = {
      'auth': self.auth
    }

    if body is not None:
      args['data'] = json.dumps(body) if isinstance(body, dict) else body
      args['headers']={'Content-Type': 'application/json'}

    if params is not None:
      args['params'] = params

    r = requests.post(self._path(path), **args)
    r.raise_for_status()
    return r.json()

  def delete(self, path, params=None):
    """
    Makes an api call with the DELETE method.

    :param path: The relative api path, without the '/api/rest/' prefix.
    :param params: Optional dict of query string parameters.
    """
    r = requests.delete(self._path(path), auth=self.auth, params=params)
    r.raise_for_status()

  def _path(self, path):
    return '%s/api/rest/%s' % (self.url, path)

if __name__ == '__main__':
  vg = Voyager()
  print vg.locations()
