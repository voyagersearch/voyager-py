import requests, json
from location import Location
from spatial import Spatial

class Client(object):
  TOKEN_HEADER = 'x-access-token'
  TOKEN_EXG_HEADER = 'x-access-token-exchange'

  def __init__(self, url='http://localhost:8888', user='admin', passwd='admin'):
    """
    Voyager client object.

    :param url: Base url to voyager server.
    :param user: User to connect as.
    :param passwd: User password.
    """
    self.url = url;
    self.user = None
    self.token = None
    self.auth(user, passwd)

  def auth(self, user, passwd):
    """
    Authenticates with basic auth and obtains an api token.
    """
    rsp = self.get('auth/info', auth=(user,passwd))

    self.user = rsp['user']
    self.token = self.user['token']

  def system_status(self):
    """
    Returns status info about the voyager server.
    """
    return self.get('system/status')

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
    return Location(self, **self.get('discovery/location/{0}'.format(id)))

  def add_location(self, location):
    """
    Adds a new location.

    :param location: The location to add, an instance of of :class:`Location`.
    """
    return Location(self, **self.post('discovery/location', location.json))

  def remove_location(self, id):
    """
    Removes a location.

    :param id: Location id.
    """
    self.delete('discovery/location/{0}'.format(id))

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
    self.post('discovery/location/{0}/scan?delta={1}'.format(id,delta))

  def wipe_location(self, id):
    """
    Clears contents of a location.

    :param id: Location id.
    """
    self.delete('discovery/location/{0}/index'.format(id))

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

  def get(self, path, params=None, auth=None):
    """
    Makes an api call with the GET method.

    :param path: The relative api path, without the '/api/rest/' prefix.
    :param params: Optional dict of query string parameters.
    :param auth: Username/password tuple for basic auth, only used on initial connection.

    :returns: dict|array -- The request response body parsed as json.
    """
    rsp = self._request('GET', path, params=params, auth=auth)
    rsp.raise_for_status()
    return rsp.json()

  def post(self, path, body=None, params=None):
    """
    Makes an api call with the POST method.

    :param path: The relative api path, without the '/api/rest/' prefix.
    :param body: The optional request body as a json object.
    :param params: Optional dict of query string parameters.

    :returns: dict|array -- The request response body parsed as json.
    """
    rsp = self._request('POST', path, params=params, body=body)
    rsp.raise_for_status()
    return rsp.json()

  def delete(self, path, params=None):
    """
    Makes an api call with the DELETE method.

    :param path: The relative api path, without the '/api/rest/' prefix.
    :param params: Optional dict of query string parameters.
    """
    rsp = self._request('DELETE', path, params=params)
    rsp.raise_for_status()

  def _request(self, method, path, params=None, body=None, auth=None):
    headers = {}
    args = {}

    # query string params
    if params:
      args['params'] = params

    # request body, always assumed to be json
    if body:
      args['data'] = json.dumps(body) if isinstance(body, dict) else body
      headers['Content-Type'] = 'application/json'

    # authentication, if specified directly use basic auth, otherwise ensure we have a token
    if auth:
      args['auth'] = auth
    else:
      if self.token:
        headers['x-access-token'] = self.token
      else:
        raise Exception('Client has no authentication token and no credentials specified for basic auth')

    if len(headers) > 0:
      args['headers'] = headers

    # make the request
    url = '{0}/api/rest/{1}'.format(self.url, path)
    rsp = requests.request(method, url, **args)
    rsp.raise_for_status()

    # check for token exchange header
    try:
      self.token = rsp.headers[Client.TOKEN_EXG_HEADER]
    except KeyError:
      pass
    return rsp
