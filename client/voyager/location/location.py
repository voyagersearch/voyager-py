class Location(object):

  def __init__(self, client=None, **kwargs):
    """
    A Voyager location.

    :param client: An :class:`voyager.client.Client` instance.
    :param kwargs: Location properties.
    """
    self.client = client
    for k,v in kwargs.iteritems():
      setattr(self, k, v)

    if not kwargs.has_key('settings'):
      self.settings = {}

  def field(name, type, dest=None):
    if not hasattr(self, 'fields'):
      self.fields = []

    self.fields.append({
      'source': name,
      'dest': dest if dest is not None else 'meta_'+name,
      'type': type
    })
    return self

  def json(self):
    """
    Returns the location in a representation suitable to send to the server.
    """
    obj = dict(self.__dict__)
    if obj.has_key('client'):
      del obj['client']

    return obj

  def attach(self, client):
    """
    Attaches the location to the specified client.
    """
    self.client = client
    return self

  def index(self, delta=False):
    """
    Indexes the contents of the location.

    :param delta: Whether to index only changes (True) or build from scratch (False).
    """
    self.client.index_location(self.id, delta)

  def wipe(self):
    """
    Clears the contents of the location.
    """
    self.client.wipe_location(self.id)

  def remove(self):
    """
    Removes the location from Voyager.
    """
    self.client.remove_location(self.id)

  def __repr__(self):
    return str(self.__dict__)
