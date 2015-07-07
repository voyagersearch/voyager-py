from voyager.location.location import Location

class Feed(Location):

  def __init__(self, name, uri):
    Location.__init__(self, type='Feed', name=name, uri=uri)
