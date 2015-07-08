import os
from voyager.location.location import Location

class Path(Location):

  def __init__(self, name, path):
    Location.__init__(self, type='PATH', name=name, path=os.path.abspath(path),
      idMode='ABSOLUTE')