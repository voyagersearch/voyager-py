import unittest, sys, time
sys.path.append('../')

import pysolr
from voyager.client import *

class DataTest(unittest.TestCase):

  def setUp(self):
    self.cli = Client()
    self.solr = pysolr.Solr(self.cli.url + '/solr/v0')

  def test_features(self):
    loc = self.cli.add_location(Shapefile('data/states.shp', 'states'))
    self.assertIsNotNone(loc.id)

    loc.index()

    docs = self.query_and_wait('location:{}'.format(loc.id), rows=100)
    self.assertEquals(56, len(docs))

  def test_layers(self):
    loc = self.cli.add_location(Shapefile('data/states.shp'))
    self.assertIsNotNone(loc.id)

    loc.index();

    docs = self.query_and_wait('location:{}'.format(loc.id), rows=100)
    self.assertEquals(2, len(docs))

  def query_and_wait(self, q, **kwargs):
    for i in range(10):
      docs = self.solr.search(q, **kwargs)
      if len(docs) > 0:
        return docs

      print('Waiting for features to index...')
      time.sleep(5)