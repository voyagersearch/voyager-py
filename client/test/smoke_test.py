import unittest, sys
sys.path.append('../')

import voyager

class SmokeTest(unittest.TestCase):

  def setUp(self):
    self.cli = voyager.client.Client()

  def test_system_status(self):
    status = self.cli.system_status()
    self.assertIsNotNone(status)

