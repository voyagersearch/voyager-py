import sys; sys.path.append('../')

import unittest
from voyager.client import Client

class AuthTest(unittest.TestCase):

    def test(self):
        cli = Client()
        self.assertIsNotNone(cli.user)
        self.assertIsNotNone(cli.token)

