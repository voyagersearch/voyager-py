__author__ = 'Jason'

import os
import unittest
import tempfile


class SampleTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_something(self):
        fileTemp = tempfile.NamedTemporaryFile(delete = False)
        try:
            fileTemp.write('Hello World!')
            fileTemp.close()
            b = os.path.getsize(fileTemp.name)
            self.assertEqual(10, b)
        finally:
            os.remove(fileTemp.name)

if __name__ == '__main__':
    unittest.main()
