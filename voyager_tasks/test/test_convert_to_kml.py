import os
import json
import unittest
import tempfile
import shutil
import sys
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

class TestConvertToKML(unittest.TestCase):
    """Test case for the Convert to KML task."""
    @classmethod
    def setUpClass(self):
        with open(os.path.join(os.getcwd(), 'convert2kml.test.json')) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, True)


    def test_convert_to_kml(self):
        """Testing the Convert to KML with WGS84 full extent as WKT"""
        lyr_file = os.path.join(os.getcwd(), 'test-data', 'usstates.lyr')
        shp_file = os.path.join(os.getcwd(), 'test-data', 'riverside.shp')
        raster = os.path.join(os.getcwd(), 'test-data', 'raster', 'worldextent')
        mxd_file = os.path.join(os.getcwd(), 'test-data', 'USA.mxd')
        self.request['params'][0]['response']['docs'][0]['[lyrFile]'] = lyr_file
        self.request['params'][0]['response']['docs'][1]['path'] = shp_file
        self.request['params'][0]['response']['docs'][2]['path'] = raster
        self.request['params'][0]['response']['docs'][3]['path'] = mxd_file
        __import__(self.request['task'])
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        byte_size = os.path.getsize(os.path.join(self.temp_folder, 'output.zip'))
        self.assertAlmostEquals(457788, byte_size, delta=5000)


if __name__ == '__main__':
    unittest.main()
