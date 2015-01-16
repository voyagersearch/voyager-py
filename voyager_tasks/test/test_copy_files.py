import os
import glob
import json
import unittest
import tempfile
import shutil
import sys
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.join(os.path.dirname(os.getcwd()), 'tasks'))


class TestCopyFilesTask(unittest.TestCase):
    """Test case for testing the zip files task."""
    @classmethod
    def setUpClass(self):
        with open(os.path.join(os.getcwd(), 'copyfiles.test.json')) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, False)

    def test_copy_files(self):
        """Testing the copy files task"""
        cad_file = os.path.join(os.getcwd(), 'test-data', 'cabottrail.DWG')
        txt_file = os.path.join(os.getcwd(), 'test-data', 'whypython.txt')
        kml_file = os.path.join(os.getcwd(), 'test-data', 'cities.kmz')
        shp_file = os.path.join(os.getcwd(), 'test-data', 'riverside.shp')
        sdc_file = os.path.join(os.getcwd(), 'test-data', 'mjrroads.sdc')
        self.request['params'][0]['response']['docs'][0]['path'] = cad_file
        self.request['params'][0]['response']['docs'][1]['path'] = txt_file
        self.request['params'][0]['response']['docs'][2]['path'] = kml_file
        self.request['params'][0]['response']['docs'][3]['path'] = shp_file
        self.request['params'][0]['response']['docs'][4]['path'] = sdc_file
        __import__(self.request['task'])
        self.request['params'][1]['value'] = self.temp_folder
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        copied_files = glob.glob(os.path.join(self.temp_folder, '*.*'))
        self.assertEqual(len(copied_files), 17)


if __name__ == '__main__':
    unittest.main()
