import os
import json
import unittest
import tempfile
import shutil
import sys
import zipfile
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))


class TestZipFilesTask(unittest.TestCase):
    """Test case for testing the zip files task."""
    @classmethod
    def setUpClass(self):
        with open(os.path.join(os.getcwd(), 'zipfiles.test.json')) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, False)

    def test_zip_files(self):
        """Testing the zip files task"""
        base_zip_files = ['whypython.txt', 'states.dbf', 'cities.kmz']

        text_file = os.path.join(os.getcwd(), 'test-data', 'whypython.txt')
        dbf_file = os.path.join(os.getcwd(), 'test-data', 'states.dbf')
        kml_file = os.path.join(os.getcwd(), 'test-data', 'cities.kmz')
        #non_file = os.path.join(os.getcwd(), 'test-data', 'emptyfolder')
        self.request['params'][0]['response']['docs'][0]['path'] = text_file
        self.request['params'][0]['response']['docs'][1]['path'] = dbf_file
        self.request['params'][0]['response']['docs'][2]['path'] = kml_file
        #self.request['params'][0]['response']['docs'][3]['path'] = non_file
        __import__(self.request['task'])
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        zip_files = zipfile.ZipFile(os.path.join(self.temp_folder, 'output.zip')).namelist()
        self.assertEqual(sorted(zip_files), sorted(base_zip_files))


if __name__ == '__main__':
    unittest.main()
