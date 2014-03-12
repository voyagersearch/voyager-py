import os
import json
import unittest
import tempfile
import shutil
import sys
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))


class TestZipFilesTask(unittest.TestCase):
    """Test case for testing the zip files task."""
    @classmethod
    def setUpClass(self):
        self.json_file = os.path.join(os.getcwd(), 'zipfiles.test.json')
        with open(self.json_file) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, False)

    def test_zip_files(self):
        """Testing the zip files task"""
        text_file = os.path.join(os.getcwd(), 'test-data', 'whypython.txt')
        dbf_file = os.path.join(os.getcwd(), 'test-data', 'usstates.dbf')
        kml_file = os.path.join(os.getcwd(), 'test-data', 'cities.kmz')
        non_file = os.path.join(os.getcwd(), 'test-data', 'emptyfolder')
        self.request['params'][0]['response']['docs'][0]['path'] = text_file
        self.request['params'][0]['response']['docs'][1]['path'] = dbf_file
        self.request['params'][0]['response']['docs'][2]['path'] = kml_file
        self.request['params'][0]['response']['docs'][3]['path'] = non_file
        __import__(self.request['task'])
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        byte_size = os.path.getsize(os.path.join(self.temp_folder, 'output.zip'))
        self.assertEqual(19619, byte_size)


if __name__ == '__main__':
    unittest.main()
