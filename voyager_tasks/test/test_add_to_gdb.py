import os
import json
import unittest
import tempfile
import shutil
import sys
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.join(os.path.dirname(os.getcwd()), 'tasks'))
import arcpy


class TestAddToGeodatabase(unittest.TestCase):
    """Test case for Add to Geodatabase task."""
    @classmethod
    def setUpClass(self):
        with open(os.path.join(os.getcwd(), 'add2gdb.test.json')) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, False)

    def test_add_to_gdb(self):
        """Testing the Add to Geodatabase task"""
        shp_file = os.path.join(os.getcwd(), 'test-data', 'riverside.shp')
        lyr_file = os.path.join(os.getcwd(), 'test-data', 'usstates.lyr')
        dbf_file = os.path.join(os.getcwd(), 'test-data', 'states.dbf')
        raster = os.path.join(os.getcwd(), 'test-data', 'raster', 'worldextent')
        dwg_file = os.path.join(os.getcwd(), 'test-data', 'cabottrail.DWG')
        self.request['params'][0]['response']['docs'][0]['path'] = shp_file
        self.request['params'][0]['response']['docs'][1]['path'] = lyr_file
        self.request['params'][0]['response']['docs'][2]['path'] = dbf_file
        self.request['params'][0]['response']['docs'][3]['path'] = raster
        self.request['params'][0]['response']['docs'][4]['path'] = dwg_file
        __import__(self.request['task'])
        target_gdb = arcpy.management.CreateFileGDB(self.temp_folder, 'test.gdb')
        self.request['params'][1]['value'] = target_gdb.getOutput(0)
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        num_items = len(arcpy.ListFeatureClasses())
        num_items += len(arcpy.ListRasters())
        self.assertEquals(9, num_items)


if __name__ == '__main__':
    unittest.main()
