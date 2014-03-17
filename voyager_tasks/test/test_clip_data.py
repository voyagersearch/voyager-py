import os
import json
import unittest
import tempfile
import shutil
import sys
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
import arcpy


class TestClipData(unittest.TestCase):
    """Test case for the Clip Data task."""
    @classmethod
    def setUpClass(self):
        with open(os.path.join(os.getcwd(), 'clipdata.test.json')) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, True)

    def test_clip_wkt(self):
        """Testing the Clip Data with WGS84 full extent as WKT and output to file gdb"""
        shp_file = os.path.join(os.getcwd(), 'test-data', 'riverside.shp')
        arcpy.MakeFeatureLayer_management(os.path.join(os.getcwd(), 'test-data', 'usstates.lyr'), 'usstates')
        layer_folder = tempfile.mkdtemp(dir=self.temp_folder)
        lyr_file = arcpy.SaveToLayerFile_management('usstates', os.path.join(layer_folder, 'temp.lyr'))
        raster = os.path.join(os.getcwd(), 'test-data', 'raster', 'worldextent')
        dwg_file = os.path.join(os.getcwd(), 'test-data', 'cabottrail.DWG')
        self.request['params'][0]['response']['docs'][0]['path'] = shp_file
        self.request['params'][0]['response']['docs'][1]['[lyrFile]'] = lyr_file.getOutput(0)
        self.request['params'][0]['response']['docs'][2]['path'] = raster
        self.request['params'][0]['response']['docs'][3]['path'] = dwg_file
        __import__(self.request['task'])
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        num_items = len(arcpy.ListFeatureClasses())
        num_items += len(arcpy.ListRasters())
        #byte_size = os.path.getsize(os.path.join(self.temp_folder, 'output.zip'))
        self.assertAlmostEquals(9, num_items)


if __name__ == '__main__':
    unittest.main()
