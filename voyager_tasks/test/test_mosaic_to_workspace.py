import os
import json
import unittest
import tempfile
import shutil
import sys
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
import arcpy


class TestMosaicToWorkspace(unittest.TestCase):
    """Test case for Mosaic to Workspace task."""
    @classmethod
    def setUpClass(self):
        with open(os.path.join(os.getcwd(), 'mosaic_to_workspace.test.json')) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder
        __import__(self.request['task'])
        self.target_ws = arcpy.management.CreateFileGDB(self.temp_folder, 'test.gdb').getOutput(0)

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, False)

    def test_mosaic_to_workspace_FGDB(self):
        """Testing mosaic to workspace, creating a raster in a file geodatabase"""
        raster1 = os.path.join(os.getcwd(), 'test-data', 'raster', 'mosaic1')
        raster2 = os.path.join(os.getcwd(), 'test-data', 'raster', 'mosaic2')
        raster3 = os.path.join(os.getcwd(), 'test-data', 'raster', 'mosaic3')
        self.request['params'][1]['response']['docs'][0]['path'] = raster1
        self.request['params'][1]['response']['docs'][1]['path'] = raster2
        self.request['params'][1]['response']['docs'][2]['path'] = raster3
        self.request['params'][3]['value'] = 'Mosaic'
        self.request['params'][5]['value'] = 'FileGDB'
        self.request['params'][2]['value'] = self.target_ws
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        dsc = arcpy.Describe(os.path.join(self.target_ws, 'Mosaic'))
        self.assertEquals([dsc.bandcount, dsc.pixeltype], [1, 'U8'])

    def test_mosaic_to_workspace_IMG(self):
        """Testing mosaic to workspace, creating an IMG"""
        raster1 = os.path.join(os.getcwd(), 'test-data', 'raster', 'mosaic1')
        raster2 = os.path.join(os.getcwd(), 'test-data', 'raster', 'mosaic2')
        raster3 = os.path.join(os.getcwd(), 'test-data', 'raster', 'mosaic3')
        self.request['params'][1]['response']['docs'][0]['path'] = raster1
        self.request['params'][1]['response']['docs'][1]['path'] = raster2
        self.request['params'][1]['response']['docs'][2]['path'] = raster3
        self.request['params'][2]['value'] = self.temp_folder
        self.request['params'][3]['value'] = 'MosaicIMG'
        self.request['params'][5]['value'] = 'IMG'
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        dsc = arcpy.Describe(os.path.join(self.target_ws, 'Mosaic'))
        self.assertEquals([dsc.bandcount, dsc.pixeltype], [1, 'U8'])

if __name__ == '__main__':
    unittest.main()
