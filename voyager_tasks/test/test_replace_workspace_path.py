import os
import json
import unittest
import tempfile
import shutil
import sys
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
import arcpy


class TestReplaceWorkspacePath(unittest.TestCase):
    """Test case for Replace to Workspace task."""
    @classmethod
    def setUpClass(self):
        with open(os.path.join(os.getcwd(), 'replace_workspace_path.test.json')) as data_file:
            self.request = json.load(data_file)
        self.temp_folder = tempfile.mkdtemp()
        self.request['folder'] = self.temp_folder

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder, False)

    def test_layer(self):
        """Testing replace workspace path for a layer file"""
        shutil.copy2(os.path.join(os.getcwd(), 'test-data', 'Cities.lyr'), self.temp_folder)
        lyr_file = os.path.join(self.temp_folder, 'Cities.lyr')
        self.request['params'][0]['response']['docs'][0]['[lyrFile]'] = lyr_file
        __import__(self.request['task'])
        old_workspace = "C:\\GISData\\MDB\\USA.mdb"
        new_workspace = os.path.join(os.getcwd(), 'test-data', 'TestData_v10.gdb')
        self.request['params'][2]['value'] = old_workspace
        self.request['params'][3]['value'] = new_workspace
        getattr(sys.modules[self.request['task']], "execute")(self.request)
        dsc = arcpy.Describe(lyr_file)
        self.assertEquals(dsc.featureclass.catalogpath, os.path.join(new_workspace, 'cities'))

    def test_layer_backup_exists(self):
        """Test if backup (.bak) file was created"""
        self.assertTrue(os.path.exists(os.path.join(self.temp_folder, 'Cities.lyr.bak')))

if __name__ == '__main__':
    unittest.main()
