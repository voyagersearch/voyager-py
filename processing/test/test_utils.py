import unittest
import json
import os
import sys


sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(os.getcwd()))
sys.path.append(os.path.join(os.path.dirname(os.getcwd()), 'tasks'))
from tasks.utils import task_utils

class TestTaskUtils(unittest.TestCase):
    """Test case for testing the processing task utility functions."""
    @classmethod
    def setUpClass(self):
        self.fl = '&fl=id,name:[name],format,path:[absolute],[thumbURL],[lyrFile],[lyrURL],[downloadURL],[lyrURL]'
        self.query = 'http://localhost:8888/solr/v0/select?&wt=json{0}'.format(self.fl)
        self.items = None

        with open(os.path.join(os.getcwd(), 'test_utils.json')) as data_file:
            self.request = json.load(data_file)
        self.parameters = self.request['params']
        self.result_count, self.response_index = task_utils.get_result_count(self.parameters)
        self.qi = task_utils.QueryIndex(self.parameters[self.response_index])
        self.fq = self.qi.get_fq()
        self.query += self.fq
        self.service_layer = task_utils.ServiceLayer("http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/PublicSafety/PublicSafetyOperationalLayers/MapServer/5")


    @classmethod
    def tearDownClass(self):
        pass

    def test_result_count(self):
        """Tests the number of requests"""
        self.assertEqual(self.result_count, 119)

    def test_query_string(self):
        """Tests getting the query string from the request"""
        expected = 'http://localhost:8888/solr/v0/select?&wt=json&fl=id,name:[name],format,path:[absolute],[thumbURL],[lyrFile],[lyrURL],[downloadURL],[lyrURL]&fq=location:baad8134e9644fc7&q=id:(25107622%20T14C47A34AF9_states_1%20T14C47A34AF9_states_8%20de71138aeb803dae%20df08413d8acdaba2%208fc582b9845105f8%202510bce8e885b1dc_0003%203adbb78602e5df2b%20726262e2a1b25862_0000%20a3fbb3a1f9ed41f8be16abc384673372),usa'
        self.assertEqual(self.query, expected)

    def test_get_items(self):
        """Tests the get_items and get_data_path functions"""
        self.items = task_utils.get_input_items([{'path': os.path.join(os.getcwd(), 'test-data', 'usstates.shp'), 'name': 'USStates'},
                      {'path': os.path.join(os.getcwd(), 'test-data', 'USA.mxd'), 'name': 'USA'},
                      {'path':'', '[lyrFile]': os.path.join(os.getcwd(), 'test-data', 'Cities.lyr'), 'name': 'Cities', 'format': ''}])

        expected_items = {'{0}\\test-data\\usstates.shp'.format(os.getcwd()): 'USStates',
                          '{0}\\test-data\\USA.mxd'.format(os.getcwd()): 'USA',
                          '{0}\\test-data\\Cities.lyr'.format(os.getcwd()): 'Cities'}
        self.assertDictEqual(expected_items, self.items)

    def test_list_files(self):
        """Tests get the list of component files"""
        shp_files = ('shp', 'shx', 'sbn', 'dbf', 'prj', 'cpg', 'shp.xml', 'dbf.xml')
        files = task_utils.list_files(os.path.join(os.getcwd(), 'test-data', 'usstates.shp'), shp_files)
        expected_files = ['{0}\\test-data\\usstates.shp'.format(os.getcwd()),
                           '{0}\\test-data\\usstates.shx'.format(os.getcwd()),
                           '{0}\\test-data\\usstates.sbn'.format(os.getcwd()),
                           '{0}\\test-data\\usstates.dbf'.format(os.getcwd()),
                           '{0}\\test-data\\usstates.prj'.format(os.getcwd()),
                           '{0}\\test-data\\usstates.cpg'.format(os.getcwd()),
                           '{0}\\test-data\\usstates.shp.xml'.format(os.getcwd())]
        self.assertItemsEqual(expected_files, files)

    def test_get_increment(self):
        """Test gettig a suitable base 10 increment"""
        increment = task_utils.get_increment(43567)
        self.assertEqual(increment, 1000)

    def test_data_frame_name(self):
        """Test getting a map docmment data frame name"""
        expected_name = task_utils.get_data_frame_name(r"C:\GISData\mxds\USRivers.mxd | Layers").strip()
        self.assertEqual(expected_name, "Layers")

    def test_service_layer_wkid(self):
        """Test getting the service layer WKID"""
        wkt = self.service_layer.wkid
        expected_wkt = 4326
        self.assertEqual(expected_wkt, wkt)

    def test_service_layer_objectids(self):
        """Test get the service layer objectids"""
        ids = self.service_layer.object_ids
        id_count = sum([len(group) for group in ids])
        expected_count = 100
        self.assertEqual(expected_count, id_count)

    def test_service_layer_oid_field_name(self):
        """Test getting the service lyaer object id field"""
        oid_field_name = self.service_layer.oid_field_name
        expected_name = "OBJECTID"
        self.assertEqual(expected_name, oid_field_name)
