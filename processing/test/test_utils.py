import unittest
import json
import os
import sys
sys.path.append('../')
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
        """Test getting a map docmment data frame name without a layer name in the path"""
        expected_name = task_utils.get_data_frame_name(r"C:\GISData\mxds\USRivers.mxd | Layers").strip()
        self.assertEqual(expected_name, "Layers")

    def test_data_frame_name_with_layer(self):
        """Test getting a map docmment data frame name with a layer name in the path"""
        name = task_utils.get_data_frame_name('C:\GISData\mxds\USRivers.mxd | Layers\Rivers')
        self.assertEqual(name, 'Layers')

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

    def test_grouper(self):
        """Test grouping a list into chunks"""
        groups = task_utils.grouper(range(0, self.result_count), task_utils.CHUNK_SIZE, '')
        self.assertEqual(len(list(groups)), 5)

    def test_get_parameter_value(self):
        """Test to get a tasks parameter value"""
        param_value = task_utils.get_parameter_value(self.parameters, 'output_format')
        self.assertEqual(param_value, 'SHP')

    def test_get_geodatabase_path(self):
        """Test get geodatabase path with no feature dataset"""
        gdb_path = task_utils.get_geodatabase_path(r'c:\testdata\gdb1.gdb\tablename')
        self.assertEqual(gdb_path, r'c:\testdata\gdb1.gdb')

    def test_get_geodatabase_path_fds(self):
        """Test get geodatabase path with a feature dataset"""
        gdb_path = task_utils.get_geodatabase_path(r'c:\testdata\gdb1.gdb\fds\tablename')
        self.assertEqual(gdb_path, r'c:\testdata\gdb1.gdb')

    def test_get_unique_strings(self):
        """Test get unique strings from a list of strings"""
        tags = ['TEST', 'test', 'Test', 'TESTER', 'tester', 'Tester', 'VOYAGER']
        unique_strings = task_utils.get_unique_strings(tags)
        self.assertEqual(sorted(unique_strings), ['TEST', 'TESTER', 'VOYAGER'])

    # def test_dd_to_dms(self):
    #     """Test converting decimal degrees to degrees minutes seconds"""
    #     import arcview
    #     dms = task_utils.dd_to_dms(-56.553191)
    #     self.assertEqual(dms, (56, 33, 11.49))

    # def test_from_wkt_to_polygon(self):
    #     """Test converting WKT to a polygon object"""
    #     poly = task_utils.from_wkt('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))', 4326)
    #     extent_min = ('{0:.1f}'.format(poly.extent.XMax), '{0:.1f}'.format(poly.extent.YMax))
    #     self.assertEqual(extent_min, ('180.0', '90.0'))
    #
    # def test_get_spatial_reference(self):
    #     """Test getting a spatial reference from SR code"""
    #     sr = task_utils.get_spatial_reference(4326)
    #     self.assertEqual(sr.name, 'GCS_WGS_1984')
    #
    # def test_get_projection_file(self):
    #     """Test get the projection file name from SR code"""
    #     pf = task_utils.get_projection_file(4326)
    #     self.assertEqual(os.path.basename(pf), 'WGS 1984.prj')
    #
    # def test_get_clip_region(self):
    #     """Test getting a clip region from WKT"""
    #     wkt = 'MULTIPOLYGON (((-75.759298375698563 41.391337611891402, -75.759298375698563 49.022078452247342, -92.303148066299968 49.022078452247342, -92.303148066299968 41.391337611891402, -75.759298375698563 41.391337611891402)))'
    #     clip_region = task_utils.get_clip_region(wkt, 3857)
    #     extent_min = ('{0:.1f}'.format(clip_region.XMax), '{0:.1f}'.format(clip_region.YMax))
    #     self.assertEqual(extent_min, ('-8433486.5', '6278608.5'))

    def test_get_local_date(self):
        """Test gettting local date"""
        ld = task_utils.get_local_date()
        self.assertIsNotNone(ld)

    def test_zip_data(self):
        """Test zipping a folder"""
        import tempfile
        import shutil
        temp = tempfile.mkdtemp()
        zf = task_utils.zip_data(temp, 'test.zip')
        self.assertTrue(zf.endswith('.zip'))
        shutil.rmtree(temp)
