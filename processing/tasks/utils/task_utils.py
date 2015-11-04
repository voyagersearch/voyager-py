# (C) Copyright 2014 Voyager Search
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import re
import glob
import json
import math
import locale
import shutil
import datetime
import itertools
import urllib
import time
import zipfile
import status

# Constants
CHUNK_SIZE = 25


# Custom Exceptions
class LicenseError(Exception):
    pass

class AnalyzeServiceException(Exception):
    pass


class PublishException(Exception):
    pass


class ZipFileManager(zipfile.ZipFile):
    """Context manager for zip files. Added to support using
    a with statement with Python 2.6 installed with ArcGIS 10.0.
    """
    def __init__(self, zip_file, mode='r', compression=zipfile.ZIP_DEFLATED):
        zipfile.ZipFile.__init__(self, zip_file, mode, compression)

    def __enter__(self):
        """Return object created in __init__ part"""
        return self

    def __exit__(self, exc_type, exc_value, trace_back):
        """Close zipfile.ZipFile"""
        self.close()


class QueryIndex(object):
    """Helper class for querying the Voyager index."""
    def __init__(self, items):
        self._items = items
        self._fq = ''

    @property
    def fl(self):
        return '&fl=id,name:[name],title,format,path:[absolute],f*,[lyrFile],[lyrURL],[downloadURL],[lyrURL],[geo],location'

    def get_fq(self):
        """Return the query request string if the results are from a
         voyager list or a bbox.
         """
        if 'query' in self._items:
            if 'voyager.list' in self._items['query']:
                self._fq = "&voyager.list={0}".format(self._items['query']['voyager.list'])

            if 'fq' in self._items['query']:
                if isinstance(self._items['query']['fq'], list):
                    self._fq += '&fq={0}'.format('&fq='.join(self._items['query']['fq']).replace('\\', ''))
                    self._fq = self._fq.replace(' ', '%20')
                else:
                    # Replace spaces with %20 & remove \\ to avoid HTTP Error 400.
                    self._fq += '&fq={0}'.format(self._items['query']['fq'].replace("\\", ""))
                    self._fq = self._fq.replace(' ', '%20')

            if 'q' in self._items['query']:
                if self._items['query']['q'].startswith('id:'):
                    ids = self._items['query']['q']
                    self._fq += '&q={0}'.format(ids)
                    self._fq = self._fq.replace(' ', '%20')
                else:
                    self._fq += '&q={0}'.format(self._items['query']['q'].replace("\\", ""))
                    self._fq = self._fq.replace(' ', '%20')

            if 'place' in self._items['query']:
                self._fq += '&place={0}'.format(self._items['query']['place'].replace("\\", ""))
                self._fq = self._fq.replace(' ', '%20')
            if 'place.op' in self._items['query']:
                self._fq += '&place.op={0}'.format(self._items['query']['place.op'])

        elif 'ids' in self._items:
            ids = self._items['ids']
            self._fq += '&fq=({0})'.format(','.join(ids))
            self._fq = self._fq.replace(' ', '%20')
        return self._fq


class ServiceLayer(object):
    """Helper class for working with ArcGIS services."""
    def __init__(self, service_layer_url, geometry='', geometry_type='', token=''):
        self.object_ids_cnt = 0
        self._service_layer_url = service_layer_url
        self._token = token
        self._wkid = self.__get_wkid()
        self._oid_field_name = ''
        self._object_ids = self.__get_object_ids(geometry, geometry_type)

    @property
    def service_layer_url(self):
        return self._service_layer_url

    @property
    def oid_field_name(self):
        return self._oid_field_name

    @property
    def object_ids(self):
        return self._object_ids

    @property
    def token(self):
        return self._token

    @property
    def wkid (self):
        return self._wkid

    def __get_wkid(self):
        """Returns the spatial reference wkid for the service layer."""
        if self._token:
            query = {'where': '1=1', 'returnExtentOnly':True, 'token': self._token, 'f': 'json'}
        else:
            query = {'where': '1=1', 'returnExtentOnly':True, 'f': 'json'}
        response = urllib.urlopen('{0}/query?'.format(self._service_layer_url), urllib.urlencode(query))
        data = json.loads(response.read())
        if 'extent' in data:
            return data['extent']['spatialReference']['wkid']
        else:
            return 4326

    def __get_object_ids(self, geom, geom_type):
        """Returns groups of OIDs/FIDs for the service layer as an iterator (groups of 100)."""
        if self._token:
            query = {'where': '1=1', 'returnIdsOnly':True, 'geometry': geom, 'geometryType': geom_type, 'token': self._token, 'f': 'json'}
        else:
            query = {'where': '1=1', 'geometry': geom, 'geometryType': geom_type, 'returnIdsOnly':True, 'f': 'json'}
        response = urllib.urlopen('{0}/query?'.format(self._service_layer_url), urllib.urlencode(query))
        data = json.loads(response.read())
        if 'error' in data:
            if data['error']['code'] == 400:
                raise Exception('Service Layer has no records')
        if 'layers' in data:
            raise Exception('Not a service layer')
        objectids = data['objectIds']

        if not objectids:
            return None
        self._oid_field_name = data['objectIdFieldName']
        args = [iter(objectids)] * 100
        self.object_ids_cnt = len(list(itertools.izip_longest(fillvalue=None, *args)))
        args = [iter(objectids)] * 100
        return itertools.izip_longest(fillvalue=None, *args)


def create_unique_name(name, gdb):
    """Creates and returns a valid and unique name for the geodatabase.
    :param name: name to be validated
    :param gdb: workspace path
    :rtype : str
    """
    import arcpy
    if gdb.endswith('.gdb'):
        valid_name = arcpy.ValidateTableName(name, gdb)
        unique_name = arcpy.CreateUniqueName(valid_name, gdb)
    else:
        unique_name = arcpy.CreateUniqueName(name + '.shp', gdb)
    return unique_name


def create_lpk(data_location, additional_files=None):
    """Creates a layer package (.lpk) for all datasets in the data location.
    :param data_location: location of data to packaged
    :param additional_files: list of additional files to include in the package
    """
    import arcpy
    # Ensure existing layer files have a description.
    lyr_files = glob.glob(os.path.join(data_location, '*.lyr'))
    for lyr in lyr_files:
        layer = arcpy.mapping.Layer(lyr)
        if layer.description == '':
            layer.description = layer.name
            layer.save()

    # Save data to layer files.
    save_to_layer_file(data_location, False)

    # Package all layer files.
    layer_files = glob.glob(os.path.join(data_location, '*.lyr'))
    arcpy.PackageLayer_management(layer_files, os.path.join(os.path.dirname(data_location), 'output.lpk'),
                                  'PRESERVE', version='10', additional_files=additional_files)
    make_thumbnail(layer_files[0], os.path.join(os.path.dirname(data_location), '_thumb.png'))


def create_mpk(data_location, mxd, additional_files=None):
    """Creates a map package (.mpk) for all the datasets in the data location.
    :param data_location: location of data to be packaged
    :param mxd: existing map document template (mxd path)
    :param additional_files: list of additional files to include in the package
    """
    import arcpy
    arcpy.PackageMap_management(mxd, mxd.replace('.mxd', '.mpk'),
                                'PRESERVE', version='10', additional_files=additional_files)
    make_thumbnail(mxd, os.path.join(os.path.dirname(data_location), '_thumb.png'))


def create_mxd(data_location, map_template, output_name):
    """Creates a map document (.mxd) for all the datasets in the data locaton.
    :param data_location: location of data to be packaged
    :param map_template:
    :param output_name:
    """
    import arcpy
    shutil.copyfile(map_template, os.path.join(data_location, "{0}.mxd".format(output_name)))
    mxd = arcpy.mapping.MapDocument(os.path.join(data_location, "{0}.mxd".format(output_name)))
    if mxd.description == '':
        mxd.description = os.path.basename(mxd.filePath)
    df = arcpy.mapping.ListDataFrames(mxd)[0]

    types = ('*.shp', '*.gdb', '*.mxd', '*.lyr')
    all_data = []
    for files in types:
        all_data.extend(glob.glob(os.path.join(data_location, files)))
    for ds in all_data:
        if ds.endswith('.shp'):
            # Add all shapefiles to the mxd template.
            layer = arcpy.MakeFeatureLayer_management(ds, '{0}_'.format(os.path.basename(ds)[:-3]))
            arcpy.mapping.AddLayer(df, layer.getOutput(0))
        elif ds.endswith('.gdb'):
            # Add all feature classes to the mxd template.
            arcpy.env.workspace = ds
            feature_datasets = arcpy.ListDatasets('*', 'Feature')
            if feature_datasets:
                for fds in feature_datasets:
                    arcpy.env.workspace = fds
                    for fc in arcpy.ListFeatureClasses():
                        layer = arcpy.MakeFeatureLayer_management(fc, '{0}_'.format(fc))
                        arcpy.mapping.AddLayer(df, layer.getOutput(0))
                arcpy.env.workspace = ds
            for fc in arcpy.ListFeatureClasses():
                layer = arcpy.MakeFeatureLayer_management(fc, '{0}_'.format(fc))
                arcpy.mapping.AddLayer(df, layer.getOutput(0))
            for raster in arcpy.ListRasters():
                layer = arcpy.MakeRasterLayer_management(raster, '{0}_'.format(raster))
                arcpy.mapping.AddLayer(df, layer.getOutput(0))
        elif ds.endswith('.lyr'):
            # Add all layer files to the mxd template.
            arcpy.mapping.AddLayer(df, arcpy.mapping.Layer(ds))
    mxd.save()
    return  mxd.filePath


def convert_to_kml(geodatabase):
    """Convert the contents of a geodatabase to KML.
    :param geodatabase: path to a geodatabase
    """
    import arcpy
    status_writer = status.Writer()
    arcpy.env.workspace = geodatabase
    arcpy.env.overwriteOutput = True
    feature_classes = arcpy.ListFeatureClasses()
    count = len(feature_classes)
    for i, fc in enumerate(feature_classes, 1):
        arcpy.MakeFeatureLayer_management(fc, "temp_layer")
        arcpy.LayerToKML_conversion("temp_layer", '{0}.kmz'.format(os.path.join(os.path.dirname(geodatabase), fc)), 1)
        status_writer.send_percent(float(i) / count, _('Converted: {0}').format(fc), 'convert_to_kml')
    arcpy.Delete_management("temp_layer")


def clip_layer_file(layer_file, aoi, workspace):
    """Clips each layer in the layer file to the output workspace and re-sources each layer and saves a copy.
    :param layer_file: path to the layer file
    :param aoi: area of interest (as a polygon object of feature class)
    :param workspace: output workspace path
    """
    import arcpy
    arcpy.env.workspace = workspace
    if workspace.endswith('.gdb'):
        layer_path = os.path.join(os.path.dirname(workspace), os.path.basename(layer_file))
    else:
        layer_path = os.path.join(workspace, os.path.basename(layer_file))
    shutil.copyfile(layer_file, layer_path)
    layer_from_file = arcpy.mapping.Layer(layer_path)
    layers = arcpy.mapping.ListLayers(layer_from_file)
    for layer in layers:
        if layer.isFeatureLayer:
            name = create_unique_name(layer.name, workspace)
            arcpy.Clip_analysis(layer.dataSource, aoi, name)
            if workspace.endswith('.gdb'):
                layer.replaceDataSource(workspace, 'FILEGDB_WORKSPACE',
                                        os.path.splitext(os.path.basename(name))[0], False)
            else:
                layer.replaceDataSource(workspace, 'SHAPEFILE_WORKSPACE', os.path.basename(name), False)
        elif layer.isRasterLayer:
            name = create_unique_name(layer.name, workspace)
            if isinstance(aoi, arcpy.Polygon):
                extent = aoi.extent
            else:
                extent = arcpy.Describe(aoi).extent
            ext = '{0} {1} {2} {3}'.format(extent.XMin, extent.YMin, extent.XMax, extent.YMax)
            arcpy.Clip_management(layer.dataSource, ext, os.path.splitext(os.path.basename(name))[0],
                                  in_template_dataset=aoi, clipping_geometry="ClippingGeometry")

            if workspace.endswith('.gdb'):
                layer.replaceDataSource(workspace, 'FILEGDB_WORKSPACE',
                                        os.path.splitext(os.path.basename(name))[0], False)
            else:
                layer.replaceDataSource(workspace, 'RASTER_WORKSPACE',
                                        os.path.splitext(os.path.basename(name))[0], False)
        if layer.description == '':
            layer.description = layer.name
        # Catch assertion error if a group layer.
        try:
            layer.save()
        except AssertionError:
            layers[0].save()
            pass


def clip_mxd_layers(mxd_path, aoi, workspace, map_frame=None):
    """Clips each layer in the map document to output workspace
    and re-sources each layer and saves a copy of the mxd.
    :param mxd_path: path to the input map document
    :param aoi: area of interest as a polygon object or feature class
    :param workspace: output workspace
    :param map_frame: name of the mxd's map frame/data frame
    """
    import arcpy
    arcpy.env.workspace = workspace
    status_writer = status.Writer()
    mxd = arcpy.mapping.MapDocument(mxd_path)
    layers = arcpy.mapping.ListLayers(mxd)
    if map_frame:
        df = arcpy.mapping.ListDataFrames(mxd, map_frame)[0]
        df_layers = arcpy.mapping.ListLayers(mxd, data_frame=df)
        [[arcpy.mapping.RemoveLayer(d, l) for l in arcpy.mapping.ListLayers(mxd)] for d in arcpy.mapping.ListDataFrames(mxd)]
        [arcpy.mapping.AddLayer(df, l) for l in df_layers]
        layers = df_layers
    for layer in layers:
        try:
            out_name = arcpy.CreateUniqueName(layer.datasetName, arcpy.env.workspace)
            if layer.isFeatureLayer:
                arcpy.Clip_analysis(layer.dataSource, aoi, out_name)
                if arcpy.env.workspace.endswith('.gdb'):
                    layer.replaceDataSource(arcpy.env.workspace, 'FILEGDB_WORKSPACE', out_name, False)
                else:
                    layer.replaceDataSource(arcpy.env.workspace, 'SHAPEFILE_WORKSPACE', out_name, False)

            elif layer.isRasterLayer:
                ext = '{0} {1} {2} {3}'.format(aoi.extent.XMin, aoi.extent.YMin, aoi.extent.XMax, aoi.extent.YMax)
                arcpy.Clip_management(layer.dataSource, ext, out_name)
                if arcpy.env.workspace.endswith('.gdb'):
                    layer.replaceDataSource(arcpy.env.workspace, 'FILEGDB_WORKSPACE', out_name, False)
                else:
                    layer.replaceDataSource(arcpy.env.workspace, 'RASTER_WORKSPACE', out_name, False)
        except arcpy.ExecuteError:
            status_writer.send_state(status.STAT_WARNING, arcpy.GetMessages(2))

    # Save a new copy of the mxd with all layers clipped and re-sourced.
    if mxd.description == '':
        mxd.description = os.path.basename(mxd.filePath)
    if arcpy.env.workspace.endswith('.gdb'):
        new_mxd = os.path.join(os.path.dirname(arcpy.env.workspace), os.path.basename(mxd.filePath))
    else:
        new_mxd = os.path.join(arcpy.env.workspace, os.path.basename(mxd.filePath))
    mxd.saveACopy(new_mxd)
    del mxd


def dd_to_dms(dd):
    """Convert decimal degrees to degrees, minutes, seconds.
    :param dd: decimal degrees as float
    :rtype : tuple of degrees, minutes, seconds
    """
    dd = abs(dd)
    minutes, seconds = divmod(dd*3600, 60)
    degrees, minutes = divmod(minutes, 60)
    seconds = float('{0:.2f}'.format(seconds))
    return int(degrees), int(minutes), seconds


def get_local_date():
    """Returns formatted local date.
    :rtype : str
    """
    locale.setlocale(locale.LC_TIME)
    d = datetime.datetime.today()
    return d.strftime('%x')


def get_clip_region(clip_area_wkt, out_coordinate_system=None):
    """Creates and returns an extent representing the clip region from WKT.

    :param clip_area_wkt: Well-known text representing clip extent
    :param out_coordinate_system: The coordinate system of the output extent
    :rtype : arcpy.Extent
    """
    import arcpy
    # WKT coordinates for each task are always WGS84.
    gcs_sr = get_spatial_reference(4326)
    clip_area = from_wkt(clip_area_wkt, gcs_sr)
    if not clip_area.area > 0:
        clip_area = from_wkt('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))', gcs_sr)
    if out_coordinate_system:
        out_sr = get_spatial_reference(int(out_coordinate_system))
        if not out_sr.name == gcs_sr.name:
            try:
                geo_transformation = arcpy.ListTransformations(gcs_sr, out_sr)[0]
                clip_area = clip_area.projectAs(out_sr, geo_transformation)
            except AttributeError:
                clip_area = clip_area.projectAs(out_sr)
            except IndexError:
                clip_area = clip_area.projectAs(out_sr)
    return clip_area.extent


def get_result_count(parameters):
    """Returns the number of results and the response index."""
    count, i = 0, 0
    for i, parameter in enumerate(parameters):
        if 'response' in parameter:
            count = parameter['response']['numFound']
            break
    return count, i


def grouper(iterable, n, fill_value=None):
    """Collect data into fixed-length chunks or blocks.
    :param iterable: input iterable (list, etc.)
    :param n: number of chunks/blocks
    :param fillvalue: value for remainder values
    :return: izip_longest object

    e.g. grouper([1,2,3,4], 2, 'end') --> (1,2) (2,3) 'end'
    """
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fill_value, *args)


def list_files(source_file, file_extensions):
    """Returns a list of files for each file type.

    :param source_file: source file path
    :param file_extensions: list of file extensions - i.e. ['*.shp', '*.prj']
    :rtype : list
    """
    folder_location = os.path.dirname(source_file)
    file_name = os.path.splitext(os.path.basename(source_file))[0]
    all_files = []
    for ext in file_extensions:
        all_files.extend(glob.glob(os.path.join(folder_location, '{0}.{1}'.format(file_name, ext))))
    return all_files


def get_parameter_value(parameters, parameter_name, value_key='value'):
    """Returns the parameter value.

    :param parameters: parameter list
    :param parameter_name: parameter name
    :param value_key: parameter key containing the value
    :rtype : str
    """
    for item in parameters:
        if item['name'] == parameter_name:
            try:
                param_value = item[value_key]
                return param_value
            except KeyError:
                return ''


def get_input_items(parameters, list_ids=False, list_components=False):
    """Get the input search result items and output names.

    :param: parameters: parameter list
    :rtype: dict
    """
    results = {}
    docs = parameters
    try:
        for i in docs:
            try:
                if list_ids:
                    results[get_data_path(i)] = (i['name'], i['id'])
                else:
                    results[get_data_path(i)] = i['name']
            except KeyError:
                if list_ids:
                    results[get_data_path(i)] = ('', i['id'])
                else:
                    results[get_data_path(i)] = ''
            except IOError:
                continue
            if list_components and 'component_files' in i:
                    for c in i['component_files']:
                        results[os.path.join(os.path.dirname(i['path']), c)] = ''
    except IOError:
        pass
    return results


def get_increment(count):
    """Returns a suitable base 10 increment."""
    p = int(math.log10(count))
    if not p:
        p = 1
    return int(math.pow(10, p - 1))


def get_geodatabase_path(input_table):
  """Return the Geodatabase path from the input table or feature class.

  :param input_table: path to the input table or feature class
  """
  workspace = os.path.dirname(input_table)
  if [any(ext) for ext in ('.gdb', '.mdb', '.sde') if ext in os.path.splitext(workspace)]:
    return workspace
  else:
    return os.path.dirname(workspace)


def get_data_path(item):
    """Return the layer file or dataset path.

    :param item: dataset path
    :rtype : str
    """
    try:
        # If input is a Geodatabase feature class.
        if [any(ext) for ext in ('.sde', '.gdb', '.mdb', '.lyr') if ext in item['path']]:
            import arcpy
            if arcpy.Exists(item['path']):
                return item['path']

        if os.path.exists(item['path']):
            return item['path']
        elif item['path'].startswith('http'):
            return item['path']
        elif item['format'] == 'format:application/vnd.esri.lyr':
            return item['[absolute]']
        elif item['format'] == 'application/vnd.esri.map.data.frame':
            return item['path']
        elif os.path.exists(item['[lyrFile]']):
            return item['[lyrFile]']
        else:
            layer_file = urllib.urlretrieve(item['[lyrURL]'])[0]
            return layer_file
    except KeyError:
        try:
            # It may be Esri geodatabase data, Esri GRID or Esri Coverage.
            if os.path.splitext(item['path'])[1] == '':
                import arcpy
                if arcpy.Exists(item['path']):
                    return item['path']
                else:
                    raise IOError
            else:
                raise IOError
        except (KeyError, IOError, ImportError):
            raise IOError


def get_data_frame_name(path):
    """Return the mxd's data frame name by removing voyager's index potion of the path.

    :param path: The map frame path from Voyager
    """
    map_frame_name = None
    if '|' in path:
        map_frame_name = path.split('|')[1]
        match = re.search('[[0-9]]', map_frame_name)
        if match:
            map_frame_name = map_frame_name.replace(map_frame_name[match.start()-1:match.end()], '').strip()
    return map_frame_name


def from_wkt(wkt, sr):
    """Creates a polygon geometry from a list of well-known text coordinates.

    :param wkt: well-known text
    :param sr: arcpy spatial reference object
    :rtype : arcpy.Polygon
    """
    import arcpy
    coordinates = wkt[wkt.find('(') + 2: wkt.find(')')].split(',')
    array = arcpy.Array()
    for p in coordinates:
        pt = p.strip().split(' ')
        array.add(arcpy.Point(float(pt[0]), float(pt[1])))
    poly = arcpy.Polygon(array, sr)
    return poly


def get_spatial_reference(factory_code):
    """Returns spatial reference object.

    :param factory_code: The projection's factory code - i.e. 4326 for WGS84
    :rtype : arcpy.SpatialReference
    """
    import arcpy
    try:
        sr = arcpy.SpatialReference(int(factory_code))
    except RuntimeError:
        sr = arcpy.SpatialReference(get_projection_file(factory_code))
    return sr


def get_security_token(owner_info):
    if 'token' in owner_info:
        return owner_info['token']
    else:
        return ''

def get_projection_file(factory_code):
    """Returns a projection file using the factory code as a lookup.
    This function adds support for ArcGIS 10.0.

    :param factory_code: The projection's factory code - i.e. 4326 is the code for WGS84
    :rtype : str
    """
    import arcpy
    lu_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'supportfiles', 'projection_files.json')
    with open(lu_file) as fp:
        prj_lu = json.load(fp)
        arcgis_folder = arcpy.GetInstallInfo()['InstallDir']
        prj_file = os.path.join(arcgis_folder, prj_lu['{0}'.format(factory_code)])
    return prj_file


def make_thumbnail(layer_or_mxd, output_png_file, use_data_frame=True):
    """Creates a thumbnail PNG file for the layer or map document.

    :param layer_or_mxd: a (layer object or file) or (map document object or file)
    :param output_png_file: the path for the output PNG
    :param use_data_frame: Use the data frame of the map
    :rtype : str
    """
    import arcpy
    if hasattr(layer_or_mxd, 'filePath'):
        mxd = layer_or_mxd
        data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
    elif hasattr(layer_or_mxd, 'name'):
        layer = layer_or_mxd
        mxd = arcpy.mapping.MapDocument(
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'supportfiles', 'MapTemplate.mxd')
        )
        data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
        arcpy.mapping.AddLayer(data_frame, layer)
    elif layer_or_mxd.endswith('.mxd'):
        mxd = arcpy.mapping.MapDocument(layer_or_mxd)
        data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
    elif layer_or_mxd.endswith('.lyr'):
        mxd = arcpy.mapping.MapDocument(
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'supportfiles', 'MapTemplate.mxd')
        )
        data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
        layer = arcpy.mapping.Layer(layer_or_mxd)
        arcpy.mapping.AddLayer(data_frame, layer)
    else:
        return
    if use_data_frame:
        arcpy.mapping.ExportToPNG(mxd, output_png_file, data_frame, 150, 150, 10)
    else:
        arcpy.mapping.ExportToPNG(mxd, output_png_file, '', 150, 150, 10)


def report(report_file, num_processed=0, num_skipped=0, num_errors=0, errors_details=None, skipped_details=None, num_warnings=0, warnings_details=None):
    """Create a markdown report of inputs processed or skipped.
    :param report_file: path of the .json file
    :param num_processed: number of items processed
    :param num_skipped: number of items skipped
    :param num_errors:  number of errors
    """
    report_dict = {}
    summary_list = [{'Action': 'Processed', 'Count': num_processed},
                    {'Action': 'Skipped', 'Count': num_skipped},
                    {'Action': 'Errors', 'Count': num_errors},
                    {'Action': 'Warnings', 'Count': num_warnings}]
    report_dict['Summary'] = summary_list

    if warnings_details:
        warnings_list = []
        for k, v in warnings_details.iteritems():
            warnings_list.append({'Item': k, 'Reason': v})
        report_dict['Warnings'] = warnings_list

    if skipped_details:
        skipped_list = []
        for k, v in skipped_details.iteritems():
            skipped_list.append({'Item': k, 'Reason': v})
        report_dict['Skipped'] = skipped_list

    if errors_details:
        errors_list = []
        for k, v in errors_details.iteritems():
            errors_list.append({'Item': k, 'Reason': v})
        report_dict['Errors'] = errors_list

    with open(report_file, 'wb') as fp:
        json.dump(report_dict, fp)



def save_to_layer_file(data_location, include_mxd_layers=True):
    """Saves all data from the data location to layer files.

    :param data_location: folder containing data to be saved as layer files
    :param include_mxd_layers: save layers in mxd's to layer files - default is True
    """
    import arcpy
    file_gdbs = glob.glob(os.path.join(data_location, '*.gdb'))
    for file_gdb in file_gdbs:
        arcpy.env.workspace = file_gdb
        feature_datasets = arcpy.ListDatasets('*', 'Feature')
        if feature_datasets:
            for fds in feature_datasets:
                arcpy.env.workspace = fds
                for fc in arcpy.ListFeatureClasses():
                    fl = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
                    arcpy.management.SaveToLayerFile(fl, os.path.join(data_location, '{0}.lyr'.format(fc)))
            arcpy.env.workspace = file_gdb
        for fc in arcpy.ListFeatureClasses():
            fl = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
            arcpy.management.SaveToLayerFile(fl, os.path.join(data_location, '{0}.lyr'.format(fc)))
        for raster in arcpy.ListRasters():
            rl = arcpy.MakeRasterLayer_management(raster, '{0}_'.format(raster))
            arcpy.management.SaveToLayerFile(rl, os.path.join(data_location, '{0}.lyr'.format(raster)))

    if include_mxd_layers:
        mxd_files = glob.glob(os.path.join(data_location, '*.mxd'))
        for mxd_file in mxd_files:
            mxd = arcpy.mapping.MapDocument(mxd_file)
            layers = arcpy.mapping.ListLayers(mxd)
            for layer in layers:
                if layer.description == '':
                    layer.description = layer.name
                arcpy.management.SaveToLayerFile(layer, os.path.join(data_location, '{0}.lyr'.format(layer.name)))


def time_it(func):
    """A timer decorator - use this to time a function."""
    def timed(*args, **kwargs):
        ts = time.time()
        result = func(*args, **kwargs)
        te = time.time()
        status_writer = status.Writer()
        status_writer.send_status('func:{} args:[{}, {}] took: {:.2f} sec'.format(func.__name__, args, kwargs, te-ts))
        return result
    return timed


def get_unique_strings(input_strings):
    """Returns unique strings with preference given to uppercase strings."""
    seen = {}  # Using {} since the order is not important.
    for s in input_strings:
        l = s.lower()
        seen[l] = min(s, seen.get(l, s))
    return seen.values()


def zip_data(data_location, name):
    """Creates a compressed zip file of the entire data location.

    :param data_location: folder containing data to be zipped
    :param name: name of zip file
    :rtype : str
    """
    zfile = os.path.join(data_location, name)
    with ZipFileManager(zfile, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(data_location):
            for f in files:
                if not f.endswith('zip'):
                    absf = os.path.join(root, f)
                    zf = absf[len(data_location) + len(os.sep):]
                    try:
                        z.write(absf, zf)
                    except IOError:
                        # Doing this because File GDB lock files throw an exception.
                        pass
    return zfile
