"""Utility functions for voyager tasks."""
import os
import sys
import glob
import json
import urllib
import zipfile
import status


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


def create_unique_name(name, gdb):
    """Creates and returns a valid and unique name for the geodatabase.

    :param name: name to be validated
    :param gdb: workspace path
    :rtype : str
    """
    import arcpy
    valid_name = arcpy.ValidateTableName(name, gdb)
    unique_name = arcpy.CreateUniqueName(valid_name, gdb)
    return unique_name


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
    return clip_area.extent


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


def get_input_items(parameters):
    """Get the input search result items and output names.

    :param: parameters: parameter list
    :rtype: dict
    """
    results = {}
    for item in parameters:
        if item['name'] == 'input_items':
            docs = item['response']['docs']
            try:
                for i in docs:
                    try:
                        results[get_data_path(i)] = i['name']
                    except KeyError:
                        results[get_data_path(i)] = ''
                    except IOError:
                        continue
            except IOError:
                pass
            if not results:
                status_writer = status.Writer()
                status_writer.send_state(status.STAT_FAILED, 'All results are invalid or do not exist.')
                sys.exit(1)
            break
    return results


def get_data_path(item):
    """Return the layer file or dataset path.

    :param item: dataset path
    :rtype : str
    """
    try:
        if os.path.exists(item['path']):
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


def get_projection_file(factory_code):
    """Returns a projection file using the factory code as a lookup.
    This function adds support for ArcGIS 10.0.

    :param factory_code: The projection's factory code - i.e. 4326 is the code for WGS84
    :rtype : str
    """
    import arcpy
    lu_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'projection_files.json')
    with open(lu_file) as fp:
        prj_lu = json.load(fp)
        arcgis_folder = arcpy.GetInstallInfo()['InstallDir']
        prj_file = os.path.join(arcgis_folder, prj_lu['{0}'.format(factory_code)])
    return prj_file


def report(report_file, num_processed, num_skipped, num_errors=0, num_warnings=0):
    """Create a markdown report of inputs processed or skipped.

    :param report_file: path of the .md file
    :param num_processed: number of items processed
    :param num_skipped: number of items skipped
    :param num_errors:  number of errors
    :param num_warnings: number of warnings
    """
    report_md = ['| Action    | Count |',
                 '| ------    | ----- |',
                 '| Processed | {0} |'.format(num_processed),
                 '| Skipped   | {0} |'.format(num_skipped)]

    report_info = {"count": num_skipped, "warnings": num_warnings, "errors": num_errors, "text": '\n'.join(report_md)}
    with open(report_file, 'w') as fp:
        json.dump(report_info, fp, indent=2)


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
