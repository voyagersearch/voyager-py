"""Utility functions for voyager tasks."""
import os
import glob
import json
import shutil
import urllib
import zipfile


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


def clean_up(data_location):
    """Deletes intermediate data created during the task process.

    :param data_location: folder path
    """
    for root, dirs, files in os.walk(data_location):
        for name in files:
            if not name.endswith('.zip'):
                try:
                    os.remove(os.path.join(root, name))
                except WindowsError:
                    pass
        for name in dirs:
            shutil.rmtree(os.path.join(root, name), True)


def find(f, seq):
    """Return first item in sequence where f(item) == True.
    :param f: lambda function
    :param seq: list of items
    :rtype : str
    """
    for item in seq:
        if f(item):
            return item


def get_feature_data(item):
    """Return a valid layer file or dataset path.
    Describe will fail if the layer file does not exist or
    if the layer's datasource does not exist.

    :param item: dataset path
    :rtype : str
    """
    import arcpy
    try:
        dsc = arcpy.Describe(item['[lyrFile]'])
        return item['[lyrFile]']
    except Exception:
        pass

    try:
        layer_file = urllib.urlretrieve(item['[lyrURL]'])[0]
        return layer_file
    except Exception:
        return item['path']


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


def get_projection_file(factory_code):
    """Returns a projection file using the factory code as a lookup.
    This function adds support for ArcGIS 10.0.

    :param factory_code: The projection's factory code - i.e. 4326 is the code for WGS84
    :rtype : str
    """
    import arcpy
    lu_file = os.path.join(os.getcwd(), 'voyager_tasks/supportfiles/projection_files.json')
    with open(lu_file) as fp:
        prj_lu = json.load(fp)
        arcgis_folder = arcpy.GetInstallInfo()['InstallDir']
        prj_file = os.path.join(arcgis_folder, prj_lu['{0}'.format(factory_code)])
    return prj_file


def make_thumbnail(layer_or_mxd, output_folder):
    """Creates a thumbnail PNG file for the layer file or map document.

    :param dataset: a layer file or document.
    :param output_folder: the output folder where PNG files are saved
    """
    import arcpy
    arcpy.env.overwriteOutput = True
    if layer_or_mxd.endswith('.mxd'):
        mxd = arcpy.mapping.MapDocument(layer_or_mxd)
        arcpy.mapping.ExportToPNG(mxd, os.path.join(output_folder, '_thumb.png'), '', 150, 150)
    else:
        support_files_dir = os.path.abspath(os.path.join(os.getcwd(), 'voyager_tasks', 'supportfiles'))
        map_template = os.path.join(support_files_dir, 'MapTemplate.mxd')
        mxd = arcpy.mapping.MapDocument(map_template)
        data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
        layer = arcpy.mapping.Layer(layer_or_mxd)
        arcpy.mapping.AddLayer(data_frame, layer)
        arcpy.mapping.ExportToPNG(mxd, os.path.join(output_folder, '_thumb.png'), data_frame, 150, 150)


def report(report_file, task_name, num_processed, num_skipped):
    """Create a markdown report of inputs processed or skipped.

    :param report_file: path of the .md file
    :param task_name:  name of the task
    :param num_processed: number of items processed
    :param num_skipped: number of items skipped
    """
    with open(report_file, 'w') as r:
        r.write('### {0}\n'.format(task_name))
        r.write('| Action    | Count |\n')
        r.write('| ------    | ----- |\n')
        r.write('| Processed | {0} |\n'.format(num_processed))
        r.write('| Skipped   | {0} |\n'.format(num_skipped))


def save_to_layer_file(data_location, include_mxd_layers=True):
    """Saves all data from the data location to layer files.

    :param data_location: folder containing data to be saved as layer files
    :param include_mxd_layers: save layers in mxd's to layer files - default is True
    """
    import arcpy

    file_gdbs = glob.glob(os.path.join(data_location, '*.gdb'))
    for file_gdb in file_gdbs:
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
                    except Exception:
                        pass
    return zfile
