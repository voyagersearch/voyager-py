"""Utility functions for voyager tasks."""
import os
import glob
import json
import shutil
import urllib
import zipfile


def create_unique_name(name, gdb):
    """Creates and returns a valid and unique name for the geodatabase."""
    import arcpy
    valid_name = arcpy.ValidateTableName(name, gdb)
    unique_name = arcpy.CreateUniqueName(valid_name, gdb)
    return unique_name


def clean_up(data_location):
    """Deletes intermediate data created during the task process."""
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
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item


def get_feature_data(item):
    """Return a valid layer file or dataset path.

    Describe will fail if the layer file does not exist or
    if the layer's datasource does not exist.
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
    """Return the clip geometry from a list
    of well-known text coordinates."""
    import arcpy
    coordinates = wkt[wkt.find('(') + 2: wkt.find(')')].split(',')
    array = arcpy.Array()
    for p in coordinates:
        pt = p.strip().split(' ')
        array.add(arcpy.Point(float(pt[0]), float(pt[1])))

    poly = arcpy.Polygon(array, sr)
    return poly


def get_projection_file(factory_code):
    """Returns the projection file using the factory code as a lookup."""
    import arcpy
    lu_file = os.path.join(os.getcwd(), 'voyager_tasks/supportfiles/projection_files.json')
    with open(lu_file) as fp:
        prj_lu = json.load(fp)
        arcgis_folder = arcpy.GetInstallInfo()['InstallDir']
        prj_file = os.path.join(arcgis_folder, prj_lu['{0}'.format(factory_code)])
    return prj_file


def report(report_file, task_name, num_processed, num_skipped):
    """Create markdown report of inputs processed or skipped.
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


def save_to_layer_file(data_location, arcgis_version='10', include_mxd_layers=True):
    """Saves all data from the data location to layer files."""
    import arcpy

    file_gdbs = glob.glob(os.path.join(data_location, '*.gdb'))
    for file_gdb in file_gdbs:
        arcpy.env.workspace = file_gdb
        for fc in arcpy.ListFeatureClasses():
            fl = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
            arcpy.management.SaveToLayerFile(fl,
                                             os.path.join(data_location, '{0}.lyr'.format(fc)),
                                             version=arcgis_version)
        for raster in arcpy.ListRasters():
            rl = arcpy.MakeRasterLayer_management(raster, '{0}_'.format(raster))
            arcpy.management.SaveToLayerFile(rl,
                                             os.path.join(data_location, '{0}.lyr'.format(raster)),
                                             version=arcgis_version)

    if include_mxd_layers:
        mxd_files = glob.glob(os.path.join(data_location, '*.mxd'))
        for mxd_file in mxd_files:
            mxd = arcpy.mapping.MapDocument(mxd_file)
            layers = arcpy.mapping.ListLayers(mxd)
            for layer in layers:
                if layer.description == '':
                    layer.description = layer.name
                arcpy.management.SaveToLayerFile(layer,
                                                 os.path.join(data_location, '{0}.lyr'.format(layer.name)),
                                                 version=arcgis_version)




def zip_data(data_location, name):
    """Creates a compressed zip file of the entire data location."""
    zfile = os.path.join(data_location, name)
    with zipfile.ZipFile(zfile, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(data_location):
            for f in files:
                if not f.endswith('zip'):
                    absf = os.path.join(root, f)
                    zf = absf[len(data_location) + len(os.sep):]
                    try:
                        #z.write(absf, os.path.join(os.path.basename(data_location), zf))
                        z.write(absf, zf)
                    except Exception:
                        pass
    return zfile
