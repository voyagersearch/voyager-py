"""Utility functions for voyager tasks."""
import os
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
                        z.write(absf, os.path.join(os.path.basename(data_location), zf))
                    except Exception:
                        pass
    return zfile
