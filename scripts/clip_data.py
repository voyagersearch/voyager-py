"""
Clips each input feature class or layer against the
clip area and creates a compressed zip file, map package, or layer package.
"""
from __future__ import unicode_literals
import os
from os.path import basename, dirname, join, splitext
import sys
import glob
import shutil
import zipfile
import traceback
import arcpy
import status


__author__ = 'VoyagerSearch'
__copyright__ = 'VoyagerSearch, 2014'
__date__ = '01/31/2014'


def clean_up(data_location):
    """Deletes intermediate data created during the process
    of creating map or layer packages."""
    for f in glob.glob(join(data_location, '*.lyr')):
        os.unlink(f)
    if os.path.exists(join(data_location, 'output.gdb')):
        arcpy.env.workspace = join(data_location, 'output.gdb')
    else:
        arcpy.env.workspace = data_location

    [arcpy.management.Delete(fc) for fc in arcpy.ListFeatureClasses()]
    shutil.rmtree(data_location, True)
# End clean_up function


def zip_data(data_location, name):
    """Create and return a compressed zip file containing
     the clipped datasets."""
    zfile = join(dirname(data_location), name)
    with zipfile.ZipFile(zfile, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(data_location):
            for f in files:
                if not f.endswith('zip'):
                    absf = join(root, f)
                    zf = absf[len(data_location) + len(os.sep):]
                    z.write(absf, join(basename(data_location), zf))

        ## Add any and all .mpk or .mxd files to the .zip.
        #for m in glob.glob(join(dirname(workspace), '*.m*')):
            #z.write(m, basename(m))
    return zfile
# End zip_data function


def create_unique_name(name, gdb):
    """Creates and returns a valid and unique name for the geodatabase."""
    valid_name = arcpy.ValidateTableName(name, gdb)
    unique_name = arcpy.CreateUniqueName(valid_name, gdb)
    return unique_name
# End create_unique_name function


def clip_layer_file(layer_file, aoi, output_name=''):
    """Clips each layer in the layer file to the output workspace
    and re-sources each layer and saves a copy of the layer file."""
    if arcpy.env.workspace.endswith('.gdb'):
        layer_path = os.path.join(dirname(arcpy.env.workspace), basename(layer_file))
    else:
        layer_path = os.path.join(arcpy.env.workspace, basename(layer_file))
    shutil.copyfile(layer_file, layer_path)
    layer_from_file = arcpy.mapping.Layer(layer_path)
    layers = arcpy.mapping.ListLayers(layer_from_file)
    for layer in layers:
        if layer.isFeatureLayer:
            if output_name == '':
                name = create_unique_name(layer.name, arcpy.env.workspace)
            else:
                name = create_unique_name(output_name, arcpy.env.workspace)
            arcpy.analysis.Clip(layer.dataSource, aoi, name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace, 'FILEGDB_WORKSPACE', splitext(basename(name))[0], False)
            else:
                layer.replaceDataSource(arcpy.env.workspace, 'SHAPEFILE_WORKSPACE', basename(name), False)
        elif layer.isRasterLayer:
            if isinstance(aoi, arcpy.Polygon):
                extent = aoi.extent
            else:
                extent = arcpy.Describe(aoi).extent
            ext = '{0} {1} {2} {3}'.format(extent.XMin, extent.YMin, extent.XMax, extent.YMax)
            if output_name == '':
                name = create_unique_name(layer.name, arcpy.env.workspace)
            else:
                name = create_unique_name(output_name, arcpy.env.workspace)
            arcpy.management.Clip(layer.dataSource, ext, name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace, 'FILEGDB_WORKSPACE', splitext(basename(name))[0], False)
            else:
                layer.replaceDataSource(arcpy.env.workspace, 'RASTER_WORKSPACE', splitext(basename(name))[0], False)

        if layer.description == '':
            layer.description == layer.name
        layer.save()
        del layer, layers
# End clip_layer_file function


def clip_mxd_layers(mxd_path, aoi):
    """Clips each layer in the map document to output workspace
    and re-sources each layer and saves a copy of the mxd."""
    mxd = arcpy.mapping.MapDocument(mxd_path)
    layers = arcpy.mapping.ListLayers(mxd)
    for layer in layers:
        if layer.isFeatureLayer:
            arcpy.analysis.Clip(layer.dataSource, aoi, layer.name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace, 'FILEGDB_WORKSPACE', splitext(layer.datasetName)[0], False)
            else:
                layer.replaceDataSource(arcpy.env.workspace, 'SHAPEFILE_WORKSPACE', layer.datasetName, False)
        elif layer.isRasterLayer:
            ext = '{0} {1} {2} {3}'.format(aoi.extent.XMin, aoi.extent.YMin, aoi.extent.XMax, aoi.extent.YMax)
            arcpy.management.Clip(layer.dataSource, ext, layer.name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace, 'FILEGDB_WORKSPACE', splitext(layer.datasetName)[0], False)
            else:
                layer.replaceDataSource(arcpy.env.workspace, 'RASTER_WORKSPACE', splitext(layer.datasetName)[0], False)

    # Save a new copy of the mxd with all layers clipped and re-sourced.
    if mxd.description == '':
        mxd.description = basename(mxd.filePath)
    if arcpy.env.workspace.endswith('.gdb'):
        new_mxd = join(dirname(arcpy.env.worksapce), basename(mxd.filePath))
    else:
        new_mxd = join(arcpy.env.workspace, basename(mxd.filePath))
    mxd.saveACopy(new_mxd)
    del layer, layers, mxd
# End clip_mxd_layers function


def from_wkt(wkt, sr):
    """Return the clip geometry from a list
    of well-known text coordinates."""
    coords = wkt[wkt.find('(') + 2: wkt.find(')')].split(',')
    array = arcpy.Array()
    for p in coords:
        pt = p.strip().split(' ')
        array.add(arcpy.Point(float(pt[0]), float(pt[1])))

    poly = arcpy.Polygon(array, sr)
    return poly
# End from_wkt function


def create_layer_package(data_location, additional_files):
    """Creates a layer package (.lpk) for all the clipped datasets."""
    gdbs = glob.glob(join(data_location, '*.gdb'))
    for gdb in gdbs:
        arcpy.env.workspace = gdb
        for fc in arcpy.ListFeatureClasses():
            fl = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
            arcpy.management.SaveToLayerFile(fl, join(data_location, '{0}.lyr'.format(fc)), version='10')
    lyrs = glob.glob(join(data_location, '*.lyr'))
    arcpy.management.PackageLayer(lyrs,
                                  join(dirname(data_location), 'output.lpk'),
                                  'PRESERVE',
                                  version='10',
                                  additional_files=additional_files)
# End create_layer_package function


def create_map_package(data_location, additional_files):
    """Creates a map package (.mpk) for all the clipped datasets."""
    mxd = arcpy.mapping.MapDocument(join(dirname(__file__), 'MapTemplate.mxd'))
    if mxd.description == '':
        mxd.description = basename(mxd.filePath)

    gdbs = glob.glob(join(data_location, '*.gdb'))
    for gdb in gdbs:
        arcpy.env.workspace = gdb
        for fc in arcpy.ListFeatureClasses():
            layer = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
            arcpy.mapping.AddLayer(mxd.activeDataFrame, layer.getOutput(0))
    lyrs = glob.glob(join(data_location, '*.lyr'))
    for lyr in lyrs:
        arcpy.mapping.AddLayer(mxd.activeDataFrame, arcpy.mapping.Layer(lyr))

    new_mxd = join(dirname(data_location), 'output.mxd')
    mxd.saveACopy(new_mxd)
    arcpy.management.PackageMap(new_mxd,
                                new_mxd.replace('.mxd', '.mpk'),
                                'PRESERVE',
                                version='10',
                                additional_files=additional_files)
    del mxd
    os.unlink(new_mxd)
# End create_map_package function


def clip_data(datasets,
              out_workspace,
              clip_area,
              out_coordinate_system=None,
              out_format='FileGDB',
              zip_up=True):
    """Clips data to a new or existing geodatabase.
    Optionally, the geodatbase can be zipped into
    a distributable file."""
    try:
        # Voyager Job Runner: passes a dictionary of inputs and output names.
        datasets = eval(datasets)
    except SyntaxError:
        # If not output names are passed in.
        datasets = dict((k, '') for k in datasets.split(';'))

    if out_coordinate_system is not None:
        out_sr = arcpy.SpatialReference(out_coordinate_system)
        arcpy.env.outputCoordinateSystem = out_sr

    if clip_area.startswith('POLYGON'):
        gcs_sr = arcpy.SpatialReference(4326)
        gcs_clip_poly = from_wkt(clip_area, gcs_sr)
        if not gcs_clip_poly.area > 0:
            gcs_clip_poly = from_wkt('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))', gcs_sr)
    else:
        clip_poly = clip_area

    # Create a valid output workspace if it does not exist.
    working_folder = join(out_workspace, 'Work')
    os.mkdir(working_folder)
    if not out_format == 'SHP':
        out_workspace = arcpy.management.CreateFileGDB(working_folder, 'output.gdb').getOutput(0)
    else:
        out_workspace = working_folder
    arcpy.env.workspace = out_workspace

    i = 1.
    count = len(datasets)
    files_to_package = list()
    status_writer= status.Writer()
    status_writer.send_status('Starting the clipping process...')
    for ds, out_name in datasets.iteritems():
        try:
            dsc = arcpy.Describe(ds)

            # If no output coord. system, get output spatial reference from input.
            if out_coordinate_system is None:
                try:
                    out_sr = dsc.spatialReference
                    arcpy.env.outputCoordinateSystem = out_sr
                except AttributeError:
                    out_sr = arcpy.SpatialReference(4326)
                    arcpy.env.outputCoordinateSystem = out_sr

            # If a file, no need to project the clip area.
            if not dsc.dataType in ('File', 'TextFile'):
                if clip_area.startswith('POLYGON'):
                    if not out_sr.name == gcs_sr.name:
                        try:
                            geo_transformation = arcpy.ListTransformations(gcs_sr, out_sr)[0]
                            clip_poly = gcs_clip_poly.projectAs(out_sr, geo_transformation)
                        except AttributeError:
                            clip_poly = gcs_clip_poly.projectAs(out_sr)
                    else:
                        clip_poly = gcs_clip_poly
                        extent = clip_poly.extent
                else:
                    if not arcpy.Describe(clip_poly).spatialReference == out_sr:
                        clip_poly = arcpy.management.Project(clip_poly, 'clip_features', out_sr)
                    extent = arcpy.Describe(clip_poly).extent

            # Geodatabase feature class
            if dsc.dataType == 'FeatureClass':
                if out_name == '':
                    name = create_unique_name(dsc.name, out_workspace)
                else:
                    name = create_unique_name(out_name, out_workspace)
                arcpy.analysis.Clip(ds, clip_poly, name)

            # Geodatabase feature dataset
            elif dsc.dataType == 'FeatureDataset':
                fds = arcpy.management.CreateFeatureDataset(out_workspace, dsc.name)
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.analysis.Clip(fc, clip_poly, create_unique_name(fc, fds))
                arcpy.env.workspace = out_workspace

            # Shapefile
            elif dsc.dataType == 'ShapeFile':
                if out_name == '':
                    name = create_unique_name(dsc.name, out_workspace)
                else:
                    name = create_unique_name(out_name, out_workspace)
                arcpy.analysis.Clip(ds, clip_poly, name)

            # Raster dataset
            elif dsc.dataType == 'RasterDataset':
                if out_name == '':
                    name = create_unique_name(dsc.name, out_workspace)
                else:
                    name = create_unique_name(out_name, out_workspace)
                ext = '{0} {1} {2} {3}'.format(extent.XMin, extent.YMin, extent.XMax, extent.YMax)
                arcpy.management.Clip(ds, ext, name)

            # Layer file
            elif dsc.dataType == 'Layer':
                clip_layer_file(dsc.catalogPath, clip_poly, out_name)

            # Cad drawing dataset
            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    name = create_unique_name('{0}_{1}'.format(cad_wks_name, cad_fc), out_workspace)
                    arcpy.analysis.Clip(cad_fc, clip_poly, name)
                arcpy.env.workspace = out_workspace

            # File
            elif dsc.dataType in ('File', 'TextFile'):
                if dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                    name = splitext(dsc.name)[0]
                    kml_layer = arcpy.conversion.KMLToLayer(dsc.catalogPath, arcpy.env.scratchFolder, name)
                    group_layer = arcpy.mapping.Layer(join(arcpy.env.scratchFolder, '{0}.lyr'.format(name)))
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.analysis.Clip(layer, gcs_clip_poly, create_unique_name(layer, out_workspace))
                    # Clean up temp KML results.
                    arcpy.management.Delete(join(arcpy.env.scratchFolder, '{0}.lyr'.format(name)))
                    arcpy.management.Delete(kml_layer[1])
                    del group_layer
                else:
                    if out_name == '':
                        out_name = dsc.name
                    if out_workspace.endswith('.gdb'):
                        f = arcpy.management.Copy(ds, join(dirname(out_workspace), out_name))
                    else:
                        f = arcpy.management.Copy(ds, join(out_workspace, out_name))
                    status_writer.send_percent(i/count, 'copied file {0}.'.format(ds), 'clip_data')
                    if out_format in ('LPK', 'MPK'):
                        files_to_package.append(f.getOutput(0))
                    status_writer.send_percent(i/count, '{0} cannot be packaged.'.format(ds), 'clip_data')
                    i += 1.
                    continue

            # Map document
            elif dsc.dataType == 'MapDocument':
                clip_mxd_layers(dsc.catalogPath, clip_poly, out_format)

            status_writer.send_percent(i/count, 'clipped {0}.'.format(ds), 'clip_data')
            i += 1.

        # Continue if an error. Process as many as possible.
        except Exception:
            tbinfo = traceback.format_tb(sys.exc_info()[2])[0]
            status_writer.send_percent(i/count, 'Traceback info: {0}.\n Error info: {1}.\n'.format(tbinfo, str(sys.exc_info()[1])), 'clip_data')
            i += 1.
            pass

    if arcpy.env.workspace.endswith('.gdb'):
        out_workspace = dirname(arcpy.env.workspace)

    if out_format == 'MPK':
        status_writer.send_status('Creating the map package...')
        create_map_package(out_workspace, files_to_package)
        clean_up(out_workspace)
    elif out_format == 'LPK':
        status_writer.send_status('Creating the layer package...')
        create_layer_package(out_workspace, files_to_package)
        clean_up(out_workspace)
    else:
        if zip_up:
            status_writer.send_status('Creating the output zip file: {0}...'.format(join(out_workspace, 'output.zip')))
            zip_data(out_workspace, 'output.zip')
            clean_up(out_workspace)
    status_writer.send_status('Completed.')
# End clip_data function