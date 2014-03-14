"""
Clips each input feature class or layer against the
clip area and creates a compressed zip file, map package, or layer package.
"""
from __future__ import unicode_literals
import sys
import os
import glob
import shutil
import traceback
import arcpy
from voyager_tasks.utils import task_utils
from voyager_tasks.utils import status


def clip_layer_file(layer_file, aoi):
    """Clips each layer in the layer file to the output workspace
    and re-sources each layer and saves a copy of the layer file."""
    if arcpy.env.workspace.endswith('.gdb'):
        layer_path = os.path.join(os.path.dirname(arcpy.env.workspace), os.path.basename(layer_file))
    else:
        layer_path = os.path.join(arcpy.env.workspace, os.path.basename(layer_file))
    shutil.copyfile(layer_file, layer_path)
    layer_from_file = arcpy.mapping.Layer(layer_path)
    layers = arcpy.mapping.ListLayers(layer_from_file)
    for layer in layers:
        if layer.isFeatureLayer:
            name = task_utils.create_unique_name(layer.name, arcpy.env.workspace)
            arcpy.analysis.Clip(layer.dataSource, aoi, name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace,
                                        'FILEGDB_WORKSPACE',
                                        os.path.splitext(os.path.basename(name))[0],
                                        False)
            else:
                layer.replaceDataSource(arcpy.env.workspace, 'SHAPEFILE_WORKSPACE', os.path.basename(name), False)
        elif layer.isRasterLayer:
            if isinstance(aoi, arcpy.Polygon):
                extent = aoi.extent
            else:
                extent = arcpy.Describe(aoi).extent
            ext = '{0} {1} {2} {3}'.format(extent.XMin, extent.YMin, extent.XMax, extent.YMax)
            name = task_utils.create_unique_name(layer.name, arcpy.env.workspace)
            arcpy.management.Clip(layer.dataSource, ext, name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace,
                                        'FILEGDB_WORKSPACE',
                                        os.path.splitext(os.path.basename(name))[0],
                                        False)
            else:
                layer.replaceDataSource(arcpy.env.workspace,
                                        'RASTER_WORKSPACE',
                                        os.path.splitext(os.path.basename(name))[0],
                                        False)

        if layer.description == '':
            layer.description == layer.name

        # Catch assertion error if a group layer.
        try:
            layer.save()
        except AssertionError:
            layers[0].save()
            pass
# End clip_layer_file function


def clip_mxd_layers(mxd_path, aoi):
    """Clips each layer in the map document to output workspace
    and re-sources each layer and saves a copy of the mxd.
    """
    mxd = arcpy.mapping.MapDocument(mxd_path)
    layers = arcpy.mapping.ListLayers(mxd)
    for layer in layers:
        if layer.isFeatureLayer:
            arcpy.analysis.Clip(layer.dataSource, aoi, layer.name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace,
                                        'FILEGDB_WORKSPACE',
                                        os.path.splitext(layer.datasetName)[0],
                                        False)
            else:
                layer.replaceDataSource(arcpy.env.workspace, 'SHAPEFILE_WORKSPACE', layer.datasetName, False)
        elif layer.isRasterLayer:
            ext = '{0} {1} {2} {3}'.format(aoi.extent.XMin, aoi.extent.YMin, aoi.extent.XMax, aoi.extent.YMax)
            arcpy.management.Clip(layer.dataSource, ext, layer.name)
            if arcpy.env.workspace.endswith('.gdb'):
                layer.replaceDataSource(arcpy.env.workspace,
                                        'FILEGDB_WORKSPACE',
                                        os.path.splitext(layer.datasetName)[0],
                                        False)
            else:
                layer.replaceDataSource(arcpy.env.workspace,
                                        'RASTER_WORKSPACE',
                                        os.path.splitext(layer.datasetName)[0],
                                        False)

    # Save a new copy of the mxd with all layers clipped and re-sourced.
    if mxd.description == '':
        mxd.description = os.path.basename(mxd.filePath)
    if arcpy.env.workspace.endswith('.gdb'):
        new_mxd = os.path.join(os.path.dirname(arcpy.env.worksapce), os.path.basename(mxd.filePath))
    else:
        new_mxd = os.path.join(arcpy.env.workspace, os.path.basename(mxd.filePath))
    mxd.saveACopy(new_mxd)
    del mxd
# End clip_mxd_layers function


def create_lpk(data_location, additional_files):
    """Creates a layer package (.lpk) for all the clipped datasets."""
    # Save all feature classes to layer files.
    file_gdbs = glob.glob(os.path.join(data_location, '*.gdb'))
    for file_gdb in file_gdbs:
        arcpy.env.workspace = file_gdb
        for fc in arcpy.ListFeatureClasses():
            fl = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
            arcpy.management.SaveToLayerFile(fl, os.path.join(data_location, '{0}.lyr'.format(fc)), version='10')

    # Save all map document layers to layer file.
    mxd_files = glob.glob(os.path.join(data_location, '*.mxd'))
    for mxd_file in mxd_files:
        mxd = arcpy.mapping.MapDocument(mxd_file)
        layers = arcpy.mapping.ListLayers(mxd)
        for layer in layers:
            if layer.description == '':
                layer.description = layer.name
            arcpy.management.SaveToLayerFile(layer,
                                             os.path.join(data_location, '{0}.lyr'.format(layer.name)),
                                             version='10')

    # Package all layer files.
    layer_files = glob.glob(os.path.join(data_location, '*.lyr'))
    arcpy.management.PackageLayer(layer_files,
                                  os.path.join(os.path.dirname(data_location), 'output.lpk'),
                                  'PRESERVE',
                                  version='10',
                                  additional_files=additional_files)
# End create_layer_package function


def create_mxd_or_mpk(data_location, additional_files=None, mpk=False):
    """Creates a map document (.mxd) or map package (.mpk) for all the clipped datasets."""
    mxd_template = arcpy.mapping.MapDocument(os.path.join(os.path.dirname(__file__), r'supportfiles\MapTemplate.mxd'))
    if mxd_template.description == '':
        mxd_template.description = os.path.basename(mxd_template.filePath)

    # Add all feature classes to the mxd template.
    file_gdbs = glob.glob(os.path.join(data_location, '*.gdb'))
    for file_gdb in file_gdbs:
        arcpy.env.workspace = file_gdb
        for fc in arcpy.ListFeatureClasses():
            layer = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
            arcpy.mapping.AddLayer(mxd_template.activeDataFrame, layer.getOutput(0))

    # Add all layer files to the mxd template.
    layer_files = glob.glob(os.path.join(data_location, '*.lyr'))
    for layer_file in layer_files:
        arcpy.mapping.AddLayer(mxd_template.activeDataFrame, arcpy.mapping.Layer(layer_file))

    # Add all layers from all map documents to the map template.
    mxd_files = glob.glob(os.path.join(data_location, '*.mxd'))
    for mxd_file in mxd_files:
        mxd = arcpy.mapping.MapDocument(mxd_file)
        layers = arcpy.mapping.ListLayers(mxd)
        for layer in layers:
            arcpy.mapping.AddLayer(mxd_template.activeDataFrame, layer)

    # Package the map template.
    new_mxd = os.path.join(data_location, 'output.mxd')
    mxd_template.saveACopy(new_mxd)
    if mpk:
        arcpy.management.PackageMap(new_mxd,
                                    new_mxd.replace('.mxd', '.mpk'),
                                    'PRESERVE',
                                    version='10',
                                    additional_files=additional_files)
        del mxd_template
        os.unlink(new_mxd)
# End create_map_package function


def execute(request):
    """Clips data to a new or existing geodatabase.
    :param request: json as a dict.
    """
    # Parse parameters.
    parameters = request['params']
    in_data = task_utils.find(lambda p: p['name'] == 'input_items', parameters)
    docs = in_data.get('response').get('docs')
    input_items = str(dict((task_utils.get_feature_data(v), v['name']) for v in docs))

    # Retrieve clip geometry.
    try:
        clip_area = task_utils.find(lambda p: p['name'] == 'clip_geometry', parameters)['wkt']
    except KeyError:
        clip_area = task_utils.find(lambda p: p['name'] == 'clip_geometry', parameters)['feature']

    # Retrieve the coordinate system code.
    out_coordinate_system = int(task_utils.find(lambda p: p['name'] == 'output_projection', parameters)['code'])

    # Retrieve the output format type.
    out_format = task_utils.find(lambda p: p['name'] == 'output_format', parameters)['value']
    out_workspace = request['folder']
    if not os.path.exists(out_workspace):
        os.makedirs(out_workspace)

    try:
        # Voyager Job Runner: passes a dictionary of inputs and output names.
        input_items = eval(input_items)
    except SyntaxError:
        # If not output names are passed in.
        input_items = dict((k, '') for k in input_items.split(';'))

    if out_coordinate_system is not None:
        try:
            out_sr = arcpy.SpatialReference(out_coordinate_system)
        except RuntimeError:
            out_sr = arcpy.SpatialReference(task_utils.get_projection_file(out_coordinate_system))
        arcpy.env.outputCoordinateSystem = out_sr

    if clip_area.startswith('POLYGON'):
        gcs_sr = arcpy.SpatialReference(4326)
        gcs_clip_poly = task_utils.from_wkt(clip_area, gcs_sr)
        if not gcs_clip_poly.area > 0:
            gcs_clip_poly = task_utils.from_wkt('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))', gcs_sr)
    else:
        clip_poly = clip_area

    if not out_format == 'SHP':
        out_workspace = arcpy.management.CreateFileGDB(out_workspace, 'output.gdb').getOutput(0)
    arcpy.env.workspace = out_workspace

    i = 1.
    count = len(input_items)
    files_to_package = list()
    status_writer = status.Writer()
    status_writer.send_status('Starting the clipping process...')
    for ds, out_name in input_items.iteritems():
        try:
            dsc = arcpy.Describe(ds)

            # If no output coord. system, get output spatial reference from input.
            if out_coordinate_system is None:
                try:
                    out_sr = dsc.spatialReference
                    arcpy.env.outputCoordinateSystem = out_sr
                except AttributeError:
                    try:
                        out_sr = arcpy.SpatialReference(4326)
                    except RuntimeError:
                        out_sr = arcpy.SpatialReference(task_utils.get_projection_file(4326))
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
                    name = task_utils.create_unique_name(dsc.name, out_workspace)
                else:
                    name = task_utils.create_unique_name(out_name, out_workspace)
                arcpy.analysis.Clip(ds, clip_poly, name)

            # Geodatabase feature dataset
            elif dsc.dataType == 'FeatureDataset':
                fds = arcpy.management.CreateFeatureDataset(out_workspace, dsc.name)
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.analysis.Clip(fc, clip_poly, task_utils.create_unique_name(fc, fds))
                arcpy.env.workspace = out_workspace

            # Shapefile
            elif dsc.dataType == 'ShapeFile':
                if out_name == '':
                    name = task_utils.create_unique_name(dsc.name, out_workspace)
                else:
                    name = task_utils.create_unique_name(out_name, out_workspace)
                arcpy.analysis.Clip(ds, clip_poly, name)

            # Raster dataset
            elif dsc.dataType == 'RasterDataset':
                if out_name == '':
                    name = task_utils.create_unique_name(dsc.name, out_workspace)
                else:
                    name = task_utils.create_unique_name(out_name, out_workspace)
                ext = '{0} {1} {2} {3}'.format(extent.XMin, extent.YMin, extent.XMax, extent.YMax)
                arcpy.management.Clip(ds, ext, name)

            # Layer file
            elif dsc.dataType == 'Layer':
                clip_layer_file(dsc.catalogPath, clip_poly)

            # Cad drawing dataset
            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    name = task_utils.create_unique_name('{0}_{1}'.format(cad_wks_name, cad_fc), out_workspace)
                    arcpy.analysis.Clip(cad_fc, clip_poly, name)
                arcpy.env.workspace = out_workspace

            # File
            elif dsc.dataType in ('File', 'TextFile'):
                if dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                    name = os.path.splitext(dsc.name)[0]
                    kml_layer = arcpy.conversion.KMLToLayer(dsc.catalogPath, arcpy.env.scratchFolder, name)
                    group_layer = arcpy.mapping.Layer(os.path.join(arcpy.env.scratchFolder, '{0}.lyr'.format(name)))
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.analysis.Clip(layer, gcs_clip_poly, task_utils.create_unique_name(layer, out_workspace))
                    # Clean up temp KML results.
                    arcpy.management.Delete(os.path.join(arcpy.env.scratchFolder, '{0}.lyr'.format(name)))
                    arcpy.management.Delete(kml_layer[1])
                    del group_layer
                else:
                    if out_name == '':
                        out_name = dsc.name
                    if out_workspace.endswith('.gdb'):
                        f = arcpy.management.Copy(ds, os.path.join(os.path.dirname(out_workspace), out_name))
                    else:
                        f = arcpy.management.Copy(ds, os.path.join(out_workspace, out_name))
                    status_writer.send_percent(i/count, 'copied file {0}.'.format(ds), 'clip_data')
                    if out_format in ('LPK', 'MPK'):
                        files_to_package.append(f.getOutput(0))
                    i += 1.
                    continue

            # Map document
            elif dsc.dataType == 'MapDocument':
                clip_mxd_layers(dsc.catalogPath, clip_poly)

            status_writer.send_percent(i/count, 'clipped {0}.'.format(ds), 'clip_data')
            i += 1.

        # Continue if an error. Process as many as possible.
        except Exception:
            tbinfo = traceback.format_tb(sys.exc_info()[2])[0]
            status_writer.send_percent(
                i/count,
                'Traceback info: {0}.\n Error info: {1}.\n'.format(tbinfo, str(sys.exc_info()[1])),
                'clip_data'
            )
            i += 1.
            pass

    if arcpy.env.workspace.endswith('.gdb'):
        out_workspace = os.path.dirname(arcpy.env.workspace)

    if out_format == 'MPK':
        status_writer.send_status('Creating the map package...')
        create_mxd_or_mpk(out_workspace, files_to_package, True)
        task_utils.clean_up(out_workspace)
    elif out_format == 'LPK':
        status_writer.send_status('Creating the layer package...')
        create_lpk(out_workspace, files_to_package)
        task_utils.clean_up(out_workspace)
    else:
        create_mxd_or_mpk(out_workspace)
        status_writer.send_status('Creating the output zip file: {0}...'.format(os.path.join(out_workspace, 'output.zip')))
        zip_file = task_utils.zip_data(out_workspace, 'output.zip')
        task_utils.clean_up(os.path.dirname(zip_file))
    status_writer.send_status('Completed.')
# End clip_data function