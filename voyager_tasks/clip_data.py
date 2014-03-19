"""
Clips each input feature class or layer against the
clip area and creates a compressed zip file, map package, or layer package.
"""
import sys
import os
import glob
import shutil
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
    """Creates a layer package (.lpk) for all datasets in the data location."""

    # Ensure existing layer files have a description.
    lyr_files = glob.glob(os.path.join(data_location, '*.lyr'))
    for lyr in lyr_files:
        layer = arcpy.mapping.Layer(lyr)
        if layer.description == '':
            layer.description = layer.name
            layer.save()

    # Save data to layer files.
    task_utils.save_to_layer_file(data_location, '10', True)

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
    shutil.copyfile(
        os.path.join(os.path.dirname(__file__), r'supportfiles\MapTemplate.mxd'),
        os.path.join(data_location, 'output.mxd')
    )
    mxd = arcpy.mapping.MapDocument(os.path.join(data_location, 'output.mxd'))
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
            layer = arcpy.management.MakeFeatureLayer(ds, '{0}_'.format(os.path.basename(ds)[:-3]))
            arcpy.mapping.AddLayer(df, layer.getOutput(0))
        elif ds.endswith('.gdb'):
            # Add all feature classes to the mxd template.
            arcpy.env.workspace = ds
            for fc in arcpy.ListFeatureClasses():
                layer = arcpy.management.MakeFeatureLayer(fc, '{0}_'.format(fc))
                arcpy.mapping.AddLayer(df, layer.getOutput(0))
            for raster in arcpy.ListRasters():
                layer = arcpy.MakeRasterLayer_management(raster, '{0}_'.format(raster))
                arcpy.mapping.AddLayer(df, layer.getOutput(0))
        elif ds.endswith('.lyr'):
            # Add all layer files to the mxd template.
            arcpy.mapping.AddLayer(df, arcpy.mapping.Layer(ds))
        elif ds.endswith('.mxd') and not ds == mxd.filePath:
            # Add all layers from all map documents to the map template.
            temp_mxd = arcpy.mapping.MapDocument(ds)
            layers = arcpy.mapping.ListLayers(temp_mxd)
            for layer in layers:
                arcpy.mapping.AddLayer(df, layer)
            del temp_mxd

    mxd.save()
    if mpk:
        # Package the map template.
        arcpy.management.PackageMap(mxd,
                                    mxd.filePath.replace('.mxd', '.mpk'),
                                    'PRESERVE',
                                    version='10',
                                    additional_files=additional_files)
        del mxd
# End create_map_package function


def execute(request):
    """Clips data to a new or existing geodatabase.
    :param request: json as a dict.
    """
    clipped = 0
    skipped = 0

    status_writer = status.Writer()
    # Parse parameters.
    parameters = request['params']
    in_data = task_utils.find(lambda p: p['name'] == 'input_items', parameters)
    docs = in_data.get('response').get('docs')
    input_items = str(dict((task_utils.get_feature_data(v), v['name']) for v in docs))

    # Retrieve clip geometry.
    try:
        clip_area = task_utils.find(lambda p: p['name'] == 'clip_geometry', parameters)['wkt']
    except KeyError:
        try:
            clip_area = task_utils.find(lambda p: p['name'] == 'clip_geometry', parameters)['feature']
        except KeyError:
            clip_area = 'POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))'
            #status_writer.send_status('Cannot clip data. No clip extent.')
            #sys.exit(1)

    # Retrieve the coordinate system code.
    out_coordinate_system = int(task_utils.find(lambda p: p['name'] == 'output_projection', parameters)['code'])

    # Retrieve the output format type.
    out_format = task_utils.find(lambda p: p['name'] == 'output_format', parameters)['value']
    out_workspace = os.path.join(request['folder'], 'temp')
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
        try:
            gcs_sr = arcpy.SpatialReference(4326)
        except RuntimeError:
            gcs_sr = arcpy.SpatialReference(task_utils.get_projection_file(4326))
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

            # Feature class
            if dsc.dataType == 'FeatureClass':
                if out_name == '':
                    name = task_utils.create_unique_name(dsc.name, out_workspace)
                else:
                    name = task_utils.create_unique_name(out_name, out_workspace)
                arcpy.analysis.Clip(ds, clip_poly, name)

            # Feature dataset
            elif dsc.dataType == 'FeatureDataset':
                fds_name = os.path.basename(task_utils.create_unique_name(dsc.name, out_workspace))
                fds = arcpy.management.CreateFeatureDataset(out_workspace, fds_name)
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    try:
                        arcpy.analysis.Clip(fc, clip_poly, task_utils.create_unique_name(fc, fds))
                    except arcpy.ExecuteError:
                        pass
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
                            arcpy.analysis.Clip(layer,
                                                gcs_clip_poly,
                                                task_utils.create_unique_name(layer, out_workspace))
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
                    status_writer.send_percent(i/count, 'copied file {0}.'.format(dsc.name), 'clip_data')
                    clipped += 1
                    if out_format in ('LPK', 'MPK'):
                        files_to_package.append(f.getOutput(0))
                    i += 1.
                    continue

            # Map document
            elif dsc.dataType == 'MapDocument':
                clip_mxd_layers(dsc.catalogPath, clip_poly)

            status_writer.send_percent(i/count, 'clipped {0}.'.format(dsc.name), 'clip_data')
            i += 1.
            clipped += 1
        # Continue. Process as many as possible.
        except Exception as ex:
            status_writer.send_percent(i/count, 'Skipped: {0}. {1}.'.format(os.path.basename(ds), repr(ex)), 'clip_data')
            i += 1.
            skipped += 1
            pass

    if arcpy.env.workspace.endswith('.gdb'):
        out_workspace = os.path.dirname(arcpy.env.workspace)

    if clipped > 0:
        if out_format == 'MPK':
            create_mxd_or_mpk(out_workspace, files_to_package, True)
            status_writer.send_status('Created output map package.')
            shutil.move(os.path.join(out_workspace, 'output.mpk'), os.path.join(os.path.dirname(out_workspace), 'output.mpk'))
        elif out_format == 'LPK':
            create_lpk(out_workspace, files_to_package)
            status_writer.send_status('Created output layer package.')
        else:
            create_mxd_or_mpk(out_workspace)
            zip_file = task_utils.zip_data(out_workspace, 'output.zip')
            status_writer.send_status('Created the output zip file.')
            shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
    else:
        status_writer.send_status('No clip results.')
    task_utils.report(os.path.join(request['folder'], '_report.md'), request['task'], clipped, skipped)
# End clip_data function
