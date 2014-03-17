"""Copies data to a new or existing geodatabase."""
import os
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def execute(request):
    """Copies data to a new or existing geodatabase.
    :param request: json as a dict.
    """
    added = 0
    skipped = 0

    # Retrieve input items to be clipped.
    parameters = request['params']
    in_data = task_utils.find(lambda p: p['name'] == 'input_items', parameters)
    docs = in_data.get('response').get('docs')
    input_items = str(dict((task_utils.get_feature_data(v), v['name']) for v in docs))

    # Get the target workspace location.
    output_workspace = task_utils.find(lambda p: p['name'] == 'target_workspace', parameters)['path']

    # Retrieve the coordinate system code.
    out_coordinate_system = task_utils.find(lambda p: p['name'] == 'output_projection', parameters)['code']
    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    try:
        # Voyager Job Runner: passes a dictionary of inputs and output names.
        input_items = eval(input_items)
    except SyntaxError:
        # If not output names are passed in.
        input_items = dict((k, '') for k in input_items.split(';'))

    # Create the geodatabase if it does not exist.
    if not output_workspace.endswith('.gdb'):
        out_gdb = arcpy.management.CreateFileGDB(output_workspace, 'output.gdb').getOutput(0)
    else:
        out_gdb = output_workspace
    arcpy.env.workspace = out_gdb

    # Set the output coordinate system environment.
    if out_coordinate_system is not None:
        arcpy.env.outputCoordinateSystem = int(out_coordinate_system)

    i = 1.
    count = len(input_items)
    status_writer = status.Writer()
    status_writer.send_status('Starting to add data to {0}...'.format(out_gdb))
    for ds, out_name in input_items.iteritems():
        try:
            dsc = arcpy.Describe(ds)
            if dsc.dataType == 'FeatureClass':
                if out_name == '':
                    arcpy.management.CopyFeatures(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyFeatures(ds, task_utils.create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'ShapeFile':
                if out_name == '':
                    arcpy.management.CopyFeatures(ds, task_utils.create_unique_name(dsc.name[:-4], out_gdb))
                else:
                    arcpy.management.CopyFeatures(ds, task_utils.create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'FeatureDataset':
                fds = arcpy.management.CreateFeatureDataset(out_gdb, dsc.name)
                arcpy.env.workspace = dsc.catalogPath
                for fc in arcpy.ListFeatureClasses():
                    name = os.path.basename(task_utils.create_unique_name(fc, out_gdb))
                    arcpy.management.CopyFeatures(fc, os.path.join(fds.getOutput(0), name))
                arcpy.env.workspace = out_gdb
            elif dsc.dataType == 'RasterDataset':
                if out_name == '':
                    arcpy.management.CopyRaster(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRaster(ds, task_utils.create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'RasterCatalog':
                if out_name == '':
                    arcpy.management.CopyRasterCatalogItems(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRasterCatalogItems(ds, task_utils.create_unique_name(out_name, out_gdb))
            elif dsc.dataType == 'Layer':
                layer_from_file = arcpy.mapping.Layer(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(layer_from_file)
                for layer in layers:
                    if out_name == '':
                        name = task_utils.create_unique_name(layer.name, out_gdb)
                    else:
                        name = task_utils.create_unique_name(out_name, out_gdb)
                    if layer.isFeatureLayer:
                        arcpy.management.CopyFeatures(layer.dataSource, name)
                    elif layer.isRasterLayer:
                        arcpy.management.CopyRaster(layer.dataSource, name)
            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    arcpy.management.CopyFeatures(
                        cad_fc,
                        task_utils.create_unique_name('{0}_{1}'.format(cad_wks_name, cad_fc), out_gdb)
                    )
                arcpy.env.workspace = out_gdb
            elif dsc.dataType == 'File':
                if dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                    name = os.path.splitext(dsc.name)[0]
                    kml_layer = arcpy.conversion.KMLToLayer(dsc.catalogPath, arcpy.env.scratchFolder, name)
                    group_layer = arcpy.mapping.Layer(os.path.join(arcpy.env.scratchFolder, '{}.lyr'.format(name)))
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.management.CopyFeatures(layer, arcpy.ValidateTableName(layer))
                    # Clean up temp KML results.
                    arcpy.management.Delete(os.path.join(arcpy.env.scratchFolder, '{}.lyr'.format(name)))
                    arcpy.management.Delete(kml_layer[1])
            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(mxd)
                for layer in layers:
                    if layer.isFeatureLayer:
                        arcpy.management.CopyFeatures(layer.dataSource,
                                                      task_utils.create_unique_name(layer.name, out_gdb))
                    elif layer.isRasterLayer:
                        arcpy.management.CopyRaster(layer.dataSource,
                                                    task_utils.create_unique_name(layer.name, out_gdb))
            elif dsc.dataType.find('Table') > 0:
                if out_name == '':
                    arcpy.management.CopyRows(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRows(ds, task_utils.create_unique_name(out_name, out_gdb))

            status_writer.send_percent(i/count, 'Added {0}.'.format(ds), 'add_to_geodatabase')
            i += 1.
            added += 1
        # Continue if an error. Process as many as possible.
        except Exception as ex:
            status_writer.send_percent(i/count, 'Failed to add: {0}. {1}.'.format(os.path.basename(ds), repr(ex)), 'add_to_geodatabase')
            skipped += 0
            pass

    task_utils.report(os.path.join(task_folder, '_report.md'), request['task'], added, skipped)
    status_writer.send_status('Completed.')
# End add_to_gdb function

