"""Copies data to an existing geodatabase."""
import os
import sys
import shutil
import tempfile
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def is_feature_dataset(workspace):
    """Checks if the workspace is a feature dataset.
    :param workspace: workspace path
    :rtype : bool
    """
    if os.path.splitext(os.path.dirname(workspace))[1] in ('.gdb', '.mdb', '.sde'):
        if arcpy.Exists(workspace):
            return True
    return False


def execute(request):
    """Copies data to a new or existing geodatabase.
    :param request: json as a dict.
    """
    added = 0
    skipped = 0
    errors = 0
    status_writer = status.Writer()
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)

    # Get the target workspace location.
    out_gdb = task_utils.get_parameter_value(parameters, 'target_workspace', 'value')

    # Retrieve the coordinate system code.
    out_coordinate_system = task_utils.get_parameter_value(parameters, 'output_projection', 'code')
    if out_coordinate_system:
        arcpy.env.outputCoordinateSystem = task_utils.get_spatial_reference(out_coordinate_system)

    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    # Check if the geodatabase exists or if it is a feature dataset.
    is_fds = False
    if not os.path.exists(out_gdb):
        # For performance reasons, only doing this if the out_gdb does not exist.
        if is_feature_dataset(out_gdb):
            is_fds = True
        else:
            status_writer.send_state(status.STAT_FAILED, '{0} does not exist.'.format(out_gdb))
            sys.exit(1)

    if out_gdb.endswith('.sde') or os.path.dirname(out_gdb).endswith('.sde'):
        status_writer.send_status('Connecting to {0}...'.format(out_gdb))
    arcpy.env.workspace = out_gdb

    i = 1.
    count = len(input_items)
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
                if not is_fds:
                    fds_name = os.path.basename(task_utils.create_unique_name(dsc.name, out_gdb))
                    fds = arcpy.management.CreateFeatureDataset(out_gdb, fds_name).getOutput(0)
                else:
                    fds = out_gdb
                arcpy.env.workspace = dsc.catalogPath
                for fc in arcpy.ListFeatureClasses():
                    name = os.path.basename(task_utils.create_unique_name(fc, out_gdb))
                    arcpy.management.CopyFeatures(fc, os.path.join(fds, name))
                arcpy.env.workspace = out_gdb

            elif dsc.dataType == 'RasterDataset':
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                if out_name == '':
                    arcpy.management.CopyRaster(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRaster(ds, task_utils.create_unique_name(out_name, out_gdb))

            elif dsc.dataType == 'RasterCatalog':
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
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
                        if is_fds:
                            name = os.path.dirname(name)
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
                    temp_dir = tempfile.mkdtemp()
                    kml_layer = arcpy.conversion.KMLToLayer(dsc.catalogPath, temp_dir, name)
                    group_layer = arcpy.mapping.Layer(os.path.join(temp_dir, '{}.lyr'.format(name)))
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.management.CopyFeatures(layer, task_utils.create_unique_name(layer, out_gdb))
                        elif layer.isRasterLayer:
                            if is_fds:
                                out_gdb = os.path.dirname(out_gdb)
                            arcpy.management.CopyRaster(layer, task_utils.create_unique_name(layer, out_gdb))
                    # Clean up temp KML results.
                    arcpy.management.Delete(os.path.join(temp_dir, '{}.lyr'.format(name)))
                    arcpy.management.Delete(kml_layer)
                else:
                    status_writer.send_percent(i/count, 'Cannot add {0}.'.format(ds), 'add_to_geodatabase')
                    i += 1.
                    skipped += 1
                    continue

            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(mxd)
                for layer in layers:
                    if layer.isFeatureLayer:
                        arcpy.management.CopyFeatures(layer.dataSource,
                                                      task_utils.create_unique_name(layer.name, out_gdb))
                    elif layer.isRasterLayer:
                        if is_fds:
                            out_gdb = os.path.dirname(out_gdb)
                        arcpy.management.CopyRaster(layer.dataSource,
                                                    task_utils.create_unique_name(layer.name, out_gdb))
                table_views = arcpy.mapping.ListTableViews(mxd)
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                for table_view in table_views:
                    arcpy.management.CopyRows(table_view.dataSource,
                                              task_utils.create_unique_name(table_view.name, out_gdb))
                out_gdb = arcpy.env.workspace

            elif dsc.dataType.find('Table') > 0:
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                if out_name == '':
                    arcpy.management.CopyRows(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.management.CopyRows(ds, task_utils.create_unique_name(out_name, out_gdb))

            else:
                # Try to copy any other types such as topologies, network datasets, etc.
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                arcpy.Copy_management(ds, task_utils.create_unique_name(dsc.name, out_gdb))

            out_gdb = arcpy.env.workspace
            status_writer.send_percent(i/count, 'Added {0}.'.format(ds), 'add_to_geodatabase')
            i += 1.
            added += 1
        # Continue if an error. Process as many as possible.
        except Exception as ex:
            status_writer.send_percent(i/count,
                                       'Failed to add: {0}. {1}.'.format(os.path.basename(ds), repr(ex)),
                                       'add_to_geodatabase')
            skipped += 1
            errors += 1
            pass

    try:
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        status_writer.send_status('Could not copy thumbnail.')
        pass

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, '{0} results could not be added.'.format(skipped))
        task_utils.report(os.path.join(task_folder, '_report.md'), request['task'], added, skipped, errors)
