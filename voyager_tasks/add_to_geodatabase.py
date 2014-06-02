# -*- coding: utf-8 -*-
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
    """Copies data to an existing geodatabase or feature dataset.
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
            status_writer.send_state(status.STAT_FAILED, _('{0} does not exist').format(out_gdb))
            sys.exit(1)

    status_writer.send_status(_('Setting the output workspace...'))
    arcpy.env.workspace = out_gdb
    i = 1.
    count = len(input_items)
    status_writer.send_percent(0.0, _('Starting to process...'), 'add_to_geodatabase')
    for ds, out_name in input_items.iteritems():
        try:
            dsc = arcpy.Describe(ds)
            if dsc.dataType == 'FeatureClass':
                if out_name == '':
                    arcpy.CopyFeatures_management(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.CopyFeatures_management(ds, task_utils.create_unique_name(out_name, out_gdb))

            elif dsc.dataType == 'ShapeFile':
                if out_name == '':
                    arcpy.CopyFeatures_management(ds, task_utils.create_unique_name(dsc.name[:-4], out_gdb))
                else:
                    arcpy.CopyFeatures_management(ds, task_utils.create_unique_name(out_name, out_gdb))

            elif dsc.dataType == 'FeatureDataset':
                if not is_fds:
                    fds_name = os.path.basename(task_utils.create_unique_name(dsc.name, out_gdb))
                    fds = arcpy.CreateFeatureDataset_management(out_gdb, fds_name).getOutput(0)
                else:
                    fds = out_gdb
                arcpy.env.workspace = dsc.catalogPath
                for fc in arcpy.ListFeatureClasses():
                    name = os.path.basename(task_utils.create_unique_name(fc, out_gdb))
                    arcpy.CopyFeatures_management(fc, os.path.join(fds, name))
                arcpy.env.workspace = out_gdb

            elif dsc.dataType == 'RasterDataset':
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                if out_name == '':
                    arcpy.CopyRaster_management(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.CopyRaster_management(ds, task_utils.create_unique_name(out_name, out_gdb))

            elif dsc.dataType == 'RasterCatalog':
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                if out_name == '':
                    arcpy.CopyRasterCatalogItems_management(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.CopyRasterCatalogItems_management(ds, task_utils.create_unique_name(out_name, out_gdb))

            elif dsc.dataType == 'Layer':
                layer_from_file = arcpy.mapping.Layer(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(layer_from_file)
                for layer in layers:
                    if out_name == '':
                        name = task_utils.create_unique_name(layer.name, out_gdb)
                    else:
                        name = task_utils.create_unique_name(out_name, out_gdb)
                    if layer.isFeatureLayer:
                        arcpy.CopyFeatures_management(layer.dataSource, name)
                    elif layer.isRasterLayer:
                        if is_fds:
                            name = os.path.dirname(name)
                        arcpy.CopyRaster_management(layer.dataSource, name)

            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                cad_wks_name = os.path.splitext(dsc.name)[0]
                for cad_fc in arcpy.ListFeatureClasses():
                    arcpy.CopyFeatures_management(
                        cad_fc,
                        task_utils.create_unique_name('{0}_{1}'.format(cad_wks_name, cad_fc), out_gdb)
                    )
                arcpy.env.workspace = out_gdb

            elif dsc.dataType == 'File':
                if dsc.catalogPath.endswith('.kml') or dsc.catalogPath.endswith('.kmz'):
                    name = os.path.splitext(dsc.name)[0]
                    temp_dir = tempfile.mkdtemp()
                    kml_layer = arcpy.KMLToLayer_conversion(dsc.catalogPath, temp_dir, name)
                    group_layer = arcpy.mapping.Layer(os.path.join(temp_dir, '{}.lyr'.format(name)))
                    for layer in arcpy.mapping.ListLayers(group_layer):
                        if layer.isFeatureLayer:
                            arcpy.CopyFeatures_management(layer, task_utils.create_unique_name(layer, out_gdb))
                        elif layer.isRasterLayer:
                            if is_fds:
                                out_gdb = os.path.dirname(out_gdb)
                            arcpy.CopyRaster_management(layer, task_utils.create_unique_name(layer, out_gdb))
                    # Clean up temp KML results.
                    arcpy.Delete_management(os.path.join(temp_dir, '{}.lyr'.format(name)))
                    arcpy.Delete_management(kml_layer)
                else:
                    status_writer.send_percent(i/count, _('Invalid input type: {0}').format(dsc.name), 'add_to_geodatabase')
                    i += 1.
                    skipped += 1
                    continue

            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(dsc.catalogPath)
                layers = arcpy.mapping.ListLayers(mxd)
                for layer in layers:
                    if layer.isFeatureLayer:
                        arcpy.CopyFeatures_management(layer.dataSource,
                                                      task_utils.create_unique_name(layer.name, out_gdb))
                    elif layer.isRasterLayer:
                        if is_fds:
                            out_gdb = os.path.dirname(out_gdb)
                        arcpy.CopyRaster_management(layer.dataSource,
                                                    task_utils.create_unique_name(layer.name, out_gdb))
                table_views = arcpy.mapping.ListTableViews(mxd)
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                for table_view in table_views:
                    arcpy.CopyRows_management(table_view.dataSource,
                                              task_utils.create_unique_name(table_view.name, out_gdb))
                out_gdb = arcpy.env.workspace

            elif dsc.dataType.find('Table') > 0:
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                if out_name == '':
                    arcpy.CopyRows_management(ds, task_utils.create_unique_name(dsc.name, out_gdb))
                else:
                    arcpy.CopyRows_management(ds, task_utils.create_unique_name(out_name, out_gdb))

            else:
                # Try to copy any other types such as topologies, network datasets, etc.
                if is_fds:
                    out_gdb = os.path.dirname(out_gdb)
                arcpy.Copy_management(ds, task_utils.create_unique_name(dsc.name, out_gdb))

            out_gdb = arcpy.env.workspace
            status_writer.send_percent(i/count, _('Added: {0}').format(ds), 'add_to_geodatabase')
            i += 1.
            added += 1
        # Continue if an error. Process as many as possible.
        except Exception as ex:
            status_writer.send_percent(i/count, _('Skipped: {0}').format(ds), 'add_to_geodatabase')
            status_writer.send_status(_('FAIL: {0}').format(repr(ex)))
            errors += 1
            pass

    try:
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Update state if necessary.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(task_folder, '_report.json'), added, skipped, errors)
