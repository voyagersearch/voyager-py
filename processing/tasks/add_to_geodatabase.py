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
import collections
import shutil
import tempfile
import urllib2
from utils import status
from utils import task_utils


status_writer = status.Writer()
result_count = 0
processed_count = 0.
import arcpy


# def export_to_shp(jobs, file_name, output_folder):
#     """Exports results to a shapefile.
#     :param jobs: list of jobs (a job contains the result information)
#     :param file_name: the output file name
#     :param output_folder: the output task folder
#     """
#     import ogr
#     driver = ogr.GetDriverByName("ESRI Shapefile")
#     for job in jobs:
#         try:
#             geo_json = job['[geo]']
#             if geo_json['type'].lower() == 'polygon':
#                 geometry_type = ogr.wkbPolygon
#             elif geo_json['type'].lower() == 'geometrycollection':
#                 geom = ogr.CreateGeometryFromJson("{0}".format(job['[geo]']))
#                 if geom.GetDimension() == 0:
#                     geometry_type = ogr.wkbPoint
#                 elif geom.GetDimension() == 1:
#                     geometry_type = ogr.wkbLineString
#                 else:
#                     geometry_type = ogr.wkbPolygon
#             elif geo_json['type'].lower() == 'multipolygon':
#                 geometry_type = ogr.wkbMultiPolygon
#             elif geo_json['type'].lower() == 'linestring':
#                 geometry_type = ogr.wkbLineString
#             elif geo_json['type'].lower() == 'multilinestring':
#                 geometry_type = ogr.wkbMultiLineString
#             elif geo_json['type'].lower() == 'point':
#                 geometry_type = ogr.wkbPoint
#             elif geo_json['type'].lower() == 'multipoint':
#                 geometry_type = ogr.wkbMultiPoint
#         except KeyError:
#             continue
#         except TypeError:
#             continue
#
#         if os.path.exists(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type']))):
#             shape_file = ogr.Open(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type'])), 1)
#             layer = shape_file.GetLayer()
#         else:
#             shape_file = driver.CreateDataSource(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type'])))
#             epsg_code = 4326
#             srs = ogr.osr.SpatialReference()
#             srs.ImportFromEPSG(epsg_code)
#             layer = shape_file.CreateLayer('{0}_{1}'.format(file_name, geo_json['type']), srs, geometry_type)
#             for name in jobs[0].keys():
#                 if not name == '[geo]':
#                     name = str(name)
#                     new_field = ogr.FieldDefn(name, ogr.OFTString)
#                     layer.CreateField(new_field)
#
#         try:
#             layer_def = layer.GetLayerDefn()
#             feature = ogr.Feature(layer_def)
#             geom = ogr.CreateGeometryFromJson("{0}".format(job['[geo]']))
#             feature.SetGeometry(geom)
#         except KeyError:
#             feature.SetGeometry(None)
#             pass
#         try:
#             job.pop('[geo]')
#         except KeyError:
#             pass
#         for field, value in job.iteritems():
#             field, value = str(field), str(value)
#             i = feature.GetFieldIndex(field)
#             feature.SetField(i, value)
#         layer.CreateFeature(feature)
#         shape_file.Destroy()
#         shape_file = None


def is_feature_dataset(workspace):
    """Checks if the workspace is a feature dataset.
    :param workspace: workspace path
    :rtype : bool
    """
    if os.path.splitext(os.path.dirname(workspace))[1] in ('.gdb', '.mdb', '.sde'):
        if arcpy.Exists(workspace):
            return True
    return False


def add_to_geodatabase(input_items, out_gdb, is_fds, show_progress=False):
    """Adds items to a geodatabase."""
    added = 0
    skipped = 0
    errors = 0
    global processed_count

    for ds, out_name in input_items.iteritems():
        try:
            # -----------------------------------------------
            # If the item is a service layer, process and continue.
            # -----------------------------------------------
            if ds.startswith('http'):
                try:
                    service_layer = task_utils.ServiceLayer(ds)
                    arcpy.env.overwriteOutput = True
                    oid_groups = service_layer.object_ids
                    out_features = None
                    for group in oid_groups:
                        group = [oid for oid in group if not oid == None]
                        where = '{0} IN {1}'.format(service_layer.oid_field_name, tuple(group))
                        url = ds + "/query?where={}&outFields={}&returnGeometry=true&geometryType=esriGeometryPolygon&f=json&token={}".format(where, '*', '')
                        feature_set = arcpy.FeatureSet()
                        feature_set.load(url)
                        if not out_features:
                            out_features = arcpy.CopyFeatures_management(feature_set, task_utils.create_unique_name(out_name, out_gdb))
                        else:
                            features = arcpy.CopyFeatures_management(feature_set, task_utils.create_unique_name(out_name, out_gdb))
                            arcpy.Append_management(features, out_features, 'NO_TEST')
                    processed_count += 1.
                    added += 1
                    status_writer.send_percent(processed_count / result_count, _('Added: {0}').format(ds), 'add_to_geodatabase')
                    continue
                except Exception as ex:
                    status_writer.send_state(status.STAT_WARNING, str(ex))
                    errors += 1
                    continue

            # ------------------------------
            # Is the input a mxd data frame.
            # ------------------------------
            map_frame_name = task_utils.get_data_frame_name(ds)
            if map_frame_name:
                ds = ds.split('|')[0].strip()

            # -------------------------------
            # Is the input a geometry feature
            # -------------------------------
            if isinstance(out_name, list):
                for row in out_name:
                    try:
                        name = os.path.join(out_gdb, ds)
                        if '[geo]' in row:
                            geo_json = row['[geo]']
                            geom = arcpy.AsShape(geo_json)
                            if not arcpy.Exists(name):
                                if arcpy.env.outputCoordinateSystem:
                                    arcpy.CreateFeatureclass_management(out_gdb, os.path.basename(name), geom.type.upper())
                                else:
                                    arcpy.env.outputCoordinateSystem = 4326
                                    arcpy.CreateFeatureclass_management(out_gdb, os.path.basename(name), geom.type.upper())

                                try:
                                    row.pop('[geo]')
                                except KeyError:
                                    pass
                                new_fields = []
                                field_values = []
                                for field, value in row.iteritems():
                                    new_fields.append(arcpy.ValidateFieldName(field, out_gdb))
                                    arcpy.AddField_management(name, field, 'TEXT')
                                    field_values.append(value)

                            with arcpy.da.InsertCursor(name, ["SHAPE@"] + new_fields) as icur:
                                icur.insertRow([geom] + field_values)
                        else:
                            pass
                        status_writer.send_percent(processed_count / result_count, _('Added: {0}').format(row['name']), 'add_to_geodatabase')
                        continue
                    except Exception as ex:
                        continue

            # -----------------------------
            # Check the data type and clip.
            # -----------------------------
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
                    processed_count += 1
                    status_writer.send_percent(processed_count / result_count, _('Invalid input type: {0}').format(dsc.name), 'add_to_geodatabase')
                    skipped += 1
                    continue

            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(dsc.catalogPath)
                if map_frame_name:
                    df = arcpy.mapping.ListDataFrames(mxd, map_frame_name)[0]
                    layers = arcpy.mapping.ListLayers(mxd, data_frame=df)
                else:
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
            processed_count += 1.
            status_writer.send_percent(processed_count / result_count, _('Added: {0}').format(ds), 'add_to_geodatabase')
            status_writer.send_status(_('Added: {0}').format(ds))
            added += 1
        # Continue if an error. Process as many as possible.
        except Exception as ex:
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(ds), 'add_to_geodatabase')
            status_writer.send_status(_('FAIL: {0}').format(repr(ex)))
            errors += 1
            continue

    return added, errors, skipped


def execute(request):
    """Copies data to an existing geodatabase or feature dataset.
    :param request: json as a dict.
    """
    added = 0
    errors = 0
    skipped = 0
    global result_count
    parameters = request['params']

    # Get the target workspace location.
    out_gdb = task_utils.get_parameter_value(parameters, 'target_workspace', 'value')

    # Retrieve the coordinate system code.
    out_coordinate_system = task_utils.get_parameter_value(parameters, 'output_projection', 'code')
    if not out_coordinate_system == '0':  # Same as Input
        arcpy.env.outputCoordinateSystem = task_utils.get_spatial_reference(out_coordinate_system)

    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    # Check if the geodatabase exists or if it is a feature dataset.
    is_fds = False
    if not os.path.exists(out_gdb):
        if out_gdb.endswith('.gdb'):
            arcpy.CreateFileGDB_management(os.path.dirname(out_gdb), os.path.basename(out_gdb))
            status_writer.send_status(_('Created output workspace: {0}').format(out_gdb))
        elif out_gdb.endswith('.mdb'):
            arcpy.CreatePersonalGDB_management(os.path.dirname(out_gdb), os.path.basename(out_gdb))
            status_writer.send_status(_('Created output workspace: {0}').format(out_gdb))
        elif out_gdb.endswith('.sde'):
            status_writer.send_state(status.STAT_FAILED, _('{0} does not exist').format(out_gdb))
            return
        else:
            # Possible feature dataset.
            is_fds = is_feature_dataset(out_gdb)
            if not is_fds:
                if os.path.dirname(out_gdb).endswith('.gdb'):
                    if not os.path.exists(os.path.dirname(out_gdb)):
                        arcpy.CreateFileGDB_management(os.path.dirname(os.path.dirname(out_gdb)),
                                                       os.path.basename(os.path.dirname(out_gdb)))
                    arcpy.CreateFeatureDataset_management(os.path.dirname(out_gdb), os.path.basename(out_gdb))
                elif os.path.dirname(out_gdb).endswith('.mdb'):
                    if not os.path.exists(os.path.dirname(out_gdb)):
                        arcpy.CreatePersonalGDB_management(os.path.dirname(os.path.dirname(out_gdb)),
                                                           os.path.basename(os.path.dirname(out_gdb)))
                    arcpy.CreateFeatureDataset_management(os.path.dirname(out_gdb), os.path.basename(out_gdb))


    status_writer.send_status(_('Setting the output workspace...'))
    arcpy.env.workspace = out_gdb

    result_count, response_index = task_utils.get_result_count(parameters)
    # Query the index for results in groups of 25.
    query_index = task_utils.QueryIndex(parameters[response_index])
    fl = query_index.fl
    # query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
    query = '{0}{1}{2}'.format("http://localhost:8888/solr/v0", '/select?&wt=json', fl)
    fq = query_index.get_fq()
    if fq:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')
        query += fq
    else:
        groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')

    status_writer.send_percent(0.0, _('Starting to process...'), 'add_to_geodatabase')
    for group in groups:
        if fq:
            results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
        else:
            results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

        docs = eval(results.read().replace('false', 'False').replace('true', 'True'))['response']['docs']
        input_items = task_utils.get_input_items(docs)

        input_rows = collections.defaultdict(list)
        for doc in docs:
            if 'path' not in doc:
               input_rows[doc['name']].append(doc)

        if input_rows:
            add_to_geodatabase(input_rows, out_gdb, is_fds)

        if not input_items:
            status_writer.send_state(status.STAT_FAILED, _('No items to process. Check if items exist.'))
            return
        result = add_to_geodatabase(input_items, out_gdb, is_fds)
        added += result[0]
        errors += result[1]
        skipped += result[2]

    # Copy the default thumbnail and create a report in the task folder..
    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Update state if necessary.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(task_folder, '_report.json'), added, skipped, errors)
