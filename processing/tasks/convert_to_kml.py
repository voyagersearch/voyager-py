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
import glob
import collections
import shutil
import urllib2
import requests
from utils import status
from utils import task_utils


result_count = 0
processed_count = 0.
status_writer = status.Writer()
import arcpy
skipped_reasons = {}
errors_reasons = {}
layer_name = ''
existing_fields = []
new_fields = []
field_values = []


def execute(request):
    """Converts each input dataset to kml (.kmz).
    :param request: json as a dict.
    """
    converted = 0
    skipped = 0
    errors = 0
    global result_count
    parameters = request['params']

    out_workspace = os.path.join(request['folder'], 'temp')
    if not os.path.exists(out_workspace):
        os.makedirs(out_workspace)

    # Get the boundary box extent for input to KML tools.
    extent = ''
    try:
        try:
            ext = task_utils.get_parameter_value(parameters, 'processing_extent', 'wkt')
            if ext:
                sr = task_utils.get_spatial_reference("4326")
                extent = task_utils.from_wkt(ext, sr)
        except KeyError:
            ext = task_utils.get_parameter_value(parameters, 'processing_extent', 'feature')
            if ext:
                extent = arcpy.Describe(ext).extent
    except KeyError:
        pass

    result_count, response_index = task_utils.get_result_count(parameters)
    # Query the index for results in groups of 25.
    query_index = task_utils.QueryIndex(parameters[response_index])
    fl = query_index.fl
    query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
    fq = query_index.get_fq()
    if fq:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')
        query += fq
    elif 'ids' in parameters[response_index]:
        groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')
    else:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')

    # Begin processing
    status_writer.send_percent(0.0, _('Starting to process...'), 'convert_to_kml')
    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    for group in groups:
        if fq:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
        elif 'ids' in parameters[response_index]:
            results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
        else:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

        docs = results.json()['response']['docs']
        input_items = task_utils.get_input_items(docs)
        if not input_items:
            input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])

        input_rows = collections.defaultdict(list)
        for doc in docs:
            if 'path' not in doc:
               input_rows[doc['name']].append(doc)
        if input_rows:
            result = convert_to_kml(input_rows, out_workspace, extent)
            converted += result[0]
            errors += result[1]
            skipped += result[2]

        if input_items:
            result = convert_to_kml(input_items, out_workspace, extent)
            converted += result[0]
            errors += result[1]
            skipped += result[2]

        if not input_items and not input_rows:
            status_writer.send_state(status.STAT_FAILED, _('No items to process. Check if items exist.'))
            return

    # Zip up kmz files if more than one.
    if converted > 1:
        status_writer.send_status("Converted: {}".format(converted))
        zip_file = task_utils.zip_data(out_workspace, 'output.zip')
        shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    elif converted == 1:
        try:
            kml_file = glob.glob(os.path.join(out_workspace, '*.kmz'))[0]
            tmp_lyr = arcpy.KMLToLayer_conversion(kml_file, out_workspace, 'kml_layer')
            task_utils.make_thumbnail(tmp_lyr.getOutput(0), os.path.join(request['folder'], '_thumb.png'))
        except arcpy.ExecuteError:
            pass
        shutil.move(kml_file, os.path.join(request['folder'], os.path.basename(kml_file)))

    # Update state if necessary.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(errors + skipped))
    task_utils.report(os.path.join(request['folder'], '__report.json'), converted, skipped, errors, errors_reasons, skipped_reasons)


def convert_to_kml(input_items, out_workspace, extent, show_progress=False):
    converted = 0
    errors = 0
    skipped = 0
    global processed_count
    global layer_name
    global existing_fields
    global new_fields
    global field_values


    arcpy.env.overwriteOutput = True
    for ds, out_name in input_items.iteritems():
        try:
            # -----------------------------------------------
            # If the item is a service layer, process and continue.
            # -----------------------------------------------
            if ds.startswith('http'):
                try:
                    service_layer = task_utils.ServiceLayer(ds, extent.JSON, 'esriGeometryPolygon')
                    arcpy.env.overwriteOutput = True
                    oid_groups = service_layer.object_ids
                    out_features = None
                    g = 0.
                    group_cnt = service_layer.object_ids_cnt
                    if not arcpy.Exists(os.path.join(out_workspace, 'temp.gdb')):
                        temp_gdb = arcpy.CreateFileGDB_management(out_workspace, 'temp.gdb')
                        temp_gdb = temp_gdb[0]
                    else:
                        temp_gdb = os.path.join(out_workspace, 'temp.gdb')
                    for group in oid_groups:
                        g += 1
                        group = [oid for oid in group if oid]
                        where = '{0} IN {1}'.format(service_layer.oid_field_name, tuple(group))
                        url = ds + "/query?where={}&outFields={}&returnGeometry=true&f=json&".format(where, '*')
                        feature_set = arcpy.FeatureSet()
                        try:
                            feature_set.load(url)
                        except Exception:
                            continue
                        if not out_features:
                            out_features = arcpy.CopyFeatures_management(feature_set, task_utils.create_unique_name(out_name, temp_gdb))
                        else:
                            features = arcpy.CopyFeatures_management(feature_set, task_utils.create_unique_name(out_name, temp_gdb))
                            arcpy.Append_management(features, out_features, 'NO_TEST')
                            try:
                                arcpy.Delete_management(features)
                            except arcpy.ExecuteError:
                                pass
                        status_writer.send_percent(float(g) / group_cnt * 100, '', 'convert_to_kml')
                    arcpy.MakeFeatureLayer_management(out_features, out_name)
                    arcpy.LayerToKML_conversion(out_name, '{0}.kmz'.format(os.path.join(out_workspace, out_name)), 1, boundary_box_extent=extent)
                    processed_count += 1.
                    converted += 1
                    status_writer.send_percent(processed_count / result_count, _('Converted: {0}').format(ds), 'convert_to_kml')
                    continue
                except Exception as ex:
                    status_writer.send_state(status.STAT_WARNING, str(ex))
                    errors += 1
                    errors_reasons[ds] = ex.message
                    continue

            # Is the input a mxd data frame.
            map_frame_name = task_utils.get_data_frame_name(ds)
            if map_frame_name:
                ds = ds.split('|')[0].strip()

            # -------------------------------
            # Is the input a geometry feature
            # -------------------------------
            if isinstance(out_name, list):
                increment = task_utils.get_increment(result_count)
                for row in out_name:
                    try:
                        name = arcpy.ValidateTableName(ds, 'in_memory')
                        name = os.path.join('in_memory', name)
                        # Clip the geometry.
                        geo_json = row['[geo]']
                        geom = arcpy.AsShape(geo_json)
                        row.pop('[geo]')
                        if not arcpy.Exists(name):
                            if arcpy.env.outputCoordinateSystem:
                                layer_name = arcpy.CreateFeatureclass_management('in_memory', os.path.basename(name), geom.type.upper())
                            else:
                                arcpy.env.outputCoordinateSystem = 4326
                                layer_name = arcpy.CreateFeatureclass_management('in_memory', os.path.basename(name), geom.type.upper())
                            # layer_name = arcpy.MakeFeatureLayer_management(name, 'flayer')
                            existing_fields = [f.name for f in arcpy.ListFields(layer_name)]
                            new_fields = []
                            field_values = []
                            for field, value in row.iteritems():
                                valid_field = arcpy.ValidateFieldName(field, 'in_memory')
                                new_fields.append(valid_field)
                                field_values.append(value)
                                arcpy.AddField_management(layer_name, valid_field, 'TEXT')
                        else:
                            if not geom.type.upper() == arcpy.Describe(name).shapeType.upper():
                                name = arcpy.CreateUniqueName(os.path.basename(name), 'in_memory')
                                if arcpy.env.outputCoordinateSystem:
                                    layer_name = arcpy.CreateFeatureclass_management('in_memory', os.path.basename(name), geom.type.upper())
                                else:
                                    arcpy.env.outputCoordinateSystem = 4326
                                    layer_name = arcpy.CreateFeatureclass_management('in_memory', os.path.basename(name), geom.type.upper())

                                existing_fields = [f.name for f in arcpy.ListFields(layer_name)]
                                new_fields = []
                                field_values = []
                                for field, value in row.iteritems():
                                    valid_field = arcpy.ValidateFieldName(field, 'in_memory')
                                    new_fields.append(valid_field)
                                    field_values.append(value)
                                    if not valid_field in existing_fields:
                                        arcpy.AddField_management(layer_name, valid_field, 'TEXT')

                        with arcpy.da.InsertCursor(layer_name, ["SHAPE@"] + new_fields) as icur:
                            icur.insertRow([geom] + field_values)

                        arcpy.MakeFeatureLayer_management(layer_name, os.path.basename(name))
                        arcpy.LayerToKML_conversion(os.path.basename(name),
                                                    '{0}.kmz'.format(os.path.join(out_workspace, os.path.basename(name))),
                                                    1,
                                                    boundary_box_extent=extent)
                        if (processed_count % increment) == 0:
                            status_writer.send_percent(float(processed_count) / result_count, _('Converted: {0}').format(row['name']), 'convert_to_kml')
                        processed_count += 1
                        converted += 1
                    except KeyError:
                        processed_count += 1
                        skipped += 1
                        skipped_reasons[ds] = 'Invalid input type'
                        status_writer.send_state(_(status.STAT_WARNING, 'Invalid input type: {0}').format(ds))
                    except Exception as ex:
                        processed_count += 1
                        errors += 1
                        errors_reasons[ds] = ex.message
                        continue
                del icur
                continue


            dsc = arcpy.Describe(ds)

            if os.path.exists(os.path.join('{0}.kmz'.format(os.path.join(out_workspace, out_name)))):
                out_name = os.path.basename(arcpy.CreateUniqueName(out_name + '.kmz', out_workspace))[:-4]

            if dsc.dataType == 'FeatureClass':
                arcpy.MakeFeatureLayer_management(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.LayerToKML_conversion(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'ShapeFile':
                arcpy.MakeFeatureLayer_management(ds, dsc.name[:-4])
                if out_name == '':
                    out_name = dsc.name[:-4]
                arcpy.LayerToKML_conversion(dsc.name[:-4],
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'RasterDataset':
                arcpy.MakeRasterLayer_management(ds, dsc.name)
                if out_name == '':
                    out_name = dsc.name
                arcpy.LayerToKML_conversion(dsc.name,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'Layer':
                if out_name == '':
                    if dsc.name.endswith('.lyr'):
                        out_name = dsc.name[:-4]
                    else:
                        out_name = dsc.name
                arcpy.LayerToKML_conversion(ds,
                                            '{0}.kmz'.format(os.path.join(out_workspace, out_name)),
                                            1,
                                            boundary_box_extent=extent)
                converted += 1

            elif dsc.dataType == 'FeatureDataset':
                arcpy.env.workspace = ds
                for fc in arcpy.ListFeatureClasses():
                    arcpy.MakeFeatureLayer_management(fc, 'tmp_lyr')
                    arcpy.LayerToKML_conversion('tmp_lyr',
                                                '{0}.kmz'.format(os.path.join(out_workspace, fc)),
                                                1,
                                                boundary_box_extent=extent)
                    converted += 1

            elif dsc.dataType == 'CadDrawingDataset':
                arcpy.env.workspace = dsc.catalogPath
                for cad_fc in arcpy.ListFeatureClasses():
                    if cad_fc.lower() == 'annotation':
                        try:
                            cad_anno = arcpy.ImportCADAnnotation_conversion(
                                cad_fc,
                                arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB)
                            )
                        except arcpy.ExecuteError:
                            cad_anno = arcpy.ImportCADAnnotation_conversion(
                                cad_fc,
                                arcpy.CreateUniqueName('cadanno', arcpy.env.scratchGDB),
                                1
                            )
                        arcpy.MakeFeatureLayer_management(cad_anno, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.LayerToKML_conversion('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    1,
                                                    boundary_box_extent=extent)
                        converted += 1
                    else:
                        arcpy.MakeFeatureLayer_management(cad_fc, 'cad_lyr')
                        name = '{0}_{1}'.format(dsc.name[:-4], cad_fc)
                        arcpy.LayerToKML_conversion('cad_lyr',
                                                    '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                                    1,
                                                    boundary_box_extent=extent)
                        converted += 1

            # Map document to KML.
            elif dsc.dataType == 'MapDocument':
                mxd = arcpy.mapping.MapDocument(ds)
                if map_frame_name:
                    data_frames = arcpy.mapping.ListDataFrames(mxd, map_frame_name)
                else:
                    data_frames = arcpy.mapping.ListDataFrames(mxd)
                for df in data_frames:
                    name = '{0}_{1}'.format(dsc.name[:-4], df.name)
                    arcpy.MapToKML_conversion(ds,
                                              df.name,
                                              '{0}.kmz'.format(os.path.join(out_workspace, name)),
                                              extent_to_export=extent)
                converted += 1

            else:
                processed_count += 1
                status_writer.send_percent(processed_count / result_count, _('Invalid input type: {0}').format(dsc.name), 'convert_to_kml')
                skipped += 1
                skipped_reasons[ds] = _('Invalid input type: {0}').format(dsc.dataType)
                continue
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Converted: {0}').format(ds), 'convert_to_kml')
            status_writer.send_status(_('Converted: {0}').format(ds))
        except Exception as ex:
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(ds), 'convert_to_kml')
            status_writer.send_status(_('WARNING: {0}').format(repr(ex)))
            errors_reasons[ds] = repr(ex)
            errors += 1
            pass

    return converted, errors, skipped
