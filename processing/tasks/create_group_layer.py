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
import requests
import arcpy
from utils import status
from utils import task_utils


status_writer = status.Writer()
result_count = 0
processed_count = 0.
skipped_reasons = {}
errors_reasons = {}
arcpy.env.overwriteOutput = True


def execute(request):
    """Copies files to a target folder.
    :param request: json as a dict.
    """
    added = 0
    skipped = 0
    errors = 0
    global result_count
    parameters = request['params']

    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    layer_name = task_utils.get_parameter_value(parameters, 'layer_name', 'value')
    result_count, response_index = task_utils.get_result_count(parameters)

    # Query the index for results in groups of 25.
    query_index = task_utils.QueryIndex(parameters[response_index])
    fl = query_index.fl
    query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
    # query = '{0}{1}{2}'.format("http://localhost:8888/solr/v0", '/select?&wt=json', fl)
    fq = query_index.get_fq()
    if fq:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')
        query += fq
    elif 'ids' in parameters[response_index]:
        groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')
    else:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')

    status_writer.send_percent(0.0, _('Starting to process...'), 'create_group_layer')
    i = 0.
    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    for group in groups:
        i += len(group) - group.count('')
        if fq:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
        elif 'ids' in parameters[response_index]:
            results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
        else:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

        docs = results.json()['response']['docs']
        if not docs:
            docs = parameters[response_index]['response']['docs']
        input_items = []
        for doc in docs:
            if 'path' in doc:
                input_items.append(doc['path'])
            elif '[lyrFile]' in doc:
                input_items.append(doc['[lyrFile]'])
        result = create_layer_file(input_items, request['folder'], layer_name)
        added += result[0]
        errors += result[1]
        skipped += result[2]

    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '__report.json'), added, skipped, errors, errors_reasons, skipped_reasons)


def create_layer_file(input_items, layer_folder, layer_name, show_progress=False):
    """Creates a layer for input items in the appropriate meta folders."""
    added = 0
    skipped = 0
    errors = 0
    global processed_count

    layers = 0
    lyr_mxd = arcpy.mapping.MapDocument(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'GroupLayerTemplate.mxd'))
    data_frame = arcpy.mapping.ListDataFrames(lyr_mxd)[0]
    group_layer = arcpy.mapping.ListLayers(lyr_mxd, 'Group Layer', data_frame)[0]
    group_layer.name = layer_name

    for input_item in input_items:
        try:
            name = os.path.splitext(os.path.basename(input_item))[0]
            dsc = arcpy.Describe(input_item)

            try:
                if dsc.dataType in ('FeatureClass', 'Shapefile', 'ShapeFile'):
                    feature_layer = arcpy.MakeFeatureLayer_management(input_item, name)
                    arcpy.mapping.AddLayerToGroup(data_frame, group_layer, feature_layer.getOutput(0))
                    layers += 1
                elif dsc.dataType == 'Layer' and dsc.catalogPath.endswith('.lyr'):
                    layer = arcpy.mapping.Layer(input_item)
                    arcpy.mapping.AddLayerToGroup(data_frame, group_layer, layer)
                    layers += 1
                elif dsc.dataType == 'RasterDataset':
                    raster_layer = arcpy.MakeRasterLayer_management(input_item, name)
                    arcpy.mapping.AddLayerToGroup(data_frame, group_layer, raster_layer.getOutput(0))
                    layers += 1
                elif dsc.dataType in ('CadDrawingDataset', 'FeatureDataset'):
                    arcpy.env.workspace = input_item
                    for fc in arcpy.ListFeatureClasses():
                        dataset_name = os.path.splitext(os.path.basename(input_item))[0]
                        l = arcpy.MakeFeatureLayer_management(fc, '{0}_{1}'.format(dataset_name, os.path.basename(fc)))
                        arcpy.mapping.AddLayerToGroup(data_frame, group_layer, l.getOutput(0))
                    arcpy.ResetEnvironments()
                    layers += 1
                elif dsc.catalogPath.lower().endswith('.tab') or dsc.catalogPath.lower().endswith('.mif'):
                    try:
                        arcpy.ImportToolbox(r"C:\Program Files (x86)\DataEast\TAB Reader\Toolbox\TAB Reader.tbx")
                        arcpy.GPTabsToArcGis_TR(dsc.catalogPath, False, '', True, True, os.path.join(layer_folder, 'temp.lyr'))
                        layer = arcpy.mapping.Layer(os.path.join(layer_folder, 'temp.lyr'))
                        arcpy.mapping.AddLayerToGroup(data_frame, group_layer, layer)
                        os.remove(os.path.join(layer_folder, 'temp.lyr'))
                        layers += 1
                    except RuntimeError as re:
                        skipped += 1
                        status_writer.send_status(_('Invalid input type: MapInfo'))
                        skipped_reasons[name] = _('Invalid input type: MapInfo')
                        continue
                else:
                    skipped += 1
                    status_writer.send_status(_('Invalid input type: {0}').format(dsc.name))
                    skipped_reasons[name] = _('Invalid input type: {0}').format(dsc.dataType)
                    continue
            except arcpy.ExecuteError:
                errors += 1
                status_writer.send_status(arcpy.GetMessages(2))
                errors_reasons[name] = arcpy.GetMessages(2)
                continue
            added += 1

            # Update the status.
            if layers > 0:
                processed_count += 1
                status_writer.send_percent(processed_count / result_count, _('Added: {0}').format(os.path.basename(input_item)), 'create_group_layer')
        except IOError as io_err:
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(os.path.basename(input_item)), 'create_group_layer')
            status_writer.send_status(_('FAIL: {0}').format(repr(io_err)))
            errors_reasons[input_item] = repr(io_err)
            errors += 1
            pass

    if layers > 0:
        group_layer.saveACopy(os.path.join(layer_folder, '{0}.lyr'.format(layer_name)))
    del lyr_mxd

    return added, errors, skipped
