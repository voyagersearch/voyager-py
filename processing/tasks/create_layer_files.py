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
import json
import shutil
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
mxd_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'supportfiles', 'GroupLayerTemplate.mxd')


class ObjectEncoder(json.JSONEncoder):
    """Support non-native Python types for JSON serialization."""
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)


def update_index(file_location, layer_file, item_id, name, location, server, hdrs):
    """Update the index by re-indexng an item."""
    import zmq
    indexer = sys.argv[3].split('=')[1]
    zmq_socket = zmq.Context.instance().socket(zmq.PUSH)
    zmq_socket.connect(indexer)
    res = requests.get("{0}/api/rest/index/record/{1}".format(server, item_id), headers=hdrs)
    fields = res.json()
    fields["path_to_lyr"] = layer_file
    fields["hasLayerFile"] = True
    entry = {"action": "UPDATE", "id": item_id, "location": location, "entry": {"fields": fields}}
    zmq_socket.send_json(entry, cls=ObjectEncoder)


def execute(request):
    """Copies files to a target folder.
    :param request: json as a dict.
    """
    created = 0
    skipped = 0
    errors = 0
    global result_count
    parameters = request['params']
    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}

    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    # meta_folder = task_utils.get_parameter_value(parameters, 'meta_data_folder', 'value')
    voyager_server = sys.argv[2].split('=')[1].split('solr')[0][:-1]
    url = "{0}/api/rest/system/settings".format(voyager_server)
    response = requests.get(url, headers=headers)
    meta_folder = response.json()['folders']['meta']
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

    status_writer.send_percent(0.0, _('Starting to process...'), 'create_layer_files')
    i = 0.

    for group in groups:
        i += len(group) - group.count('')
        if fq:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
        elif 'ids' in parameters[response_index]:
            results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
        else:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

        docs = results.json()['response']['docs']
        # docs = eval(results.read().replace('false', 'False').replace('true', 'True'))['response']['docs']
        if not docs:
            docs = parameters[response_index]['response']['docs']
        input_items = []
        for doc in docs:
            if 'path' in doc:
                input_items.append((doc['id'], doc['path'], doc['name'], doc['location']))
        result = create_layer_file(input_items, meta_folder, voyager_server, headers)
        created += result[0]
        errors += result[1]
        skipped += result[2]

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass
    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '__report.json'), created, skipped, errors, errors_reasons, skipped_reasons)


def create_layer_file(input_items, meta_folder, voyager_server, hdrs, show_progress=False):
    """Creates a layer for input items in the appropriate meta folders."""
    created = 0
    skipped = 0
    errors = 0
    global processed_count

    for input_item in input_items:
        try:
            lyr = None
            id = input_item[0]
            path = input_item[1]
            name = input_item[2]
            location = input_item[3]
            layer_folder = os.path.join(meta_folder, id[0], id[1:4])
            lyr_mxd = arcpy.mapping.MapDocument(mxd_path)
            dsc = arcpy.Describe(path)

            # Create layer folder if it does not exist.
            if not os.path.exists(layer_folder):
                os.makedirs(layer_folder)

            if not os.path.exists(os.path.join(layer_folder, '{0}.layer.lyr'.format(id))):
                # os.makedirs(layer_folder)
                try:
                    if dsc.dataType in ('FeatureClass', 'Shapefile', 'ShapeFile'):
                        feature_layer = arcpy.MakeFeatureLayer_management(path, os.path.basename(path))
                        lyr = arcpy.SaveToLayerFile_management(feature_layer, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                    elif dsc.dataType == 'RasterDataset':
                        raster_layer = arcpy.MakeRasterLayer_management(path, os.path.splitext(os.path.basename(path))[0])
                        lyr = arcpy.SaveToLayerFile_management(raster_layer, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                    elif dsc.dataType in ('CadDrawingDataset', 'FeatureDataset'):
                        arcpy.env.workspace = path
                        lyr_mxd = arcpy.mapping.MapDocument(mxd_path)
                        data_frame = arcpy.mapping.ListDataFrames(lyr_mxd)[0]
                        group_layer = arcpy.mapping.ListLayers(lyr_mxd, 'Group Layer', data_frame)[0]
                        for fc in arcpy.ListFeatureClasses():
                            dataset_name = os.path.splitext(os.path.basename(path))[0]
                            l = arcpy.MakeFeatureLayer_management(fc, '{0}_{1}'.format(dataset_name, os.path.basename(fc)))
                            arcpy.mapping.AddLayerToGroup(data_frame, group_layer, l.getOutput(0))
                        arcpy.ResetEnvironments()
                        group_layer.saveACopy(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                        lyr = '{0}.layer.lyr'.format(id)
                    elif dsc.catalogPath.lower().endswith('.tab') or dsc.catalogPath.lower().endswith('.mif'):
                        arcpy.ImportToolbox(r"C:\Program Files (x86)\DataEast\TAB Reader\Toolbox\TAB Reader.tbx")
                        lyr = arcpy.GPTabsToArcGis_TR(dsc.catalogPath, False, '', True, True, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
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
                except RuntimeError as re:
                    errors += 1
                    status_writer.send_status(re.message)
                    errors_reasons[name] = re.message
                    continue
                except AssertionError as ae:
                    status_writer.send_status(_('FAIL: {0}. MXD - {1}').format(repr(ae), mxd_path))
            else:
                lyr = os.path.join(layer_folder, '{0}.layer.lyr'.format(id))
            created += 1

            # Update the index.
            if lyr:
                try:
                    update_index(path, lyr, id, name, location, voyager_server, hdrs)
                except (IndexError, ImportError) as ex:
                    status_writer.send_state(status.STAT_FAILED, ex)
                processed_count += 1
                status_writer.send_percent(processed_count / result_count, _('Created: {0}').format('{0}.layer.lyr'.format(id)), 'create_layer_file')
        except IOError as io_err:
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(input_item), 'create_layer_file')
            status_writer.send_status(_('FAIL: {0}').format(repr(io_err)))
            errors_reasons[input_item] = repr(io_err)
            errors += 1
            pass
    return created, errors, skipped
