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
import urllib2
import arcpy
from utils import status
from utils import task_utils


status_writer = status.Writer()
result_count = 0
processed_count = 0.
skipped_reasons = {}
errors_reasons = {}
arcpy.env.overwriteOutput = True


def index_item(id):
    """Re-indexes an item.
    :param id: Item's index ID
    """
    solr_url = "{0}/flags?op=add&flag=__to_extract&fq=id:({1})&fl=*,[true]".format(sys.argv[2].split('=')[1], id)
    request = urllib2.Request(solr_url)
    urllib2.urlopen(request)


def execute(request):
    """Adds a field and calculates it to some value.
    :param request: json as a dict.
    """
    created = 0
    skipped = 0
    errors = 0
    global result_count
    parameters = request['params']

    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    # Parameter values
    field_name = task_utils.get_parameter_value(parameters, 'field_name', 'value')
    field_type = task_utils.get_parameter_value(parameters, 'field_type', 'value')
    field_value = task_utils.get_parameter_value(parameters, 'field_value', 'value')

    # Query the index for results in groups of 25.
    result_count, response_index = task_utils.get_result_count(parameters)
    query_index = task_utils.QueryIndex(parameters[response_index])
    fl = query_index.fl + ',links'
    # query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
    query = '{0}{1}{2}'.format("http://localhost:8888/solr/v0", '/select?&wt=json', fl)
    fq = query_index.get_fq()
    if fq:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')
        query += fq
    else:
        groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')

    status_writer.send_percent(0.0, _('Starting to process...'), 'add_field')
    i = 0.
    for group in groups:
        i += len(group) - group.count('')
        if fq:
            results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
        else:
            results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

        docs = eval(results.read().replace('false', 'False').replace('true', 'True'))['response']['docs']
        input_items = []
        for doc in docs:
            if 'path' in doc:
                if 'links' in doc:
                    links = eval(doc['links'])
                    input_items.append((doc['id'], doc['path'], links['links'][0]['link'][0]['id']))
                else:
                    input_items.append((doc['id'], doc['path']))
        # input_items = task_utils.get_input_items(docs, True)
        result = add_field(input_items, field_name, field_type, field_value)
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
    task_utils.report(os.path.join(request['folder'], 'report.json'), created, skipped, errors, errors_reasons, skipped_reasons)


def add_field(input_items, field_name, field_type, field_value):
    """Creates a layer for input items in the appropriate meta folders."""
    updated = 0
    skipped = 0
    errors = 0
    global processed_count

    for input_item in input_items:
        try:
            id = input_item[0]
            path = input_item[1]
            if len(input_item) == 3:
                id = input_item[2]
            dsc = arcpy.Describe(path)
            try:
                if dsc.dataType in ('FeatureClass', 'Shapefile', 'ShapeFile', 'Table'):
                    field_name = arcpy.ValidateFieldName(field_name, task_utils.get_geodatabase_path(path))
                    arcpy.AddField_management(path, field_name, field_type)
                    try:
                        arcpy.CalculateField_management(path, field_name, "'{0}'".format(field_value), expression_type='PYTHON')
                    except UnicodeEncodeError:
                        arcpy.CalculateField_management(path, field_name, "'{0}'".format(field_value.encode('utf-8')), expression_type='PYTHON')
                else:
                    skipped +=1
                    file_ext = os.path.splitext(dsc.path)[1]
                    if file_ext in ('.sdc', '.dxf', '.dwg', '.dgn'):
                        status_writer.send_status(_('Format is not editable') + dsc.name)
                        skipped_reasons[dsc.name] = _('Format is not editable')
                    else:
                        status_writer.send_status(_('Invalid input type: {0}').format(dsc.name))
                        skipped_reasons[dsc.name] = _('Invalid input type: {0}').format(dsc.dataType)
                    continue
            except arcpy.ExecuteError:
                errors += 1
                status_writer.send_status(arcpy.GetMessages(2))
                errors_reasons[dsc.name] = arcpy.GetMessages(2)
                continue
            updated += 1

            # Update the index.
            try:
                index_item(id)
            except (IndexError, urllib2.HTTPError, urllib2.URLError) as e:
                status_writer.send_status(e.message)
                pass
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Added field: {0} to {1}').format(field_name, path), 'add_field')

        except IOError as io_err:
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(path), 'add_field')
            status_writer.send_status(_('FAIL: {0}').format(repr(io_err)))
            errors_reasons[input_item] = repr(io_err)
            errors += 1
            pass
    return updated, errors, skipped
