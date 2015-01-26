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
from utils import status
from utils import task_utils
from tasks import _


status_writer = status.Writer()
status_writer.send_status(_('Initializing...'))
import arcpy


def execute(request):
    """Replace the workspace path for layer files and map document layers.
    :param request: json as a dict.
    """
    updated = 0
    skipped = 0
    parameters = request['params']
    backup = task_utils.get_parameter_value(parameters, 'create_backup', 'value')
    old_workspace = task_utils.get_parameter_value(parameters, 'old_workspace_path', 'value').lower()
    new_workspace = task_utils.get_parameter_value(parameters, 'new_workspace_path', 'value')

    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    if not arcpy.Exists(new_workspace):
        status_writer.send_state(status.STAT_FAILED, _('{0} does not exist').format(new_workspace))
        return

    num_results = parameters[0]['response']['numFound']
    if num_results > task_utils.CHUNK_SIZE:
        # Query the index for results in groups of 25.
        query_index = task_utils.QueryIndex(parameters[0])
        fl = query_index.fl
        query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
        fq = query_index.get_fq()
        if fq:
            groups = task_utils.grouper(range(0, num_results), task_utils.CHUNK_SIZE, '')
            query += fq
        else:
            groups = task_utils.grouper(list(parameters[0]['ids']), task_utils.CHUNK_SIZE, '')

        status_writer.send_percent(0.0, _('Starting to process...'), 'replace_workspace_path')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
            else:
                results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

            input_items = task_utils.get_input_items(eval(results.read())['response']['docs'])
            result = replace_workspace_path(input_items, old_workspace, new_workspace, backup)
            updated += result[0]
            skipped += result[1]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'replace_workspace_path')
    else:
        input_items = task_utils.get_input_items(parameters[0]['response']['docs'])
        updated, skipped = replace_workspace_path(input_items, old_workspace, new_workspace, backup, True)

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass
    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '_report.json'), updated, skipped)


def replace_workspace_path(input_items, old_workspace, new_workspace, backup, show_progress=False):
    """Replace workspace path."""
    updated = 0
    skipped = 0
    if show_progress:
        i = 1.
        count = len(input_items)

    for item in input_items:
        layers = None
        table_views = None
        mxd = None
        if item.endswith('.lyr') or item.endswith('.mxd'):
            if backup:
                try:
                    shutil.copyfile(item, '{0}.bak'.format(item))
                except IOError:
                    status_writer.send_status(_('Cannot make a backup of: {0}').format(item))
                    skipped += 1
                    continue
            if item.endswith('.lyr'):
                layer_from_file = arcpy.mapping.Layer(item)
                layers = arcpy.mapping.ListLayers(layer_from_file)
            else:
                mxd = arcpy.mapping.MapDocument(item)
                layers = arcpy.mapping.ListLayers(mxd)
                table_views = arcpy.mapping.ListTableViews(mxd)
        else:
            status_writer.send_status(_('{0} is not a layer file or map document').format(item))
            skipped += 1
            continue

        if layers:
            for layer in layers:
                try:
                    layer.findAndReplaceWorkspacePath(old_workspace, new_workspace, validate=False)
                except ValueError:
                    status_writer.send_status(_('Invalid workspace'))
                    skipped += 1
                    pass
            if item.endswith('.lyr'):
                layer_from_file.save()

        if table_views:
            for table_view in table_views:
                try:
                    table_view.findAndReplaceWorkspacePath(old_workspace, new_workspace, validate=False)
                except ValueError:
                    status_writer.send_status(_('Invalid workspace'))
                    skipped += 1
                    pass
        if mxd:
            mxd.save()
        if show_progress:
            status_writer.send_percent(i / count, _('Updated: {0}').format(item), 'replace_workspace_path')
            i += 1.
        else:
            status_writer.send_status(_('Updated: {0}').format(item))
        updated += 1
    return updated, skipped
