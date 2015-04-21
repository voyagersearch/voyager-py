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
import tasks
from tasks import _
import arcpy


status_writer = status.Writer()
arcpy.ImportToolbox(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'clearterra', 'LocateXT.tbx'))


def execute(request):
    """Copies files to a target folder.
    :param request: json as a dict.
    """
    extracted = 0
    skipped = 0
    errors = 0
    parameters = request['params']

    output_type = task_utils.get_parameter_value(parameters, 'output_format', 'value')
    task_folder = os.path.join(request['folder'], 'temp')
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)
        if output_type == 'FGDB':
            arcpy.CreateFileGDB_management(task_folder, 'output.gdb')

    num_results, response_index = task_utils.get_result_count(parameters)
    if num_results > task_utils.CHUNK_SIZE:
        # Query the index for results in groups of 25.
        query_index = task_utils.QueryIndex(parameters[response_index])
        fl = query_index.fl
        query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
        fq = query_index.get_fq()
        if fq:
            groups = task_utils.grouper(range(0, num_results), task_utils.CHUNK_SIZE, '')
            query += fq
        else:
            groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')

        status_writer.send_percent(0.0, _('Starting to process...'), 'locate_xt_tool')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
            else:
                results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

            input_items = task_utils.get_input_items(eval(results.read())['response']['docs'])
            result = extract(input_items, output_type, task_folder)
            extracted += result[0]
            errors += result[1]
            skipped += result[2]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'locate_xt_tool')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
        converted, errors, skipped = extract(input_items, output_type, task_folder, True)

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), task_folder)
    except IOError:
        pass

    # Zip up outputs.
    zip_file = task_utils.zip_data(task_folder, 'output.zip')
    shutil.move(zip_file, os.path.join(os.path.dirname(task_folder), os.path.basename(zip_file)))

    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '_report.json'), extracted, skipped, errors)


def extract(input_items, out_type, output_dir, show_progress=False):
    """Extract geographic information from input items."""
    extracted = 0
    skipped = 0
    errors = 0
    if show_progress:
        i = 1.
        file_count = len(input_items)
        status_writer.send_percent(0.0, _('Starting to process...'), 'locate_xt')


    # Get the LocateXT GP Toolbox
    for src_file in input_items:
        try:
            if os.path.isfile(src_file):
                #TODO: ADD GP TOOL SUPPORT HERE
                file_name = arcpy.ValidateTableName(os.path.basename(os.path.splitext(src_file)[0]))
                if out_type == 'CSV':
                    # arcpy.LocateXTServerTool(src_file, os.path.join(output_dir, '{0}.csv'.format(file_name)))
                    arcpy.Command("LocateXTServerTool {0} {1}".format(src_file, os.path.join(output_dir, '{0}.csv'.format(file_name))))
                elif out_type == 'KML':
                    arcpy.LocateXTServerTool(src_file, os.path.join(output_dir, '{0}.kml'.format(file_name)))
                elif out_type == 'SHP':
                    # arcpy.LocateXTServerTool(src_file, os.path.join(output_dir, '{0}.shp'.format(file_name)))
                    arcpy.Command("LocateXTServerTool {0} {1}".format(src_file, os.path.join(output_dir, '{0}.shp'.format(file_name))))
                elif out_type == 'FGDB':
                    arcpy.Command("LocateXTServerTool {0} {1}".format(src_file, os.path.join(output_dir, 'output.gdb', file_name)))
                    # arcpy.LocateXTServerTool(src_file, os.path.join(output_dir, 'output.gdb', file_name))

                if show_progress:
                    status_writer.send_percent(i / file_count, _('Extracted: {0}').format(src_file), 'locate_xt_tool')
                extracted += 1
            else:
                if show_progress:
                    status_writer.send_percent(
                        i / file_count,
                        _('{0} is not a supported file type or does no exist').format(src_file),
                        'locate_xt'
                    )
                    i += 1
                else:
                    status_writer.send_status(_('{0} is not a supported file type or does no exist').format(src_file))
                skipped += 1
        except IOError as io_err:
            if show_progress:
                status_writer.send_percent(i / file_count, _('Skipped: {0}').format(src_file), 'locate_xt_tool')
                i += 1
            status_writer.send_status(_('FAIL: {0}').format(repr(io_err)))
            errors += 1
            pass
        except arcpy.ExecuteError:
            status_writer.send_status(_('FAIL: {0}').format(arcpy.GetMessages()))
            errors += 1
            pass
    return extracted, errors, skipped
