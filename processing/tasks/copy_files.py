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


def execute(request):
    """Copies files to a target folder.
    :param request: json as a dict.
    """
    copied = 0
    skipped = 0
    errors = 0
    parameters = request['params']

    target_dirs = ''
    target_folder = task_utils.get_parameter_value(parameters, 'target_folder', 'value')
    flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')
    if not flatten_results:
        target_dirs = os.path.splitdrive(target_folder)[1]
        flatten_results = 'false'
    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

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

        status_writer.send_percent(0.0, _('Starting to process...'), 'copy_files')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
            else:
                results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

            input_items = task_utils.get_input_items(eval(results.read())['response']['docs'])
            result = copy_files(input_items, target_folder, flatten_results, target_dirs)
            copied += result[0]
            errors += result[1]
            skipped += result[2]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'copy_files')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'])
        converted, errors, skipped = copy_files(input_items, target_folder, flatten_results, target_dirs, True)

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass
    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '_report.json'), copied, skipped, errors)


def copy_files(input_items, target_folder, flatten_results, target_dirs, show_progress=False):
    """Copy files to target folder."""
    copied = 0
    skipped = 0
    errors = 0
    if show_progress:
        i = 1.
        file_count = len(input_items)
        status_writer.send_percent(0.0, _('Starting to process...'), 'copy_files')

    shp_files = ('shp', 'shx', 'sbn', 'sbx', 'dbf', 'prj', 'cpg', 'shp.xml', 'dbf.xml')
    sdc_files = ('sdc', 'sdi', 'sdc.xml', 'sdc.prj')
    for src_file in input_items:
        try:
            if os.path.isfile(src_file) or src_file.endswith('.gdb'):
                if flatten_results == 'false':
                    # Maintain source file's folder structure.
                    copy_dirs = os.path.splitdrive(os.path.dirname(src_file))[1]
                    if not copy_dirs == target_dirs:
                        dst = target_folder + copy_dirs
                        if not os.path.exists(dst):
                            os.makedirs(dst)
                else:
                    if not os.path.exists(target_folder):
                        dst = target_folder
                        os.makedirs(dst)
                    else:
                        dst = target_folder
                if os.path.isfile(src_file):
                    if src_file.endswith('.shp'):
                        all_files = task_utils.list_files(src_file, shp_files)
                    elif src_file.endswith('.sdc'):
                        all_files = task_utils.list_files(src_file, sdc_files)
                    else:
                        all_files = [src_file]
                    for f in all_files:
                        shutil.copy2(f, dst)
                else:
                    shutil.copytree(src_file, os.path.join(dst, os.path.basename(src_file)))
                if show_progress:
                    status_writer.send_percent(i / file_count, _('Copied: {0}').format(src_file), 'copy_files')
                copied += 1
            else:
                if show_progress:
                    status_writer.send_percent(
                        i / file_count,
                        _('{0} is not a file or does no exist').format(src_file),
                        'copy_files'
                    )
                    i += 1
                else:
                    status_writer.send_status(_('{0} is not a file or does no exist').format(src_file))
                skipped += 1
        except IOError as io_err:
            if show_progress:
                status_writer.send_percent(i / file_count, _('Skipped: {0}').format(src_file), 'copy_files')
                i += 1
            status_writer.send_status(_('FAIL: {0}').format(repr(io_err)))
            errors += 1
            pass
    return copied, errors, skipped
