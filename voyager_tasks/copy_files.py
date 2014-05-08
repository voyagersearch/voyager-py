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
#from __future__ import unicode_literals
import os
import shutil
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def execute(request):
    """Copies files to a target folder.
    :param request: json as a dict.
    """
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    target_folder = task_utils.get_parameter_value(parameters, 'target_folder', 'value')
    flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')
    if not flatten_results:
        target_dirs = os.path.splitdrive(target_folder)[1]
        flatten_results = 'false'
    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    i = 1.
    copied = 0
    skipped = 0
    errors = 0
    file_count = len(input_items)
    shp_files = ('shp', 'shx', 'sbn', 'dbf', 'prj', 'cpg', 'shp.xml', 'dbf.xml')
    sdc_files = ('sdc', 'sdi', 'sdc.xml', 'sdc.prj')
    status_writer = status.Writer()

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
                status_writer.send_percent(i/file_count, _('SUCCESS'), 'copy_files')
                copied += 1
            else:
                status_writer.send_percent(
                    i/file_count,
                    _('{0} is not a file or does no exist').format(src_file),
                    'copy_files'
                )
                skipped += 1
        except IOError as io_err:
            status_writer.send_percent(
                i/file_count, _('FAIL: {0}').format(repr(io_err)), 'copy_files')
            errors += 1
            pass

    try:
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '_report.json'), copied, skipped, errors)
