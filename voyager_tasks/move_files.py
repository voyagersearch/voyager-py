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
import shutil
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def create_dir(src_file, target_folder):
    """Create a directory if it does not exist.

    :param src_file: path of file being archived.
    :param target_folder: path of the archive folder
    """
    if os.path.splitdrive(src_file)[0]:
        copy_dirs = os.path.splitdrive(os.path.dirname(src_file))[1]
    else:
        copy_dirs = os.path.splitunc(src_file)[0] + os.sep + os.path.splitunc(os.path.dirname(src_file))[1]
    if not os.path.exists(target_folder + copy_dirs):
        dst = target_folder + copy_dirs
        os.makedirs(dst)
    else:
        dst = target_folder + copy_dirs
    return dst


def execute(request):
    """Move files to a target folder.
    :param request: json as a dict.
    """
    status_writer = status.Writer()
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    target_folder = task_utils.get_parameter_value(parameters, 'target_folder', 'value')
    flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')

    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    if target_folder:
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

    i = 1.
    moved = 0
    skipped = 0
    errors = 0
    file_count = len(input_items)
    status_writer.send_percent(0.0, _('Starting to process...'), 'move_files')
    for src_file in input_items:
        try:
            if os.path.isfile(src_file) or src_file.endswith('.gdb'):

                try:
                    if not flatten_results:
                        dst = create_dir(src_file, target_folder)
                    else:
                        dst = target_folder
                    shutil.move(src_file, dst)
                except (OSError, WindowsError) as err:
                    status_writer.send_status(_(err))
                    skipped += 1
                    continue
                status_writer.send_percent(i/file_count, _('Archived: {0}').format(src_file), 'move_files')
                moved += 1
            else:
                status_writer.send_percent(
                    i/file_count,
                    _('{0} is not a file or does no exist').format(src_file), 'move_files')
                skipped += 1
        except (IOError, EnvironmentError) as err:
            status_writer.send_percent(
                i/file_count, _('Skipped: {0}').format(src_file), 'move_files')
            status_writer.send_status(_('FAIL: {0}').format(repr(err)))
            errors += 1
            pass
        i += 1

    try:
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '_report.json'), moved, skipped, errors)
