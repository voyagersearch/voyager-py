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
    moved = 0
    skipped = 0
    errors = 0
    parameters = request['params']
    target_folder = task_utils.get_parameter_value(parameters, 'target_folder', 'value')
    flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')
    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])
    if target_folder:
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

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

        status_writer.send_percent(0.0, _('Starting to process...'), 'move_files')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
            else:
                results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

            input_items = task_utils.get_input_items(eval(results.read())['response']['docs'])
            result = move_files(input_items, target_folder, flatten_results)
            moved += result[0]
            errors += result[1]
            skipped += result[2]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'move_files')
    else:
        input_items = task_utils.get_input_items(parameters[0]['response']['docs'])
        moved, errors, skipped = move_files(input_items, target_folder, flatten_results, True)

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass
    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '_report.json'), moved, skipped, errors)


def move_files(input_items, target_folder, flatten_results, show_progress=False):
    """Moves files."""
    moved = 0
    skipped = 0
    errors = 0
    if show_progress:
        i = 1.
        file_count = len(input_items)
        status_writer.send_percent(0.0, _('Starting to process...'), 'copy_files')

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
                if show_progress:
                    status_writer.send_percent(i / file_count, _('Archived: {0}').format(src_file), 'move_files')
                    i += 1
                moved += 1
            else:
                if show_progress:
                    status_writer.send_percent(
                        i / file_count,
                        _('{0} is not a file or does no exist').format(src_file), 'move_files')
                    i += 1
                skipped += 1
        except (IOError, EnvironmentError) as err:
            if show_progress:
                status_writer.send_percent(
                    i/file_count, _('Skipped: {0}').format(src_file), 'move_files')
                i += 1
            status_writer.send_status(_('FAIL: {0}').format(repr(err)))
            errors += 1
            pass
    return moved, errors, skipped
