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
from voyager_tasks import _


def execute(request):
    """Deletes files.
    :param request: json as a dict.
    """
    status_writer = status.Writer()
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)

    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    i = 1.
    deleted = 0
    skipped = 0
    errors = 0
    file_count = len(input_items)
    status_writer.send_percent(0.0, _('Starting to process...'), 'delete_files')
    for src_file in input_items:
        try:
            if os.path.isfile(src_file) or src_file.endswith('.gdb'):
                try:
                    os.remove(src_file)
                except (OSError, WindowsError) as err:
                    status_writer.send_status(_(err))
                    skipped += 1
                    continue
                status_writer.send_percent(i/file_count, _('Deleted: {0}').format(src_file), 'delete_files')
                deleted += 1
            else:
                status_writer.send_percent(
                    i/file_count,
                    _('{0} is not a file or does no exist').format(src_file), 'delete_files')
                skipped += 1
        except (IOError, EnvironmentError) as err:
            status_writer.send_percent(
                i/file_count, _('Skipped: {0}').format(src_file), 'delete_files')
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
    task_utils.report(os.path.join(request['folder'], '_report.json'), deleted, skipped, errors)
