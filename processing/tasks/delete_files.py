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

status_writer = status.Writer()
skipped_reasons = {}
errors_reasons = {}


def remove_from_index(id):
    """Remove the item from the index."""
    solr_url = "{0}/update?stream.body=<delete><id>{1}</id></delete>&commit=true".format(sys.argv[2].split('=')[1], id)
    request = urllib2.Request(solr_url, headers={'Content-type': 'application/json'})
    urllib2.urlopen(request)


def execute(request):
    """Deletes files.
    :param request: json as a dict.
    """
    deleted = 0
    skipped = 0

    parameters = request['params']
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

        status_writer.send_percent(0.0, _('Starting to process...'), 'delete_files')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
            else:
                results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

            input_items = task_utils.get_input_items(eval(results.read().replace('false', 'False').replace('true', 'True'))['response']['docs'], True, True)
            result = delete_files(input_items)
            deleted += result[0]
            skipped += result[1]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'delete_files')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'], True, True)
        deleted, skipped = delete_files(input_items, True)

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], 'report.json'), deleted, skipped, skipped_details=skipped_reasons)


def delete_files(input_items, show_progress=False):
    """Delete files."""
    deleted = 0
    skipped = 0
    if show_progress:
        i = 1.
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
                if show_progress:
                    status_writer.send_percent(i / file_count, _('Deleted: {0}').format(src_file), 'delete_files')
                    i += 1
                # Remove item from the index.
                try:
                    remove_from_index(input_items[src_file][1])
                except (IndexError, urllib2.HTTPError, urllib2.URLError):
                    pass
                deleted += 1
            else:
                if show_progress:
                    status_writer.send_percent(i / file_count,
                                               _('{0} is not a file or does no exist').format(src_file),
                                               'delete_files')
                    i += 1
                else:
                    status_writer.send_status(_('{0} is not a file or does no exist').format(src_file))
                skipped_reasons[src_file] = _('{0} is not a file or does no exist').format(src_file)
                skipped += 1
        except (IOError, EnvironmentError) as err:
            if show_progress:
                status_writer.send_percent(i / file_count, _('Skipped: {0}').format(src_file), repr(err))
                i += 1
            else:
                status_writer.send_status(_('Skipped: {0}').format(src_file), repr(err))
            skipped_reasons[src_file] = repr(err)
            skipped += 1
            pass
    return deleted, skipped
