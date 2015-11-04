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
import zipfile
from utils import status
from utils import task_utils
import requests


status_writer = status.Writer()
shp_files = ('shp', 'shx', 'sbn', 'dbf', 'prj', 'cpg', 'shp.xml', 'dbf.xml')
sdc_files = ('sdc', 'sdi', 'sdc.xml', 'sdc.prj')
skipped_reasons = {}


def execute(request):
    """Zips all input files to output.zip.
    :param request: json as a dict.
    """
    zipped = 0
    skipped = 0
    parameters = request['params']
    flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')
    if not flatten_results:
        flatten_results = False
    zip_file_location = request['folder']
    if not os.path.exists(zip_file_location):
        os.makedirs(request['folder'])

    zip_file = os.path.join(zip_file_location, 'output.zip')
    zipper = task_utils.ZipFileManager(zip_file, 'w', zipfile.ZIP_DEFLATED)
    num_results, response_index = task_utils.get_result_count(parameters)
    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    if num_results > task_utils.CHUNK_SIZE:
        # Query the index for results in groups of 25.
        query_index = task_utils.QueryIndex(parameters[response_index])
        fl = query_index.fl
        query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
        fq = query_index.get_fq()
        if fq:
            groups = task_utils.grouper(range(0, num_results), task_utils.CHUNK_SIZE, '')
            query += fq
        elif 'ids' in parameters[response_index]:
            groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')
        else:
            groups = task_utils.grouper(range(0, num_results), task_utils.CHUNK_SIZE, '')

        status_writer.send_percent(0.0, _('Starting to process...'), 'zip_files')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
            elif 'ids' in parameters[response_index]:
                results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
            else:
                results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

            input_items = task_utils.get_input_items(results.json()['response']['docs'], list_components=True)
            if not input_items:
                input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'], list_components=True)
            result = zip_files(zipper, input_items, zip_file_location, flatten_results)
            zipped += result[0]
            skipped += result[1]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'zip_files')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'], list_components=True)
        zipped, skipped = zip_files(zipper, input_items, zip_file_location, flatten_results, True)

    zipper.close()

    if zipped == 0:
        status_writer.send_state(status.STAT_FAILED, _('No results were zipped.'))
        return

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '__report.json'), zipped, skipped, skipped_details=skipped_reasons)


def zip_files(zipper, input_items, zip_file_location, flatten_results, show_progress=False):
    """Zips files."""
    zipped = 0
    skipped = 0
    if show_progress:
        file_count = len(input_items)
        i = 1.

    for in_file in input_items:
        if os.path.isfile(in_file):
            if in_file.endswith('.shp'):
                files = task_utils.list_files(in_file, shp_files)
            elif in_file.endswith('.sdc'):
                files = task_utils.list_files(in_file, sdc_files)
            else:
                files = [in_file]
            if flatten_results:
                for f in files:
                    zipper.write(f, os.path.basename(f))
            else:
                for f in files:
                    zipper.write(f, os.path.join(os.path.abspath(os.path.join(f, os.pardir)), os.path.basename(f)))
            if show_progress:
                status_writer.send_percent(i / file_count, _('Zipped: {0}').format(in_file), 'zip_files')
                i += 1
            zipped += 1
        elif in_file.endswith('.gdb'):
            for root, dirs, files in os.walk(in_file):
                for f in files:
                    if not f.endswith('zip'):
                        absf = os.path.join(root, f)
                        zf = absf[len(in_file) + len(os.sep):]
                        try:
                            if flatten_results:
                                zipper.write(absf, os.path.join(os.path.basename(in_file), zf))
                            else:
                                zipper.write(absf, os.path.join(in_file, zf))
                        except IOError:
                            # For File GDB lock files.
                            pass
        else:
            status_writer.send_status(_('{0} is not a file or does not exist').format(in_file))
            if show_progress:
                status_writer.send_percent(
                    i / file_count,
                    _('{0} is not a file or does not exist').format(in_file),
                    'zip_files'
                )
                i += 1
            skipped_reasons[in_file] = _('is not a file or does not exist')
            skipped += 1
    return zipped, skipped
