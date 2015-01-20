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
import zipfile
from utils import status
from utils import task_utils
from tasks import _


def execute(request):
    """Zips all input files to output.zip.
    :param request: json as a dict.
    """
    zipped = 0
    skipped = 0
    shp_files = ('shp', 'shx', 'sbn', 'dbf', 'prj', 'cpg', 'shp.xml', 'dbf.xml')
    sdc_files = ('sdc', 'sdi', 'sdc.xml', 'sdc.prj')
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')
    if not flatten_results:
        flatten_results = False
    zip_file_location = request['folder']
    if not os.path.exists(zip_file_location):
        os.makedirs(request['folder'])
    file_count = len(input_items)
    i = 1.
    status_writer = status.Writer()
    zip_file = os.path.join(zip_file_location, 'output.zip')
    with task_utils.ZipFileManager(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipper:
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
                status_writer.send_percent(i/file_count, _('Zipped: {0}').format(in_file), 'zip_files')
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
                status_writer.send_percent(
                    i/file_count,
                    _('{0} is not a file or does not exist').format(in_file),
                    'zip_files'
                )
                skipped += 1
            i += 1.0

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Update state if necessary.
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped))
    task_utils.report(os.path.join(request['folder'], '_report.json'), zipped, skipped)