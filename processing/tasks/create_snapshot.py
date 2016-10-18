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
import glob
import shutil
import requests
from utils import status
from utils import task_utils


status_writer = status.Writer()
errors_reasons = {}


def create_snapshot(snapshot_name, owner, query=''):
    """TODO ."""
    try:
        voyager_server = sys.argv[2].split('=')[1].split('solr')[0][:-1]
        if query:
            url = "{0}/api/rest/snapshot/export?name={1}&query={2}&format=CORE".format(voyager_server, snapshot_name, query)
        else:
            url = "{0}/api/rest/snapshot/export?name={1}&format=CORE".format(voyager_server, snapshot_name)
        response = requests.post(url, headers={'Content-type': 'application/json', 'x-access-token': task_utils.get_security_token(owner)})
        if response.status_code == 200:
            return True, 'Created snapshot: {0}'.format(response.json()['target']['file'])
        else:
            return False, 'Error creating snapshot: {0}: {1}'.format(snapshot_name, 'Error {0}: {1}'.format(response.status_code, response.reason))
    except requests.HTTPError as http_error:
        return False, http_error
    except requests.exceptions.InvalidURL as url_error:
        return False, url_error
    except requests.RequestException as re:
        return False, re


def execute(request):
    """Remove tags.
    :param request: json as a dict.
    """
    query = ''
    errors = 0
    parameters = request['params']
    archive_location = request['folder']
    if not os.path.exists(archive_location):
        os.makedirs(archive_location)

    # Parameter values
    snapshot_name = task_utils.get_parameter_value(parameters, 'snapshot_name', 'value')
    data_folder = task_utils.get_parameter_value(parameters, 'data_folder', 'value')
    request_owner = request['owner']

    result_count, response_index = task_utils.get_result_count(parameters)
    # Get the query index and query (if any).
    # query_index = task_utils.QueryIndex(parameters[response_index])
    fq = ''
    if 'fq' in parameters[response_index]['query']:
        if isinstance(parameters[response_index]['query']['fq'], list):
            for q in parameters[response_index]['query']['fq']:
                if '{!tag=' in q:
                    q = q.split('}')[1]
                fq += q + ' AND '
            fq = fq.strip(' AND ')
        else:
            # Replace spaces with %20 & remove \\ to avoid HTTP Error 400.
            fq += '&fq={0}'.format(parameters[response_index]['query']['fq'].replace("\\", ""))
            fq = fq.replace(' ', '%20')

    # fq = query_index.get_fq()
    if fq:
        query = fq.replace('&fq=', '')
    if query:
        result = create_snapshot(snapshot_name, request_owner, query)
    else:
        result = create_snapshot(snapshot_name, request_owner)
    if not result[0]:
        errors += 1
        errors_reasons[snapshot_name] = result[1]

    # Update state if necessary.
    if errors > 0:
        status_writer.send_state(status.STAT_FAILED)
    else:
        status_writer.send_status(result[1])
        result_name = ''
        while not result_name:
            file_path = os.path.join(data_folder, 'backup', '*{0}.zip'.format(snapshot_name))
            try:
                result_name = os.path.basename(glob.glob(file_path)[0])
            except IndexError:
                continue
        shutil.copyfile(os.path.join(data_folder, 'backup', result_name), os.path.join(archive_location, result_name))
    task_utils.report(os.path.join(request['folder'], '__report.json'), 1, 0, errors, errors_details=errors_reasons)
