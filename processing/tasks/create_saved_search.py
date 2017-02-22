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
import json
import requests
import urllib
from utils import status
from utils import task_utils


status_writer = status.Writer()
errors_reasons = {}


def create_saved_search(search_name, groups, owner, query, query_type='fq'):
    """Create the saved search using Voyager API."""
    try:
        voyager_server = sys.argv[2].split('=')[1].split('solr')[0][:-1]
        url = "{0}/api/rest/display/ssearch".format(voyager_server)
        if query:
            if query_type == 'fq':
                path = "/f.format=" + urllib.urlencode({"path": query}).split('=')[1]
            else:
                path = "/q=" + query
            query = {
                "title": search_name,
                "owner": owner['name'],
                "path": path,
                "share": groups
            }
        else:
            query = {
                "title": search_name,
                "owner": owner['name'],
                "path": "",
                "share": groups
            }
        response = requests.post(url, json.dumps(query), headers={'Content-type': 'application/json', 'x-access-token': task_utils.get_security_token(owner)})
        if response.status_code == 200:
            return True, 'Created save search: {0}'.format(response.json()['title'])
        else:
            return False, 'Error creating saved search: {0}: {1}'.format(search_name, response.reason)
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
    search_name = task_utils.get_parameter_value(parameters, 'search_name', 'value')
    groups = task_utils.get_parameter_value(parameters, 'groups', 'value')
    request_owner = request['owner']

    result_count, response_index = task_utils.get_result_count(parameters)
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

    if fq:
        if 'format:' in fq:
            fq = fq.replace('format:', '')
        query = fq.replace('&fq=', '')
        qtype = 'fq'

    if 'q' in parameters[response_index]['query']:
        query = parameters[response_index]['query']['q']
        qtype = 'q'

    if query:
        result = create_saved_search(search_name, groups, request_owner, query, qtype)
    else:
        result = create_saved_search(search_name, groups, request_owner, "")
    if not result[0]:
        errors += 1
        errors_reasons[search_name] = result[1]

    # Update state if necessary.
    if errors > 0:
        status_writer.send_state(status.STAT_FAILED)
    else:
        status_writer.send_status(result[1])
    task_utils.report(os.path.join(request['folder'], '__report.json'), 1, 0, errors, errors_details=errors_reasons)
