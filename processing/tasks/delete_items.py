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
import requests
from requests.utils import requote_uri
from utils import status
from utils import task_utils
import warnings
# from requests.packages.urllib3.exceptions import InsecureRequestWarning
# warnings.simplefilter(' ignore', InsecureRequestWarning)


# Get SSL trust setting.
verify_ssl = task_utils.get_ssl_mode()

status_writer = status.Writer()
errors_reasons = {}


def delete_items(fq_query, q_query, thumbs, metadata, layers, owner):
    """Delete items from the index using the Voyager API."""
    try:
        voyager_server = sys.argv[2].split('=')[1].split('solr')[0][:-1]
        # voyager_server = "http://localhost:8888"
        if fq_query:
            if '{!tag=' in fq_query:
                fq_query = "&fq=" + fq_query.split('}')[1]
            if ' AND ' in fq_query:
                fq_query = fq_query.replace(' AND ', '&fq=')
            query = fq_query
            fq = ""
            if q_query:
                query = q_query + query
        else:
            query = q_query
            if fq_query:
                fq = fq_query
            else:
                fq = ''

        url = "{0}/api/rest/index/records?query={1}{2}&items=true&thumbnails={3}&metadata={4}&layers={5}".format(voyager_server, query, fq, thumbs, metadata, layers)
        url = requote_uri(url.replace(",", "%20C"))
        status_writer.send_status(url)
        response = requests.delete(url, verify=verify_ssl, headers={'Content-type': 'application/json', 'x-access-token': task_utils.get_security_token(owner)})
        if response.status_code == 200:
            return True, 'Deleted items: {0}'.format(response.json())
        else:
            return False, 'Error deleting items: {0}: {1}'.format('delete_items', response.reason)
    except requests.HTTPError as http_error:
        return False, http_error
    except requests.exceptions.InvalidURL as url_error:
        return False, url_error
    except requests.RequestException as re:
        return False, re


def execute(request):
    """Delete items.
    :param request: json as a dict.
    """
    query = ''
    errors = 0
    parameters = request['params']
    archive_location = request['folder']
    if not os.path.exists(archive_location):
        os.makedirs(archive_location)

    # Parameter values
    delete_thumbs = task_utils.get_parameter_value(parameters, 'delete_thumbnails', 'value') or False
    delete_metadata = task_utils.get_parameter_value(parameters, 'delete_metadata', 'value') or False
    delete_layers = task_utils.get_parameter_value(parameters, 'delete_layers', 'value') or False
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
            fq = parameters[response_index]['query']['fq'].replace("\\", "")

    if 'q' in parameters[response_index]['query']:
        query = parameters[response_index]['query']['q']
    if fq:
        fq = "&fq=" + fq
    if query:
        query = query

    result = delete_items(fq, query, delete_thumbs, delete_metadata, delete_layers, request_owner)

    if not result[0]:
        errors += 1
        errors_reasons['delete_items'] = result[1]

    # Update state if necessary.
    if errors > 0:
        status_writer.send_state(status.STAT_FAILED)
    else:
        status_writer.send_status(result[1])
    task_utils.report(os.path.join(request['folder'], '__report.json'), 1, 0, errors, errors_details=errors_reasons)
