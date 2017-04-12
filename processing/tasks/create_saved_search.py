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


def find_between( s, first, last ):
    """Find a string between two characters."""
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""


def create_saved_search(search_name, groups, owner, query, has_q):
    """Create the saved search using Voyager API."""
    try:
        voyager_server = sys.argv[2].split('=')[1].split('solr')[0][:-1]
        url = "{0}/api/rest/display/ssearch".format(voyager_server)
        if query:
            if has_q:
                path = "/q=" + query
            else:
                path ="/" + query
            query = {
                "title": str(search_name),
                "owner": str(owner['name']),
                "path": str(path),
                "share": groups,
                "overwrite": True
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
            if hasattr(response, 'content'):
                return False, eval(response.content)['error']
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
    fq = '/'
    if 'fq' in parameters[response_index]['query']:
        if isinstance(parameters[response_index]['query']['fq'], list):
            for q in parameters[response_index]['query']['fq']:
                if '{!tag=' in q:
                    q = q.split('}')[1]
                if ':' in q:
                    facet = q.split(':')[0]
                    value = q.split(':')[1]
                    if '(' in value:
                        value = value.replace('(', '').replace(')', '')
                    value = urllib.urlencode({'val': value.replace('"', '')})
                    value = value.split('val=')[1]
                    facet2 = 'f.{0}='.format(facet)
                    q = '{0}{1}'.format(facet2, value) #q.replace(facet + ':', facet2)
                fq += '{0}/'.format(q).replace('"', '')
        else:
            # Replace spaces with %20 & remove \\ to avoid HTTP Error 400.
            fq += '&fq={0}'.format(parameters[response_index]['query']['fq'].replace("\\", ""))
            if '{!tag=' in fq:
                fq = fq.split('}')[1]
            if ':' in fq:
                if fq.startswith('/&fq='):
                    fq = fq.replace('/&fq=', '')
                facet = fq.split(':')[0]
                value = fq.split(':')[1].replace('(', '').replace(')', '').replace('"', '')
                if 'place' not in facet:
                    value = urllib.urlencode({'val': value}).split('val=')[1]
                facet2 = 'f.{0}='.format(facet)
                if '(' in value:
                    fq = ''
                    if value.split(' '):
                        for v in  value.split(' '):
                            fq += (facet2 + v.replace('(', '').replace(')', '') + '/').replace(':', '')
                else:
                    value = urllib.urlencode({'val': value}).split('val=')[1]
                    fq = '{0}{1}'.format(facet2, value)
            if '{! place.op=' in fq:
                relop = find_between(fq, 'place.op=', '}')
                fq = fq.replace('}', '').replace('{', '')
                fq = fq.replace('! place.op={0}'.format(relop), '/place.op={0}/'.format(relop))
                fq = fq.replace('place:', 'place=')
                fq = fq.replace('&fq=', '')

    hasQ = False
    if 'q' in parameters[response_index]['query']:
        query = parameters[response_index]['query']['q']
        hasQ = True
        if fq:
            query += '/'

    if fq:
        if fq.startswith('/place'):
            query += fq.replace('"', '')
        elif '!tag' in query and 'OR' in query:
            # e.g. "path": "/q=id:(92cdd06e01761c4c d9841b2f59b8a326) OR format:(application%2Fvnd.esri.shapefile)"
            q = query.split('}')[1].replace('))/', '').replace('(', '').replace('(', '')
            q = urllib.urlencode({'val': q.split(':')[1]}).split('val=')[1]
            query = query.split(' OR ')[0] + ' OR ' + q
        else:
            if fq.startswith('f.//'):
                fq = fq.replace('f.//', '/').replace('"', '')
            if ' place.id' in fq:
                fq = fq.replace(' place.id', '/place.id').replace('"', '')
            if '{! place.op=' in fq:
                relop = find_between(fq, 'place.op=', '}')
                fq = fq.replace('}', '').replace('{', '')
                fq = fq.replace('! place.op={0}'.format(relop), '/place.op={0}/'.format(relop)).replace('"', '')
            query += fq.rstrip('/')
            query = query.replace('f./', '')
        query = query.replace('&fq=', '')

    if query:
        result = create_saved_search(search_name, groups, request_owner, query, hasQ)
    else:
        result = create_saved_search(search_name, groups, request_owner, "", hasQ)
    if not result[0]:
        errors += 1
        errors_reasons[search_name] = result[1]

    # Update state if necessary.
    if errors > 0:
        status_writer.send_state(status.STAT_FAILED, result[1])
    else:
        status_writer.send_status(result[1])
    task_utils.report(os.path.join(request['folder'], '__report.json'), 1, 0, errors, errors_details=errors_reasons)
