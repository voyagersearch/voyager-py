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
import argparse
import json
import urllib2
import os


def send_job(file_location, url, username, password, extract):
    """Posts a discovery job to Voyager for indexing.
    :param file_location: the location of the file to be indexed.
    :param url: request url i.e. http://localhost:8888/api/rest/discovery/job/index/
    """
    # Sample discovery job.
    data = {"path": file_location,
            "action": "ADD",
            "entry": {"fields": {"__to_extract":extract, "name": os.path.basename(file_location)}}}

    # Build the request and post.
    try:
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, url, username, password)
        urllib2.install_opener(urllib2.build_opener(urllib2.HTTPBasicAuthHandler(passman)))
        request = urllib2.Request(url, json.dumps(data), headers={'Content-type': 'application/json'})
        response = urllib2.urlopen(request)
        if response.code == 200:
            print('Sent {0} for indexing...'.format(file_location))
        else:
            print ('Error sending {0}: {1}'.format(file_location, response.code))
    except urllib2.HTTPError as http_error:
        print(http_error)
    except urllib2.URLError as url_error:
        print(url_error)
# End send_job function


if __name__ == '__main__':
    # Set up arguments - can be run standalone or via the command line.
    argument_parser = argparse.ArgumentParser(description='Sample script to index a list of files.')
    argument_parser.add_argument('-f', '--file_list', action='store',
                                 help='File containing a list of items to index',
                                 default='c:/voyager/files_to_index.txt')
    argument_parser.add_argument('-u', '--request_url', action='store',
                                 help='The request URL',
                                 default='http://localhost:8888/api/rest/discovery/job/index/')
    argument_parser.add_argument('-un', '--username', action="store", help="Voyager account username", default='admin')
    argument_parser.add_argument('-pw', '--password', action="store", help="Voyager account password", default='admin')
    argument_parser.add_argument('-ex', '--extract', action="store", help="Option to extract files", default='true')
    arguments = argument_parser.parse_args()

    # For each line (path to a file), send it to be indexed.
    files_to_skip = [".xxx", ".yyy", ".zzz"]
    with open(arguments.file_list, 'rb') as f:
        for location in f:
            ext = os.path.splitext(location.strip('\r\n'))[1]
            if ext in files_to_skip:
                print ("Skipping {0}".format(location))
            else:
                send_job(location.strip('\r\n'), arguments.request_url, arguments.username, arguments.password, arguments.extract)
