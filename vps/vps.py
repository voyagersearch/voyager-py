# (C) Copyright 2017 Voyager Search
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
import imp
import argparse
import logging
import json
import requests
from requests import ConnectionError

from bottle import Bottle, response, request

ROOT_APP = Bottle()

logging.basicConfig(filename="voyager-python-service.log",
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")


def jsonp(req, dictionary):
    """
    Returns the callback wrapped response if it's been requested.
    """
    if req.query.callback:
        return "%s(%s)" % (req.query.callback, dictionary)
    return dictionary


@ROOT_APP.route('/')
def root_index():
    """
    lists all the loaded routes and their methods.
    """
    response.content_type = 'application/json'
    routes = []
    for _route in ROOT_APP.routes:
        routes.append({'path': _route.rule, 'method': _route.method})
    return jsonp(request, {'routes': routes})


def merge_routes(folder_path):
    """
    List all the files in a folder and load the services & routes into the main
    voyager_service app. 
    """
    files = os.listdir(folder_path)

    # What else might need to be removed?
    if '__init__.py' in files:
        files.remove('__init__.py')

    for service_name in files:
        # Does it end in .py? can add additional checking here.
        # maybe enforce a "name.service.py" convention.
        if service_name.split('.')[-1] == 'py':
            _file = imp.load_source('module', '{2}{0}{1}'.format(os.sep, service_name, folder_path))
            try:
                ROOT_APP.merge(_file.service)
            except Exception as e:
                logging.exception('Error while merging routes from {0}: {1}'.format(service_name, e))


def check_env(user_name='', password=''):
    """
    Checks the os.environ for certain vars and sets them if they dont exist.
    Voyager 1.9.10 - added specific env vars for pipeline steps and workers,
    Voyager 1.9.13 - added specific env vars for this service.
    """
    # TODO: is there a better way of defaulting the baseurl if it's not in the os.environ? 
    # especially since we want to use this to try to access the other values the os.environ 
    # hasn't been set.
    settings = None
    if 'VOYAGER_BASE_URL' not in os.environ:
        # NOTE: change this if running manually and the base url is not localhost:8888
        os.environ['VOYAGER_BASE_URL'] = 'http://localhost:8888'
        # if the VOYAGER_BASE_URL isn't in the environ, can safely assume the other 
        # environ vars won't be either, so go get them from the service. 
        try:
            resp = requests.get('{0}/api/rest/system/settings'.format(os.environ['VOYAGER_BASE_URL']),
                                auth=(user_name, password))
            settings = resp.json()
            if 'message' in settings and 'Subject does not have permission' in settings['message']:
                raise ConnectionError
        except ConnectionError:
            logging.exception('Error trying to get the system settings from {0}/api/rest/system/settings'.format(os.environ['VOYAGER_BASE_URL']))

    if 'VOYAGER_LOGS_DIR' not in os.environ:
        if settings and settings['folders'] and settings['folders']['logs']:
            os.environ['VOYAGER_LOGS_DIR'] = settings['folders']['logs']
        else:
            # default to <working dir>/logs
            os.environ['VOYAGER_LOGS_DIR'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

    if 'VOYAGER_META_DIR' not in os.environ:
        if settings and settings['folders'] and settings['folders']['meta']:
            os.environ['VOYAGER_META_DIR'] = settings['folders']['meta']
        else:
            # default to <working dir>/meta
            os.environ['VOYAGER_META_DIR'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'meta')

    if 'VOYAGER_APPS_DIR' not in os.environ:
        if settings and settings['folders'] and settings['folders']['apps']:
            os.environ['VOYAGER_APPS_DIR'] = settings['folders']['apps']
        else:
            # default to <working dir>/apps
            os.environ['VOYAGER_APPS_DIR'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'apps')

    if 'VOYAGER_DATA_DIR' not in os.environ:
        if settings and settings['folders'] and settings['folders']['data']:
            os.environ['VOYAGER_DATA_DIR'] = settings['folders']['data']
        else:
            # default to <working dir>/data
            os.environ['VOYAGER_DATA_DIR'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    if 'VOYAGER_PYTHON_SERVICE_FOLDERS' not in os.environ:
        os.environ['VOYAGER_PYTHON_SERVICE_FOLDERS'] = json.dumps([])


def create_and_start_service(*args):
    """
    Initialize the voyager service, load all the mapped service files and merge the routes
    """
    check_env(args[0].username, args[0].password)

    logging_path = os.path.normpath(os.environ['VOYAGER_LOGS_DIR'])
    if not os.path.exists(logging_path):
        os.makedirs(logging_path)

    # remove any previous handlers and set the logging to the voyager logs dir
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(filename=os.path.join(os.sep, logging_path, 'voyager-python-service.log'),
                        level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    # load the default services dir 
    services_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'services')
    merge_routes(services_dir)

    try:
        # load any additional services dirs
        folders = json.loads(os.environ['VOYAGER_PYTHON_SERVICE_FOLDERS'])
        logging.debug(folders)
        for folder in folders:
            merge_routes(folder)
    except Exception as e:
        logging.exception(e)

    port = args[0].port
    if 'VOYAGER_PYTHON_SERVICE_PORT' in os.environ:
        port = os.environ['VOYAGER_PYTHON_SERVICE_PORT']

    address = args[0].address
    if 'VOYAGER_PYTHON_SERVICE_ADDRESS' in os.environ:
        address = os.environ['VOYAGER_PYTHON_SERVICE_ADDRESS']

    ROOT_APP.run(debug=True, host=address, port=port)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='Voyager Service')
    arg_parser.add_argument('-un', '--username', help="Voyager account username", default='')
    arg_parser.add_argument('-pw', '--password', help="Voyager account password", default='')
    arg_parser.add_argument('-p', '--port', help='port to run on', default=9999)
    arg_parser.add_argument('-a', '--address', help='service address', default='0.0.0.0')

    arrgs = arg_parser.parse_args()
    create_and_start_service(arrgs)
