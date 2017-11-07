"""
Voyager Python Service 
"""
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
    returns the callback wrapped response if it's been requested.
    """
    if  req.query.callback:
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
    list all the files in a folder and load the services & routes into the main 
    voyager_service app. 
    """
    files = os.listdir(folder_path)

    # what else might need to be removed?
    if '__init__.py' in files:
        files.remove('__init__.py')

    for service_name in files:
        # does it end in .py? can add additional checking here.
        # maybe enforce a "name.service.py" convention.
        if service_name.split('.')[-1] == 'py':
            _file = imp.load_source('module', '{2}{0}{1}'.format(os.sep, service_name, folder_path))
            try:
                ROOT_APP.merge(_file.service)
            except Exception as e:
                logging.exception('error while merging routes from %s: %s', service_name, e)


def check_env():
    """
    checks the os.environ for certain vars and sets them if they dont exist.
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
            resp = requests.get('%s/api/rest/system/settings' % os.environ['VOYAGER_BASE_URL'])
            settings = resp.json()
        except ConnectionError as e:
            logging.exception("error trying to get the system settings from %s", ('%s/api/rest/system/settings' % os.environ['VOYAGER_BASE_URL']))

    if 'VOYAGER_LOGS_DIR' not in os.environ:
        if settings and settings['folders'] and settings['folders']['logs']:
            os.environ['VOYAGER_LOGS_DIR'] = settings['folders']['logs']
        else:
            # default to <working dir>/logs
            os.environ['VOYAGER_LOGS_DIR'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

    if 'VOYAGER_DATA_DIR' not in os.environ:
        if settings and settings['folders'] and settings['folders']['data']:
            os.environ['VOYAGER_DATA_DIR'] = settings['folders']['data']
        else:
            # default to <working dir>/data
            os.environ['VOYAGER_DATA_DIR'] =  os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    if 'VOYAGER_SERVICE_FOLDERS' not in os.environ:
        os.environ['VOYAGER_SERVICE_FOLDERS'] = json.dumps([])


def create_and_start_service():
    """
    Initialize the voyager service, load all the mapped service files and merge the routes
    """

    check_env()

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
        folders = json.loads(os.environ['VOYAGER_SERVICE_FOLDERS'])
        logging.debug(folders)
        for folder in folders:
            merge_routes(folder)
    except Exception as e:
        logging.exception(e)

    arg_parser = argparse.ArgumentParser(description='Voyager Service')
    arg_parser.add_argument('-p', '--port', help='port to run on', default=9999)
    arg_parser.add_argument('-a', '--address', help='service address', default='localhost')
    arrgs = arg_parser.parse_args()

    port = arrgs.port
    if 'VOYAGER_SERVICE_PORT' in os.environ:
        port = os.environ['VOYAGER_SERVICE_PORT']

    address = arrgs.address
    if 'VOYAGER_SERVICE_ADDRESS' in os.environ:
        address = os.environ['VOYAGER_SERVICE_ADDRESS']

    ROOT_APP.run(debug=True, host=address, port=port)


if __name__ == '__main__':
    create_and_start_service()
