import os
import sys
import json
import logging


sys.path.append(os.path.join(os.path.abspath(os.path.join(__file__, "../../../../..")), 'arch/win32_x86/py'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'layer_creation'))
try:
    import zmq
    import requests
    from requests_ntlm import HttpNtlmAuth
    import settings
except ImportError as ie:
    sys.stdout.write(ie.message)
    sys.exit(1)


logging.basicConfig(filename=r"D:\voyager\voyager_testing_temp\record_count.log", level=logging.DEBUG)


def update_index(record_count, item_id, location, name, endpoint):
    """
    Update the index by re-indexng an item.
    """
    indexer = settings.INDEXER
    zmq_socket = zmq.Context.instance().socket(zmq.PUSH)
    zmq_socket.connect(indexer)
    entry = {"action": "UPDATE", "id": item_id, "path": endpoint, "location": location,
             "entry": {"fields": {"rowCount": record_count, "name": name}}}
    zmq_socket.send_json(entry)


def run(entry):
    """
    Gets the record count for map and feature service layers.
    """
    new_entry = json.load(open(entry, "rb"))
    query = {'where': '1=1', 'returnCountOnly': True, 'f': 'json'}

    if 'job' in new_entry and 'id' in new_entry['entry']['fields']:
        id = new_entry['entry']['fields']['id']
        if 'rest_endpoint' not in new_entry['entry']['fields'] and 'path' not in new_entry['entry']['fields']:
            return
        try:
            service_type = new_entry['entry']['fields']['format']
        except KeyError:
            return

    if service_type == 'application/x-arcgis-map-server':
        # Get record count the map server layers (children of the map server item).
        try:
            children = new_entry['entry']['children']
            for child in children:
                child_id = child['fields']['id']
                name = child['fields']['name']
                location = child['fields']['location']
                path = child['fields']['path']
                try:
                    res = requests.get('{0}/query?'.format(path), params=query, auth=HttpNtlmAuth(settings.USERNAME, settings.PASSWORD))
                    count = res.json()['count']
                    update_index(count, child_id, location, name, path)
                except Exception:
                    pass

                try:
                    sub_children = child['children']
                    for sub_child in sub_children:
                        child_id = sub_child['fields']['id']
                        name = sub_child['fields']['name']
                        location = sub_child['fields']['location']
                        path = sub_child['fields']['location']
                        res = requests.get('{0}/query?'.format(path), params=query, auth=HttpNtlmAuth(settings.USERNAME, settings.PASSWORD))
                        count = res.json()['count']
                        update_index(count, child_id, location, name, path)
                except KeyError:
                    continue
        except KeyError:
            pass

    elif service_type == 'application/x-arcgis-feature-server':
        try:
            children = new_entry['entry']['children']
            for child in children:
                child_id = child['fields']['id']
                location = child['fields']['location']
                path = child['fields']['path']
                name = child['fields']['name']
                res = requests.get('{0}/query?'.format(path), params=query, auth=HttpNtlmAuth(settings.USERNAME, settings.PASSWORD))
                count = res.json()['count']
                update_index(count, child_id, location, name, child['fields']['path'])
        except KeyError:
            pass

    elif service_type in ('application/x-arcgis-online-service', 'application/x-arcgis-online-map'):
        id = ''
        try:
            service_url = new_entry['entry']['fields']['service_url']
            id = new_entry['entry']['fields']['id']
            location = new_entry['entry']['fields']['location']
            path = new_entry['entry']['fields']['path']
            name = new_entry['entry']['fields']['name']
            res = requests.get('{0}/query?'.format(service_url), params=query, auth=HttpNtlmAuth(settings.USERNAME, settings.PASSWORD))
            count = res.json()['count']
            update_index(count, id, location, name, path)
        except Exception as ex:
            logging.info('Item ID: {0} - {1}'.format(id, ex.message))
            pass

    return
