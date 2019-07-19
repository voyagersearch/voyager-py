import requests
import json
import os
import logging

import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

logging.basicConfig(filename=r'D:/tmp/oea_tmp/alfresco_connector.log',
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")


class Alfresco(object):
    def __init__(self, job):
        self.job = job
        self.job.connect_to_zmq()

        for key in self.job.alfresco_config.keys():
            logging.info("%s: %s" % (key, self.job.alfresco_config[key]))

        config_keys = self.job.alfresco_config.keys()

        if 'site' in config_keys:
            self.site = self.job.alfresco_config['site']
        else:
            raise Exception("site missing from config")

        if 'username' in config_keys:
            self.username = self.job.alfresco_config['username']
        else:
            raise Exception("username missing from config")

        if 'password' in config_keys:
            self.password = self.job.alfresco_config['password']
        else:
            raise Exception("password missing from config")

        if 'temp_dir' in config_keys:
            self.temp_dir = self.job.alfresco_config['temp_dir']
        else:
            raise Exception("temp_dir missing from config")

        if 'folder_ids' in config_keys:
            self.folders_to_index = self.job.alfresco_config['folder_ids']
        else:
            raise Exception("temp_dir missing from config")

        if 'exclude_node_ids' in config_keys:
            self.exclude_node_ids = self.job.alfresco_config['exclude_node_ids']
        else:
            self.exclude_node_ids = []

        if 'exclude_extensions' in config_keys:
            self.exclude_extensions = self.job.alfresco_config['exclude_extensions']
        else:
            self.exclude_extensions = []

        self.base_url = '%s%s' % (self.site, '/alfresco/api/-default-/public/alfresco/versions/1/%s')

    def fetch(self, endpoint):
        return requests.get(self.base_url % endpoint, auth=(self.username, self.password), verify=False)

    def get_node_info(self, node='-root-'):
        return self.fetch('nodes/%s' % node).json()

    def get_node_children(self, node='-root-', max_items=20, skip=0):
        return self.fetch('nodes/%s/children?maxItems=%s&skipCount=%s' % (node, max_items, skip)).json()

    def create_entry(self, local_path, remote_path, file_id, parent_id):

        location_id = self.job.location_id
        action_type = self.job.action_type
        discovery_id = self.job.discovery_id

        entry = {
            'location': location_id,
            'action': action_type,
            'path': local_path,
            'entry': {
                'fields': {
                    '_discoveryID': discovery_id,
                    '__to_extract': True,
                    'path': local_path,
                    'id': file_id,
                    '__keep_fields': {
                        'fs_download_url': '%s/files/%s/display/list(overlay:files/%s/view)' % (self.site, parent_id, file_id),
                        'fs_path': remote_path
                    }
                }
            }
        }

        return entry

    def download_file(self, node_id, local_path):
        local_file = os.path.join(self.temp_dir, local_path)

        if not os.path.exists(local_file):
            if not os.path.exists(os.path.dirname(local_file)):
                os.makedirs(os.path.dirname(local_file))

            remote_file = requests.get(self.base_url % "nodes/%s/content" % node_id, auth=(self.username, self.password), verify=False, stream=True)
            with open(local_file, 'wb') as f:
                for chunk in remote_file.iter_content(chunk_size=4096):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

        return local_file

    # TODO: paging
    def recurse_children(self, node, path=''):
        rsp = self.get_node_children(node=node)
        # logging.info(json.dumps(rsp, indent=4))
        children = rsp['list']['entries']
        while rsp['list']['pagination']['hasMoreItems']:
            skip = rsp['list']['pagination']['skipCount'] + rsp['list']['pagination']['maxItems']
            rsp = self.get_node_children(node=node, skip=skip)
            children = children + rsp['list']['entries']
        for child in children:
            if child['entry']['id'] in self.exclude_node_ids:
                logging.info("node id exclusion rule: %s - ignoring %s" % (child['entry']['id'], child['entry']['name']))
                continue

            if child['entry']['isFolder']:
                self.recurse_children(node=child['entry']['id'], path="%s/%s" % (path, child['entry']['name']))

            elif child['entry']['isFile']:
                exclude = False
                for ext in self.exclude_extensions:
                    if child['entry']['name'].lower().endswith(ext.lower()):
                        logging.info(
                            "file extension exclusion rule: %s - ignoring %s" % (child['entry']['id'], child['entry']['name']))
                        exclude = True

                if exclude:
                    continue

                # logging.info(json.dumps(child['entry'], indent=4))
                local_file = self.download_file(child['entry']['id'], "%s/%s" % (path, child['entry']['name']))
                # logging.info('saved to %s' % local_file)
                entry = self.create_entry(
                    local_file,
                    file_id=child['entry']['id'],
                    remote_path=path,
                    parent_id=child['entry']['parentId']
                )
                # logging.info(json.dumps(entry, indent=4))
                self.job.send_entry(entry)

    def scan(self):
        for folder in self.folders_to_index:
            info = self.get_node_info(folder)
            self.recurse_children(folder, path=info['entry']['name'])


def run_job(job):
    Alfresco(job).scan()
