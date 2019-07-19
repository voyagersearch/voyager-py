import requests
import json
import logging
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

'''
Report	                /API/1/report/
Company	                /API/1/company/
User	                /API/1/user/
System Variables	    /API/1/sysvar/
UTA Level 1	            /API/1/levelone/
UTA Level 2	            /API/1/leveltwo/
UTA Level 3	            /API/1/levelthree/
UTA Level Transaction	/API/1/transactions/
'''


logging.basicConfig(filename=r'/tmp/oea_tmp/smartsimple_connector.log',
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")


class SmartSimpleEndpoint(object):
    def __init__(self, endpoint, apikey):
        self.endpoint = endpoint
        self.api_key = apikey


class SmartSimpleEndpoints(object):
    # SYSVAR_LIST = SmartSimpleEndpoint(endpoint="sysvar", apikey='RHJnBwJDWQ5SVUNaA1EMUH1Db2FiWAZWDAM~')
    GRANTS_LIST = SmartSimpleEndpoint(endpoint="levelone", apikey='cQRnZ1pgZExFdFViVQV5ZmtBdFdDcHJLDAI~')
    # GRANTS_ACTIVITY_LIST = SmartSimpleEndpoint(endpoint="levelone", apikey="QVIaaVFGR3tCQWpdXmVRUUpZX0V!W3lkDDY~")
    # COMPANY_FILES_LIST = SmartSimpleEndpoint(endpoint="company", apikey="XXVAWUlScX96cnVpZQQBUWFgU2dPdXxmDAE~")
    # COMPANY_LIST = SmartSimpleEndpoint(endpoint="company", apikey="GVoHBElCbnlTWl56Q3tUUUkKGVNcYmBCDA4~")
    GRANTS_GET = SmartSimpleEndpoint(endpoint="levelone", apikey="dUI1C39jWhlRV3J5WgcMfVBUBlBFQHRVDA8~")
    GRANTS_GET_META = SmartSimpleEndpoint(endpoint="levelone", apikey="RVJjB3Zhfm9hM0AcA3BaemZGYW5ifgIFDAcF")
    # COMPANY_DOWNLOAD_FILE = SmartSimpleEndpoint(endpoint="company", apikey="cAFae1hmW1JAYlt3eQJxSVV1HQ5cUVtEDAcE")
    GRANTS_DOWNLOAD_FILE = SmartSimpleEndpoint(endpoint="levelone", apikey="Uwd8cl4BYg92WkN5A0JcQ3xrY0FRM1hBDAcH")


class SmartSimpleConnector(object):
    def __init__(self, job):
        self.job = job
        self.job.connect_to_zmq()
        self.endpoints = SmartSimpleEndpoints()

        for key in self.job.smartsimple_config.keys():
            logging.info("%s: %s" % (key, self.job.smartsimple_config[key]))

        config_keys = self.job.smartsimple_config.keys()

        try:
            if 'site' in config_keys:
                self.url = '%s/API/1/' % self.job.smartsimple_config['site']
            else:
                raise Exception("site missing from config")

            if 'companyid' in config_keys:
                self.company_id = self.job.smartsimple_config['companyid']
            else:
                raise Exception("companyid missing from config")

            if 'alias' in config_keys:
                self.alias = self.job.smartsimple_config['alias']
            else:
                raise Exception("alias missing from config")

            if 'username' in config_keys:
                self.username = self.job.smartsimple_config['username']
            else:
                raise Exception("username missing from config")

            if 'password' in config_keys:
                self.password = self.job.smartsimple_config['password']
            else:
                raise Exception("password missing from config")

            if 'temp_dir' in config_keys:
                self.temp_dir = self.job.smartsimple_config['temp_dir']
            else:
                raise Exception("temp_dir missing from config")

            if 'grants_list_api_key' in config_keys:
                self.endpoints.GRANTS_LIST.api_key = self.job.smartsimple_config['grants_list_api_key']
            else:
                raise Exception("grants_list_api_key missing from config")

            if 'grants_get_api_key' in config_keys:
                self.endpoints.GRANTS_GET.api_key = self.job.smartsimple_config['grants_get_api_key']
            else:
                raise Exception("grants_get_api_key missing from config")

            if 'grants_download_file_api_key' in config_keys:
                self.endpoints.GRANTS_DOWNLOAD_FILE.api_key = self.job.smartsimple_config['grants_download_file_api_key']
            else:
                raise Exception("grants_download_file_api_key missing from config")

            if 'file_field_ids' in config_keys:
                self.file_field_ids = self.job.smartsimple_config['file_field_ids']
            else:
                raise Exception("file_field_ids missing from config")

        except Exception as e:
            logging.exception(e)
            raise e

        self.payload = "apitoken=%s&username=%s&password=%s&alias=%s&companyid=%s"
        self.headers = {
            'Content-Type': "application/x-www-form-urlencoded",
        }

    def do_request(self, api_token, endpoint='company'):
        return requests.post(self.get_url(endpoint), data=self.get_payload(api_token), headers=self.headers, verify=False)

    def get_url(self, endpoint):
        return '%s%s' % (self.url, endpoint)

    def get_payload(self, api_token):
        return self.payload % (api_token, self.username, self.password, self.alias, self.company_id)

    def get_grant_ids(self):
        resp = self.do_request(
            api_token=self.endpoints.GRANTS_LIST.api_key,
            endpoint=self.endpoints.GRANTS_LIST.endpoint
        )
        records = [x['recordid'] for x in resp.json()['records'] if x['recordid']]
        return records

    def get_grant(self, recordid):
        payload = self.get_payload(self.endpoints.GRANTS_GET.api_key)
        payload = '%s&recordid=%s' % (payload, recordid)
        return requests.post(self.get_url(self.endpoints.GRANTS_GET.endpoint), data=payload, headers=self.headers, verify=False)

    def get_grant_meta(self):
        payload = self.get_payload(self.endpoints.GRANTS_GET_META.api_key)
        return requests.post(self.get_url(self.endpoints.GRANTS_GET_META.endpoint), data=payload, headers=self.headers, verify=False)

    # - the api endpoint in the system needs to have the returned fields defined (cf_Grant PDF),
    # - the config for this connector tells us which of those fields refer to files (cf_Grant PDF)
    # - the file needs the field id to download via api (1234567)
    # so... we build a lookup from the meta result (all the fields!) to map the two together
    # {cf_Grant PDF: 1234567}
    def create_fields_lookup(self, meta):
        meta_fields = meta.json()['levelone']['fields']
        return {'cf_%s' % x['name']: x['id'] for x in meta_fields if 'cf_%s' % x['name'] in self.file_field_ids}

    def download_file(self, filename, objectid, fieldid):
        """
        :param filename: name of the file to be downloaded.
        :param objectid: record id of the grant
        :param fieldid: id of the field where the file is associated
        """

        file_dir = os.path.join(self.temp_dir, objectid)
        file_path = os.path.join(file_dir, filename)

        if os.path.exists(file_path):
            logging.info("file exists at path: %s, skipping download" % file_path)
            return file_path

        else:

            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            payload = self.get_payload(self.endpoints.GRANTS_DOWNLOAD_FILE.api_key)
            payload = '%s&fieldid=%s&objectid=%s&filename=%s' % (payload, fieldid, objectid, filename)

            file_rsp = requests.post(
                self.get_url(self.endpoints.GRANTS_DOWNLOAD_FILE.endpoint),
                data=payload,
                headers=self.headers,
                verify=False)

            logging.info(file_rsp.headers['Content-Length'])

            if int(file_rsp.headers['Content-Length']) > 0:

                logging.info("downloading file to %s" % file_path)

                with open(file_path, 'wb') as f:
                    for chunk in file_rsp.iter_content(chunk_size=1024):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)

        return file_path

    def create_file_job(self, local_path, keep_fields, file_id):

        job = {
            'location': self.job.location_id,
            'action': self.job.action_type,
            'path': local_path,
            'entry': {
                'fields': {
                    'id': file_id,
                    '_discoveryID': self.job.discovery_id,
                    '__to_extract': True,
                    'path': local_path,
                    '__keep_fields': keep_fields
                }
            }
        }

        return job

    def create_grant_job(self, grant, grant_id):
        job = {
            'location': self.job.location_id,
            'action': self.job.action_type,
            'entry': {
                'fields': {
                    '_discoveryID': self.job.discovery_id,
                    '__to_extract': False,
                    'id': grant_id,
                    # todo: don't hardcode this
                    'name': grant['sf_Application Name']
                }
            }
        }

        for key in grant.keys():

            if grant[key]:

                cleaned_key = key.replace('sf_', 'fs_').replace('cf_', 'fs_').replace(' ', '_').lower()
                job['entry']['fields'][cleaned_key] = grant[key]

        return job

    def scan(self):
        # get all the grants (paging not needed :)
        grant_ids = self.get_grant_ids()

        # sanity, for testing
        grant_ids = grant_ids[75:85]

        # get the grant meta so we can do a fields lookup.
        # we need the field id to download the file,
        # but want to supply the field name for ease of setup and config / sanity.
        fields_lookup = self.create_fields_lookup(self.get_grant_meta())
        logging.info(json.dumps(fields_lookup, indent=4))

        # iterate over the grant ids
        for grant_id in grant_ids:
            try:
                # get whatever info we can from the grant
                grant_rsp = self.get_grant(grant_id)
                logging.info(json.dumps(grant_rsp.json(), indent=4))

                # there can be only one... or none?
                if 'records' in grant_rsp.json() and len(grant_rsp.json()['records']) > 0:
                    grant = grant_rsp.json()['records'][0]

                    # solr id
                    grant_doc_id = '%s_%s' % (self.job.location_id, grant_id)

                    grant_job = self.create_grant_job(grant, grant_doc_id)
                    grant_links = []

                    # get the files for this grant.
                    # there are multiple fields that be files,
                    # and multiple files can be in each field
                    for field_id in self.file_field_ids:
                        if field_id in grant:

                            file_fields = [{
                                'name': x.split('|')[0], 'id': x.split('|')[1].replace(':', '')
                            } for x in grant[field_id].split('\n') if len(x.split('|')) > 1]

                            files = [foo for foo in file_fields if foo]

                            for f in files:

                                # field lookup maps field name -> field id
                                file_id = '%s_%s_%s' % (self.job.location_id, grant_id, f['id'])
                                file_path = self.download_file(f['name'], grant_id, fields_lookup[field_id])
                                logging.info(file_path)

                                grant_fields = {
                                    'fs_grant_id': grant_id,
                                    'fs_field_id': fields_lookup[field_id],
                                    'fs_field_name': field_id,
                                    'fs_document_type': field_id.replace('cf_', '').replace('sf_', '')
                                }

                                grant_links.append({'relation': 'contains', 'id': file_id})

                                file_job = self.create_file_job(file_path, grant_fields, file_id)
                                logging.info(json.dumps(file_job, indent=4))
                                self.job.send_entry(file_job)

                    grant_job['entry']['links'] = grant_links
                    logging.info(json.dumps(grant_job, indent=4))
                    self.job.send_entry(grant_job)

            except Exception as e:
                logging.exception(e)


def run_job(job):
    SmartSimpleConnector(job).scan()
