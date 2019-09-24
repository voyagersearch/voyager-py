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

logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)


class SmartSimpleEndpoint(object):
    def __init__(self, endpoint, apikey):
        self.endpoint = endpoint
        self.api_key = apikey


class SmartSimpleEndpoints(object):
    GRANTS_ACTIVITY_LIST = SmartSimpleEndpoint(endpoint="leveltwo", apikey="QVIaaVFGR3tCQWpdXmVRUUpZX0V!W3lkDDY~")
    GRANT_ACTIVITY_GET = SmartSimpleEndpoint(endpoint="leveltwo", apikey="HVxtW1hbf1NEHGNSQAMMVkNCQFRMC3Z7DAcB")
    # SYSVAR_LIST = SmartSimpleEndpoint(endpoint="sysvar", apikey='RHJnBwJDWQ5SVUNaA1EMUH1Db2FiWAZWDAM~')
    GRANTS_LIST = SmartSimpleEndpoint(endpoint="levelone", apikey='cQRnZ1pgZExFdFViVQV5ZmtBdFdDcHJLDAI~')
    GRANTS_GET = SmartSimpleEndpoint(endpoint="levelone", apikey="dUI1C39jWhlRV3J5WgcMfVBUBlBFQHRVDA8~")
    GRANTS_GET_META = SmartSimpleEndpoint(endpoint="levelone", apikey="RVJjB3Zhfm9hM0AcA3BaemZGYW5ifgIFDAcF")
    GRANT_ACTIVITY_GET_META = SmartSimpleEndpoint(endpoint="leveltwo", apikey="GVoHBElCbnlTWl56Q3tUUUkKGVNcYmBCDA4~")
    GRANTS_DOWNLOAD_FILE = SmartSimpleEndpoint(endpoint="levelone", apikey="Uwd8cl4BYg92WkN5A0JcQ3xrY0FRM1hBDAcH")
    GRANT_ACTIVITY_DOWNLOAD_FILE = SmartSimpleEndpoint(endpoint="leveltwo", apikey="XXVAWUlScX96cnVpZQQBUWFgU2dPdXxmDAE~")
    # COMPANY_FILES_LIST = SmartSimpleEndpoint(endpoint="company", apikey="XXVAWUlScX96cnVpZQQBUWFgU2dPdXxmDAE~")
    # COMPANY_LIST = SmartSimpleEndpoint(endpoint="company", apikey="GVoHBElCbnlTWl56Q3tUUUkKGVNcYmBCDA4~")
    # COMPANY_DOWNLOAD_FILE = SmartSimpleEndpoint(endpoint="company", apikey="cAFae1hmW1JAYlt3eQJxSVV1HQ5cUVtEDAcE")


class SmartSimpleConnector(object):
    def __init__(self, job):
        self.job = job
        self.job.connect_to_zmq()
        self.endpoints = SmartSimpleEndpoints()

        # for key in self.job.smartsimple_config.keys():
        #     logging.info("%s: %s" % (key, self.job.smartsimple_config[key]))

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

            if 'grants_get_meta_api_key' in config_keys:
                self.endpoints.GRANTS_GET_META.api_key = self.job.smartsimple_config['grants_get_meta_api_key']
            else:
                raise Exception("grants_get_meta_api_key missing from config")

            if 'grant_activity_get_meta_api_key' in config_keys:
                self.endpoints.GRANT_ACTIVITY_GET_META.api_key = self.job.smartsimple_config['grant_activity_get_meta_api_key']
            else:
                raise Exception("grant_activity_get_meta_api_key missing from config")

            if 'grant_activity_download_file_api_key' in config_keys:
                self.endpoints.GRANT_ACTIVITY_DOWNLOAD_FILE.api_key = self.job.smartsimple_config['grant_activity_download_file_api_key']
            else:
                raise Exception("grant_activity_download_file_api_key missing from config")

            if 'grants_download_file_api_key' in config_keys:
                self.endpoints.GRANTS_DOWNLOAD_FILE.api_key = self.job.smartsimple_config['grants_download_file_api_key']
            else:
                raise Exception("grants_download_file_api_key missing from config")

            if 'grant_activity_list_api_key' in config_keys:
                self.endpoints.GRANTS_ACTIVITY_LIST.api_key = self.job.smartsimple_config['grant_activity_list_api_key']
            else:
                raise Exception("grant_activity_list_api_key missing from config")

            if 'grant_activity_get_api_key' in config_keys:
                self.endpoints.GRANT_ACTIVITY_GET.api_key = self.job.smartsimple_config['grant_activity_get_api_key']
            else:
                raise Exception("grant_activity_get_api_key missing from config")

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
        self.grant_fields_lookup = self.create_grant_fields_lookup()
        self.grant_activity_fields_lookup = self.create_grant_activity_fields_lookup()

    def do_request(self, api_token, endpoint='company'):
        return requests.post(self.get_url(endpoint),
                             data=self.get_payload(api_token),
                             headers=self.headers,
                             verify=False)

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

    def get_grant_activity_ids(self):
        resp = self.do_request(
            api_token=self.endpoints.GRANTS_ACTIVITY_LIST.api_key,
            endpoint=self.endpoints.GRANTS_ACTIVITY_LIST.endpoint
        )
        records = [{x['recordid']: x['cf_Import - OEA Grant Number']} for x in resp.json()['records'] if x['recordid']]
        return records

    def get_grant(self, recordid):
        payload = self.get_payload(self.endpoints.GRANTS_GET.api_key)
        payload = '%s&recordid=%s' % (payload, recordid)
        return requests.post(self.get_url(self.endpoints.GRANTS_GET.endpoint),
                             data=payload,
                             headers=self.headers,
                             verify=False)

    def get_grant_activity(self, recordid):
        payload = self.get_payload(self.endpoints.GRANT_ACTIVITY_GET.api_key)
        payload = '%s&recordid=%s' % (payload, recordid)
        return requests.post(self.get_url(self.endpoints.GRANT_ACTIVITY_GET.endpoint),
                             data=payload,
                             headers=self.headers,
                             verify=False)

    def get_grant_meta(self):
        payload = self.get_payload(self.endpoints.GRANTS_GET_META.api_key)
        return requests.post(self.get_url(self.endpoints.GRANTS_GET_META.endpoint),
                             data=payload,
                             headers=self.headers,
                             verify=False)

    def get_grant_activity_meta(self):
        payload = self.get_payload(self.endpoints.GRANT_ACTIVITY_GET_META.api_key)
        return requests.post(self.get_url(self.endpoints.GRANT_ACTIVITY_GET_META.endpoint),
                             data=payload,
                             headers=self.headers,
                             verify=False)

    # - the api endpoint in the system needs to have the returned fields defined (cf_Grant PDF),
    # - the config for this connector tells us which of those fields refer to files (cf_Grant PDF)
    # - the file needs the field id to download via api (1234567)
    #   so... we build a lookup from the meta result (all the fields!) to map the two together
    #   {cf_Grant PDF: 1234567}
    def create_grant_fields_lookup(self):
        # get the grant meta so we can do a fields lookup.
        # we need the field id to download the file,
        # but want to supply the field name for ease of setup and config / sanity.
        meta = self.get_grant_meta()
        lookup = {}
        if 'levelone' in meta.json() and 'fields' in meta.json()['levelone']:
            meta_fields = meta.json()['levelone']['fields']
            lookup.update({'cf_%s' % x['name']: x['id'] for x in meta_fields if 'cf_%s' % x['name'] in self.file_field_ids})

        return lookup

    def create_grant_activity_fields_lookup(self):
        # get the grant meta so we can do a fields lookup.
        # we need the field id to download the file,
        # but want to supply the field name for ease of setup and config / sanity.
        meta = self.get_grant_activity_meta()
        lookup = {}
        if 'levelone' in meta.json() and 'fields' in meta.json()['levelone']:
            meta_fields = meta.json()['levelone']['fields']
            lookup.update({'cf_%s' % x['name']: x['id'] for x in meta_fields if 'cf_%s' % x['name'] in self.file_field_ids})

        if 'leveltwo' in meta.json() and 'fields' in meta.json()['leveltwo']:
            meta_fields = meta.json()['leveltwo']['fields']
            lookup.update(
                {'cf_%s' % x['name']: x['id'] for x in meta_fields if 'cf_%s' % x['name'] in self.file_field_ids})

        return lookup

    def download_file(self, filename, objectid, fieldid, smart_simple_endpoint):
        """
        :param filename: name of the file to be downloaded.
        :param objectid: record id of the grant
        :param fieldid: id of the field where the file is associated
        :param smart_simple_endpoint endpoint to download the file from
        """

        file_dir = os.path.join(self.temp_dir, objectid)
        file_path = os.path.join(file_dir, filename)

        if os.path.exists(file_path):
            # logging.info("file exists at path: %s, skipping download" % file_path)
            return file_path

        else:

            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            payload = self.get_payload(smart_simple_endpoint.api_key)
            payload = '%s&fieldid=%s&objectid=%s&filename=%s' % (payload, fieldid, objectid, filename)

            file_rsp = requests.post(
                self.get_url(smart_simple_endpoint.endpoint),
                data=payload,
                headers=self.headers,
                verify=False)

            # logging.info(file_rsp.headers['Content-Length'])

            if int(file_rsp.headers['Content-Length']) > 0:

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
                    'name': grant['sf_Application Name'],
                    'grp_content_type': 'Grant'
                }
            }
        }

        # add all the other grant fields, just convert them to voyager dynamic fields
        # TODO - field mapping for types?
        for key in grant.keys():
            if grant[key]:
                cleaned_key = key.replace('sf_', 'fs_').replace('cf_', 'fs_').replace(' ', '_').lower()
                job['entry']['fields'][cleaned_key] = grant[key]

        return job

    def scan(self):
        # get all the grants (paging not needed :)
        grant_ids = self.get_grant_ids()
        logging.info("has %s grants" % len(grant_ids))
        grant_activity_ids = self.get_grant_activity_ids()

        # TODO: remove this... sanity, for testing
        # grant_ids = grant_ids[5:10]

        # iterate over the grant ids
        for grant_id in grant_ids:
            try:
                # get whatever info we can from the grant
                grant_rsp = self.get_grant(grant_id)

                # there can be only one... or none?
                if 'records' in grant_rsp.json() and len(grant_rsp.json()['records']) > 0:
                    grant = grant_rsp.json()['records'][0]
                    # solr id
                    grant_doc_id = '%s_%s' % (self.job.location_id, grant_id)
                    # create the grant job
                    grant_job = self.create_grant_job(grant, grant_doc_id)
                    grant_links = []

                    grant_links.extend(
                        self.process_files(
                            grant,
                            grant_id,
                            grant_id,
                            self.grant_fields_lookup,
                            self.endpoints.GRANTS_DOWNLOAD_FILE,
                            "Grant File"))

                    # select the activity ids for the grant number
                    activity_ids = self.get_activity_ids_for_grant(grant['cf_OEA Grant Number'], grant_activity_ids)
                    logging.info("grant %s has %s activity records" % (grant['cf_OEA Grant Number'], len(activity_ids)))

                    for activity_id in activity_ids:
                        # get each grant activity
                        grant_activity_rsp = self.get_grant_activity(activity_id.keys()[0])
                        activity = grant_activity_rsp.json()['records'][0]

                        grant_links.extend(
                            self.process_files(
                                activity,
                                activity_id.keys()[0],
                                grant_id,
                                self.grant_activity_fields_lookup,
                                self.endpoints.GRANT_ACTIVITY_DOWNLOAD_FILE,
                                "Grant Activity File"))

                    grant_job['entry']['links'] = grant_links
                    self.job.send_entry(grant_job)

            except Exception as e:
                logging.exception(e)
                
        logging.info("done.")

    def get_activity_ids_for_grant(self, grant_number, grant_activity_ids):
        return [x for x in grant_activity_ids if x[x.keys()[0]] == grant_number]

    def process_files(self, obj, object_id, grant_id, fields_lookup, endpoint, content_type):
        links = []
        # get the files for this grant.
        # there are multiple fields that be files,
        # and multiple files can be in each field
        for field_id in self.file_field_ids:

            if field_id in obj:

                file_fields = [{
                    'name': x.split('|')[0], 'id': x.split('|')[1].replace(':', '')
                } for x in obj[field_id].split('\n') if len(x.split('|')) > 1]

                files = [foo for foo in file_fields if foo]

                for f in files:
                    file_id = '%s_%s_%s' % (self.job.location_id, grant_id, f['id'])

                    file_path = self.download_file(f['name'],
                                                   object_id,
                                                   fields_lookup[field_id],
                                                   endpoint)

                    keep_fields = {
                        'fs_grant_id': grant_id,
                        'fs_field_id': fields_lookup[field_id],
                        'fs_field_name': field_id,
                        'fs_document_type': field_id.replace('cf_', '').replace('sf_', ''),
                        'grp_content_type': content_type
                    }

                    links.append({'relation': 'contains', 'id': file_id})

                    file_job = self.create_file_job(file_path, keep_fields, file_id)
                    self.job.send_entry(file_job)

        return links


def run_job(job):
    SmartSimpleConnector(job).scan()

