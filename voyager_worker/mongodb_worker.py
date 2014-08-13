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
import decimal
import json
import base_job
import status


class ComplexEncoder(json.JSONEncoder):
    """To handle decimal types for json encoding."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def global_job(args):
    """Create a global job object for multiprocessing."""
    global job
    job = args


def worker():
    """Worker function to index each document in each collection in the database."""
    job.connect_to_zmq()
    job.connect_to_database()
    collection_names = [col for col in job.db_connection.collection_names() if not col == 'system.indexes']

    status_writer = status.Writer()
    for collection_name in collection_names:
        i = 0
        col = job.db_connection[collection_name]
        query = job.get_table_query(col)
        if query:
            documents = col.find(eval(query))
        else:
            documents = col.find()
        for doc in documents:
            entry = {}
            geo = {}
            if 'loc' in doc:
                if isinstance(doc['loc'][0], float):
                    geo['lon'] = doc['loc'][0]
                    geo['lat'] = doc['loc'][1]
                else:
                    geo['xmin'] = doc['loc'][0][0]
                    geo['xmax'] = doc['loc'][0][1]
                    geo['ymin'] = doc['loc'][1][0]
                    geo['ymax'] = doc['loc'][1][1]

            fields = doc.keys()
            fields.remove('loc')
            mapped_fields = job.map_fields(col.name, fields)
            mapped_fields = dict(zip(mapped_fields, doc.values()))
            mapped_fields['_discoveryID'] = job.discovery_id
            entry['id'] = '{0}_{1}_{2}'.format(job.location_id, col.name, i)
            entry['location'] = job.location_id
            entry['action'] = job.action_type
            entry['entry'] = {'geo': geo, 'fields': mapped_fields}
            job.send_entry(entry)
            i += 1
            status_writer.send_percent(float(i/documents.count()), collection_name, 'MongoDB')


def assign_job(job_info):
    job = base_job.Job(job_info)
    global_job(job)
    worker()
