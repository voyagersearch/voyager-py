# (C) Copyright 2016 Voyager Search
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
import boto3
import base_job
from utils import status
from utils import worker_utils


status_writer = status.Writer()


class ComplexEncoder(json.JSONEncoder):
    """To handle decimal types for json encoding."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def get_tables(job):
    """Return the tables to be indexed."""
    table_names = []
    tables_to_skip = job.tables_to_skip()
    tables_to_keep = job.tables_to_keep()

    if tables_to_keep == ['*']:
        table_names = [tbl.name for tbl in job.dynamodb.tables.iterator()]
    else:
        table_names = job.tables_to_keep()

    if tables_to_skip:
        for ts in tables_to_skip():
            [table_names.remove(tbl) for tbl in job.dynamodb.tables.iterator() if ts == tbl]

    return table_names


def run_job(dynamodb_job):
    """Worker function to index each document in each table in the database."""
    job = dynamodb_job
    job.connect_to_zmq()
    job.connect_to_database()
    tables = get_tables(job)

    for table_name in tables:
        table = job.dynamodb.Table(table_name)
        has_start_key = True
        if not table.item_count == 0:
            i = 0
            increment = job.get_increment(table.item_count)
            scan = table.scan()
            items = scan['Items']
            while has_start_key:
                for item in items:
                    field_names = item.keys()
                    field_types = dict((k, type(v)) for k, v in item.items())
                    field_values = item.values()

                    entry = {}

                    # Map fields to voyager fields and to those specified in the configuration.
                    mapped_fields = job.map_fields (table.name, field_names, field_types)
                    mapped_fields = dict(zip(mapped_fields, field_values))

                    # TODO: Fetch geographic information from mapped fields.
                    geo = {}
                    if 'geo' in mapped_fields:
                        geo['wkt'] = mapped_fields['geo']

                    mapped_fields['_discoveryID'] = job.discovery_id
                    mapped_fields['title'] = table.name
                    mapped_fields['format_type'] = 'Record'
                    mapped_fields['format'] = 'application/vnd.dynamodb.record'
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                    job.send_entry(entry)
                    i += 1
                    if (i % increment) == 0:
                        status_writer.send_percent(float(i) / table.item_count,
                                                   '{0}: {1:.2f}%'.format(table.name, float(i) / table.item_count),
                                                   'DynamoDB')

                # Fetch all other items remaining in the table.
                if 'LastEvaluatedKey' not in scan:
                    has_start_key = False
                else:
                    scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
                    items = scan['Items']
