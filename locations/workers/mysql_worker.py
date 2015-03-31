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
import re
import decimal
import json
from utils import status

status_writer = status.Writer()


class ComplexEncoder(json.JSONEncoder):
    """To handle decimal types for json encoding."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def get_tables(job):
    """Return the list of tables to index (based on the user connected to the database)."""
    tables = []
    tables_to_skip = job.tables_to_skip()
    tables_to_keep = job.tables_to_keep()

    if not tables_to_keep == ['*']:
        for tk in tables_to_keep:
            statement = "select table_name from information_schema.tables where table_name like"
            [tables.append(t[0]) for t in job.db_cursor.execute("{0} '{1}'".format(statement, tk)).fetchall()]
    else:
        qry = "select table_name from information_schema.tables where table_schema='{0}'".format(job.sql_connection_info['connection']['database'])
        [tables.append(t[0]) for t in job.db_cursor.execute(qry).fetchall()]

    if tables_to_skip:
        for ts in tables_to_skip:
            statement = "select table_name from information_schema.tables where table_name like"
            [tables.remove(t[0]) for t in job.db_cursor.execute("{0} '{1}'".format(statement, ts)).fetchall()]
    return tables


def run_job(mysql_job):
    """Worker function to index each row in each table in the MySQL database."""
    job = mysql_job
    job.connect_to_zmq()
    job.connect_to_database()
    tables = get_tables(job)

    for table in tables:
        geo = {}
        is_point = False
        has_shape = False
        if not job.fields_to_keep == ['*']:
            columns = []
            column_types = {}
            geo = {}
            for col in job.fields_to_keep:
                qry = "select column_name, data_type from information_schema.columns where table_name = '{0}' and column_name like '{1}'".format(table, col)
                for col in job.execute_query(qry).fetchall():
                    columns.append(col[0])
                    column_types[col[0]] = col[1]
        else:
            columns = []
            column_types = {}
            for col in job.db_cursor.columns(table=table).fetchall():
                columns.append(col.column_name)
                column_types[col.column_name] = col.type_name

        if job.fields_to_skip:
            for col in job.fields_to_skip:
                qry = "select column_name from information_schema.columns where table_name = '{0}' AND column_name like '{1}'".format(table, col)
                [columns.remove(col[0]) for col in job.execute_query(qry).fetchall()]

        query = job.get_table_query(table)
        constraint = job.get_table_constraint(table)
        if query and constraint:
            expression = """{0} AND {1}""".format(query, constraint)
        else:
            if query:
                expression = query
            else:
                expression = constraint

        # Check for a geometry column and pull out X,Y for points and extent coordinates for other geometry types.
        for col in job.db_cursor.columns(table=table).fetchall():
            if col.type_name == 'geometry':
                has_shape = True
                srid = job.db_cursor.execute("select SRID({0}) from {1}".format(col.column_name, table)).fetchone()[0]
                geo['code'] = srid
                geom_type = job.db_cursor.execute("select GeometryType({0}) from {1}".format(col.column_name, table)).fetchone()[0]
                if geom_type == 'POINT':
                    is_point = True
                    columns.insert(0, "X({0})".format(col.column_name))
                    columns.insert(0, "Y({0})".format(col.column_name))
                    columns.insert(0, "AsText({0})".format(col.column_name))
                else:
                    columns.insert(0, "AsText(Envelope({0}))".format(col.column_name))
                columns.remove(col.column_name)
                column_types.pop(col.column_name)
                break

        # Query the table for the rows.
        if not expression:
            rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), table))
        else:
            rows = job.db_cursor.execute("select {0} from {1} where {2}".format(','.join(columns), table, expression))

        # Index each row in the table.
        entry = {}
        location_id = job.location_id
        discovery_id = job.discovery_id
        action_type = job.action_type
        mapped_fields = job.map_fields(table, columns, column_types)
        row_count = float(rows.rowcount)
        increment = job.get_increment(row_count)
        for i, row in enumerate(rows):
            if has_shape:
                if job.include_wkt:
                    geo['wkt'] = row[0]
                if is_point:
                    geo['lon'] = row[2]
                    geo['lat'] = row[1]
                    #mapped_cols = job.map_fields(table, columns, column_types)
                    mapped_cols = dict(zip(mapped_fields[3:], row[3:]))
                    mapped_cols['geometry_type'] = 'Point'
                else:
                    nums = re.findall("-?(?:\.\d+|\d+(?:\.\d*)?)", row[0].rpartition(',')[0])
                    geo['xmin'] = float(nums[0])
                    geo['ymin'] = float(nums[1])
                    geo['xmax'] = float(nums[4])
                    geo['ymax'] = float(nums[5])
                    #mapped_cols = job.map_fields(table, columns, column_types)
                    mapped_cols = dict(zip(mapped_fields[1:], row[1:]))
                    if 'POLYGON' in geom_type:
                        mapped_cols['geometry_type'] = 'Polygon'
                    else:
                        mapped_cols['geometry_type'] = 'Polyline'
            else:
                #mapped_cols = job.map_fields(table, columns, column_types)
                mapped_cols = dict(zip(mapped_fields, row))

            # Create an entry to send to ZMQ for indexing.
            mapped_cols['title'] = table
            entry['id'] = '{0}_{1}_{2}'.format(location_id, table, i)
            entry['location'] = location_id
            entry['action'] = action_type
            entry['entry'] = {'geo': geo, 'fields': mapped_cols}
            entry['entry']['fields']['_discoveryID'] = discovery_id
            job.send_entry(entry)
            if (i % increment) == 0:
                status_writer.send_percent(i / row_count, '{0}: {1:%}'.format(table, i / row_count), 'MySql')
