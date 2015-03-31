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
    """Return the list of tables to index (based on the user connected to the database)."""
    tables = []
    tables_to_skip = job.tables_to_skip()
    tables_to_keep = job.tables_to_keep()

    if not tables_to_keep == ['*']:
        for tk in tables_to_keep:
            statement = "select * from sys.objects where name like"
            [tables.append(t[0]) for t in job.db_cursor.execute("{0} '{1}'".format(statement, tk)).fetchall()]
    else:
        [tables.append(t[0]) for t in job.db_cursor.execute("select name from sys.objects where type='U'").fetchall()]

    if tables_to_skip:
        for ts in tables_to_skip:
            statement = "select * from sys.objects where name like"
            [tables.remove(t[0]) for t in job.db_cursor.execute("{0} '{1}'".format(statement, ts)).fetchall()]

    return tables


def run_job(sql_job):
    """Worker function to index each row in each table in the database."""
    job = sql_job
    job.connect_to_zmq()
    job.connect_to_database()
    tables = get_tables(job)

    for tbl in set(tables):
        geo = {}
        has_shape = False
        is_point = False

        query = job.get_table_query(tbl)
        constraint = job.get_table_constraint(tbl)
        if query and constraint:
            expression = """{0} AND {1}""".format(query, constraint)
        else:
            if query:
                expression = query
            else:
                expression = constraint

        if not job.fields_to_keep == ['*']:
            columns = []
            column_types = {}
            for col in job.fields_to_keep:
                qry = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}' AND column_name LIKE '{1}'".format(tbl, col)
                for c in job.execute_query(qry).fetchall():
                    columns.append(c[0])
                    column_types[c[0]] = c[1]
        else:
            columns = []
            column_types = {}
            for c in job.db_cursor.columns(table=tbl).fetchall():
                columns.append(c.column_name)
                column_types[c.column_name] = c.type_name

        if job.fields_to_skip:
            for col in job.fields_to_skip:
                qry = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}' AND column_name LIKE '{1}'".format(tbl, col)
                [columns.remove(c[0]) for c in job.execute_query(qry).fetchall()]

        # Check for a geometry column and pull out X,Y for points and extent coordinates for other geometry types.
        geom_type = ''
        for c in job.db_cursor.columns(table=tbl).fetchall():
            if c.type_name == 'geometry':
                has_shape = True
                srid = job.db_cursor.execute("select {0}.STSrid from {1}".format(c.column_name, tbl)).fetchone()[0]
                geo['code'] = srid
                geom_type = job.db_cursor.execute("select {0}.STGeometryType() from {1}".format(c.column_name, tbl)).fetchone()[0]
                if geom_type == 'Point':
                    is_point = True
                    columns.insert(0, "{0}.STPointN(1).STX".format(c.column_name))
                    columns.insert(0, "{0}.STPointN(1).STY".format(c.column_name))
                    columns.insert(0, "{0}.STAsText() as WKT".format(c.column_name))
                else:
                    columns.insert(0, "{0}.STEnvelope().STPointN((3)).STY".format(c.column_name))
                    columns.insert(0, "{0}.STEnvelope().STPointN((3)).STX".format(c.column_name))
                    columns.insert(0, "{0}.STEnvelope().STPointN((1)).STY".format(c.column_name))
                    columns.insert(0, "{0}.STEnvelope().STPointN((1)).STX".format(c.column_name))
                    columns.insert(0, "{0}.STAsText() as WKT".format(c.column_name))
                columns.remove(c.column_name)
                break

        # Query the table for the rows.
        if not expression:
            row_count = float(job.db_cursor.execute("select Count(*) from {0}".format(tbl)).fetchone()[0])
            rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl))
        else:
            row_count = float(job.db_cursor.execute("select Count(*) from {0} where {1}".format(tbl, expression)).fetchone()[0])
            rows = job.db_cursor.execute("select {0} from {1} where {2}".format(','.join(columns), tbl, expression))

        # Index each row in the table.
        entry = {}
        action_type = job.action_type
        discovery_id = job.discovery_id
        location_id = job.location_id
        if job.field_mapping:
            mapped_fields = job.map_fields(tbl, columns, column_types)
        else:
            mapped_fields = columns

        increment = job.get_increment(row_count)
        geometry_ops = worker_utils.GeometryOps()
        generalize_value = job.generalize_value
        for i, row in enumerate(rows):
            if has_shape:
                if is_point:
                    if job.include_wkt:
                        geo['wkt'] = row[0]
                    geo['lon'] = row[2]
                    geo['lat'] = row[1]
                    mapped_cols = dict(zip(mapped_fields[3:], row[3:]))
                    mapped_cols['geometry_type'] = 'Point'
                else:
                    if job.include_wkt:
                        if generalize_value == 0:
                            geo['wkt'] = row[0]
                        else:
                            geo['wkt'] = geometry_ops.generalize_geometry(str(row[0]), generalize_value)
                    geo['xmin'] = row[1]
                    geo['ymin'] = row[2]
                    geo['xmax'] = row[3]
                    geo['ymax'] = row[4]
                    mapped_cols = dict(zip(mapped_fields[5:], row[5:]))
                    if 'Polygon' in geom_type:
                        mapped_cols['geometry_type'] = 'Polygon'
                    else:
                        mapped_cols['geometry_type'] = 'Polyline'
            else:
                mapped_cols = dict(zip(mapped_fields, row))

            # Create an entry to send to ZMQ for indexing.
            mapped_cols['title'] = tbl
            entry['id'] = '{0}_{1}_{2}'.format(location_id, tbl, i)
            entry['location'] = location_id
            entry['action'] = action_type
            entry['entry'] = {'geo': geo, 'fields': mapped_cols}
            entry['entry']['fields']['_discoveryID'] = discovery_id
            job.send_entry(entry)
            if (i % increment) == 0:
                status_writer.send_percent(i / row_count, '{0}: {1:%}'.format(tbl, i / row_count), 'sql_server')
