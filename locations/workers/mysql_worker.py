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
import random
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
    processed = 0
    mysql_entry = {}
    mysql_links = []

    for table in tables:
        geo = {}
        is_point = False
        has_shape = False

        # --------------------------------------------------------------------------------------------------
        # Get the table schema.
        # --------------------------------------------------------------------------------------------------
        schema = {}
        # Get the primary key.
        qry = "SELECT column_name FROM information_schema.table_constraints JOIN information_schema.key_column_usage USING (CONSTRAINT_NAME, TABLE_NAME) WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND table_name = '{0}'".format(table)
        cols = job.execute_query(qry).fetchall()
        primary_key = ''
        if cols:
            primary_key = cols[0][0]

        # Get the columns which are indexed.
        qry = "SELECT *  FROM information_schema.statistics WHERE table_name = '{0}'".format(table)
        cols = job.execute_query(qry).fetchall()
        indexed_cols = []
        if cols:
            for col in cols:
                indexed_cols.append(col.COLUMN_NAME)

        schema_columns = []
        for col in job.db_cursor.columns(table=table).fetchall():
            column = {}
            props = []
            column['name'] = col.column_name
            column['type'] = col.type_name
            if col.column_name == primary_key:
                props.append('PRIMARY KEY')
            if col.column_name in indexed_cols:
                props.append('INDEXED')
            if col.is_nullable == 'YES':
                props.append('NULLABLE')
            else:
                props.append('NOTNULLABLE')
            column['properties'] = props
            schema_columns.append(column)
        schema['fields'] = schema_columns
        schema['name'] = table

        # --------------------------------------------------------------------------------------------------
        # Set up the fields to index.
        # --------------------------------------------------------------------------------------------------
        columns = []
        column_types = {}
        if not job.fields_to_keep == ['*']:
            for col in job.fields_to_keep:
                qry = "select column_name, data_type from information_schema.columns where table_name = '{0}' and column_name like '{1}'".format(table, col)
                for col in job.execute_query(qry).fetchall():
                    columns.append(col[0])
                    column_types[col[0]] = col[1]
        else:
            for col in job.db_cursor.columns(table=table).fetchall():
                columns.append(col.column_name)
                column_types[col.column_name] = col.type_name

        if job.fields_to_skip:
            for col in job.fields_to_skip:
                qry = "select column_name from information_schema.columns where table_name = '{0}' AND column_name like '{1}'".format(table, col)
                [columns.remove(col[0]) for col in job.execute_query(qry).fetchall()]

        # ------------------------------------------------------------------------------------------------
        # Get the query information.
        # ------------------------------------------------------------------------------------------------
        query = job.get_table_query(table)
        constraint = job.get_table_constraint(table)
        if query and constraint:
            expression = """{0} AND {1}""".format(query, constraint)
        else:
            if query:
                expression = query
            else:
                expression = constraint

        # --------------------------------------------------------------------------------------------------------
        # Check for a geometry column and pull out X,Y for points and extent coordinates for other geometry types.
        # --------------------------------------------------------------------------------------------------------
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

        # ------------------------------
        # Query the table for the rows.
        # ------------------------------
        if not expression:
            rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), table))
        else:
            rows = job.db_cursor.execute("select {0} from {1} where {2}".format(','.join(columns), table, expression))

        # --------------------------------------
        # Remove shape columns from field list.
        # --------------------------------------
        for x in ("X({0})".format(col.column_name), "Y({0})".format(col.column_name), "AsText({0})".format(col.column_name)):
            try:
                columns.remove(x)
            except ValueError:
                continue

        # -----------------------------
        # Index each row in the table.
        # -----------------------------
        entry = {}
        location_id = job.location_id
        discovery_id = job.discovery_id
        action_type = job.action_type
        mapped_fields = job.map_fields(table, columns, column_types)
        new_fields = job.new_fields
        row_count = float(rows.rowcount)
        increment = job.get_increment(row_count)
        geometry_ops = worker_utils.GeometryOps()
        generalize_value = job.generalize_value

        # Add an entry for the table itself with schema.
        table_entry = {}
        table_entry['id'] = '{0}_{1}'.format(location_id, table)
        table_entry['location'] = location_id
        table_entry['action'] = action_type
        table_entry['relation'] = 'contains'
        table_entry['entry'] = {'fields': {'format': 'schema', 'format_type': 'Schema',
                                           '_discoveryID': discovery_id, 'name': table, 'fi_rows': int(row_count)}}
        table_entry['entry']['fields']['schema'] = schema
        mysql_links.append(table_entry)
        if job.schema_only:
            job.send_entry(table_entry)
            continue

        if not job.schema_only:
            table_links = []
            for i, row in enumerate(rows):
                if has_shape:
                    if is_point:
                        if job.include_wkt:
                            geo['wkt'] = row[0]
                        geo['lon'] = row[2]
                        geo['lat'] = row[1]
                        mapped_cols = dict(zip(mapped_fields[0:], row[3:]))
                        mapped_cols['geometry_type'] = 'Point'
                        for nf in new_fields:
                            if nf['name'] == '*' or nf['name'] == table:
                                for k, v in nf['new_fields'].iteritems():
                                    mapped_fields[k] = v
                    else:
                        if job.include_wkt:
                            if generalize_value == 0:
                                geo['wkt'] = row[0]
                            else:
                                geo['wkt'] = geometry_ops.generalize_geometry(str(row[0]), generalize_value)
                        nums = re.findall("-?(?:\.\d+|\d+(?:\.\d*)?)", row[0].rpartition(',')[0])
                        geo['xmin'] = float(nums[0])
                        geo['ymin'] = float(nums[1])
                        geo['xmax'] = float(nums[4])
                        geo['ymax'] = float(nums[5])
                        mapped_cols = dict(zip(mapped_fields[1:], row[1:]))
                        for nf in new_fields:
                            if nf['name'] == '*' or nf['name'] == table:
                                for k, v in nf['new_fields'].iteritems():
                                    mapped_fields[k] = v
                        if 'POLYGON' in geom_type:
                            mapped_cols['geometry_type'] = 'Polygon'
                        else:
                            mapped_cols['geometry_type'] = 'Polyline'
                else:
                    mapped_cols = dict(zip(mapped_fields, row))
                    for nf in new_fields:
                            if nf['name'] == '*' or nf['name'] == table:
                                for k, v in nf['new_fields'].iteritems():
                                    mapped_fields[k] = v

                # Create an entry to send to ZMQ for indexing.
                mapped_cols['format_type'] = 'Record'
                mapped_cols['format'] = 'application/vnd.mysql.record'
                entry['id'] = '{0}_{1}_{2}'.format(location_id, table, i)
                entry['location'] = location_id
                entry['action'] = action_type
                entry['entry'] = {'geo': geo, 'fields': mapped_cols}
                entry['entry']['fields']['_discoveryID'] = discovery_id
                job.send_entry(entry)
                table_links.append({'relation': 'contains', 'id': entry['id']})
                if (i % increment) == 0:
                    status_writer.send_percent(i / row_count, '{0}: {1:%}'.format(table, i / row_count), 'MySql')
            processed += i
            table_entry['entry']['links'] = table_links
            job.send_entry(table_entry)

    mysql_properties = {}
    mysql_entry['id'] = job.location_id + str(random.randint(0, 1000))
    mysql_entry['location'] = job.location_id
    mysql_entry['action'] = job.action_type
    mysql_properties['_discoveryID'] = job.discovery_id
    mysql_properties['name'] = job.sql_connection_info['connection']['database']
    mysql_properties['fs_driver'] = job.sql_connection_info['connection']['driver']
    mysql_properties['fs_server'] = job.sql_connection_info['connection']['server']
    mysql_properties['fs_database'] = job.sql_connection_info['connection']['database']
    mysql_properties['format'] = 'MySQL Database'
    mysql_entry['entry'] = {'fields': mysql_properties}
    mysql_entry['entry']['links'] = mysql_links
    job.send_entry(mysql_entry)

    status_writer.send_status("Processed: {0}".format(processed))
