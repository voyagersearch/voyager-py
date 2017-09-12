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
import random
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
            statement = "select * from sys.objects where name like"
            [tables.append(t[0]) for t in job.db_cursor.execute("{0} '{1}'".format(statement, tk)).fetchall()]
    else:
        [tables.append(t[0]) for t in job.db_cursor.execute("select name from sys.objects where type='U'").fetchall()]

    if tables_to_skip:
        for ts in tables_to_skip:
            statement = "select * from sys.objects where name like"
            [tables.remove(t[0]) for t in job.db_cursor.execute("{0} '{1}'".format(statement, ts)).fetchall()]

    return tables


def run_job(job):
    """Worker function to index each row in each table in the database."""
    job.connect_to_zmq()
    job.connect_to_database()
    tables = get_tables(job)
    sql_links = []

    for tbl in set(tables):
        geo = {}
        has_shape = False
        is_point = False
        shape_field_name = ''

        # --------------------------------------------------------------------------------------------------
        # Get the table schema.
        # --------------------------------------------------------------------------------------------------
        schema = {}

        # Get the primary key.
        qry = "SELECT K.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C JOIN " \
              "INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON " \
              "C.TABLE_NAME = K.TABLE_NAME AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME WHERE " \
              "C.CONSTRAINT_TYPE = 'PRIMARY KEY' AND K.TABLE_NAME = '{0}'".format(tbl)
        cols = job.execute_query(qry).fetchall()
        primary_key = ''
        if cols:
            primary_key = cols[0][0]

        # Get the foreign key.
        qry = "SELECT K.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C JOIN " \
              "INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON " \
              "C.TABLE_NAME = K.TABLE_NAME AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME WHERE " \
              "C.CONSTRAINT_TYPE = 'FOREIGN KEY' AND K.TABLE_NAME = '{0}'".format(tbl)
        cols = job.execute_query(qry).fetchall()
        foreign_key = ''
        if cols:
            foreign_key = cols[0][0]

        # Get the columns that have indexes.
        qry = "SELECT COL_NAME(ic.object_id,ic.column_id) AS column_name " \
              "FROM sys.indexes AS i INNER JOIN sys.index_columns AS ic ON i.object_id = ic.object_id " \
              "AND i.index_id = ic.index_id WHERE i.object_id = OBJECT_ID('{0}')".format(tbl)
        cols = job.execute_query(qry).fetchall()
        indexed_cols = []
        if cols:
            for col in cols:
                indexed_cols.append(col[0])

        schema_columns = []
        for col in job.db_cursor.columns(table=tbl).fetchall():
            column = {}
            props = []
            column['name'] = col.column_name
            column['type'] = col.type_name
            if col.type_name == 'geometry':
                column['isGeo'] = True
                column['crs'] = job.db_cursor.execute("select {0}.STSrid from {1}".format(col.column_name, tbl)).fetchone()[0]
            if col.column_name == primary_key:
                props.append('PRIMARY KEY')
            if col.column_name == foreign_key:
                props.append('FOREIGN KEY')
            if col.column_name in indexed_cols:
                props.append('INDEXED')
            if col.is_nullable == 'YES':
                props.append('NULLABLE')
            else:
                props.append('NOTNULLABLE')
            column['properties'] = props
            schema_columns.append(column)
        schema['fields'] = schema_columns

        # --------------------------------
        # Get the list of columns to keep.
        # --------------------------------
        if not job.fields_to_keep == ['*']:
            columns = []
            column_types = {}
            for col in job.fields_to_keep:
                qry = "select column_name, data_type from INFORMATION_SCHEMA.columns where table_name = '{0}' and column_name like '{1}'".format(tbl, col)
                for c in job.execute_query(qry).fetchall():
                    if not c.type_name == 'geometry':
                        columns.append("{0}.{1}".format(tbl, c[0]))
                        column_types[c[0]] = c[1]
                    else:
                        shape_field_name = c.column_name
        else:
            columns = []
            column_types = {}
            for c in job.db_cursor.columns(table=tbl).fetchall():
                if not c.type_name == 'geometry':
                    columns.append("{0}.{1}".format(tbl, c.column_name))
                    column_types[c.column_name] = c.type_name
                else:
                    shape_field_name = c.column_name

        if job.fields_to_skip:
            for col in job.fields_to_skip:
                qry = "select column_name from INFORMATION_SCHEMA.columns where table_name = '{0}' and column_name like '{1}'".format(tbl, col)
                [columns.remove("{0}.{1}".format(tbl, c[0])) for c in job.execute_query(qry).fetchall()]

        # --------------------------------------------------------------------------------------------------------
        # Get the column names and types from the related tables.
        # --------------------------------------------------------------------------------------------------------
        related_columns = []
        if job.related_tables:
            for related_table in job.related_tables:
                for c in job.db_cursor.columns(table=related_table):
                    if not c.type_name == 'geometry':
                        related_columns.append("{0}.{1}".format(related_table, c.column_name))

        # --------------------------------------------------------------------------------------------------------
        # Check for a geometry column and pull out X,Y for points and extent coordinates for other geometry types.
        # --------------------------------------------------------------------------------------------------------
        geom_type = ''
        if shape_field_name:
            has_shape = True
            srid = job.db_cursor.execute("select {0}.STSrid from {1}".format(shape_field_name, tbl)).fetchone()[0]
            geo['code'] = srid
            geom_type = job.db_cursor.execute("select {0}.STGeometryType() from {1}".format(shape_field_name, tbl)).fetchone()[0]
            if geom_type == 'Point':
                is_point = True
                columns.insert(0, "{0}.{1}.STPointN(1).STX as X".format(tbl, shape_field_name))
                columns.insert(0, "{0}.{1}.STPointN(1).STY as Y".format(tbl, shape_field_name))
            else:
                columns.insert(0, "{0}.{1}.STEnvelope().STPointN((3)).STY as YMAX".format(tbl, shape_field_name))
                columns.insert(0, "{0}.{1}.STEnvelope().STPointN((3)).STX as XMAX".format(tbl, shape_field_name))
                columns.insert(0, "{0}.{1}.STEnvelope().STPointN((1)).STY as YMIN".format(tbl, shape_field_name))
                columns.insert(0, "{0}.{1}.STEnvelope().STPointN((1)).STX as XMIN".format(tbl, shape_field_name))
                columns.insert(0, "{0}.{1}.STAsText() as WKT".format(tbl, shape_field_name))

        # -----------------------------
        # Query the table for the rows.
        # -----------------------------
        sql_query = job.get_table_query(tbl)
        if not sql_query:
            row_count = float(job.db_cursor.execute("select Count(*) from {0}".format(tbl)).fetchone()[0])
            rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl))
        else:
            q = re.search('FROM(.*)', sql_query, re.IGNORECASE).group(0)
            try:
                row_count = float(job.db_cursor.execute("select Count(*) {0}".format(q)).fetchone()[0])
            except Exception:
                row_count = float(job.db_cursor.execute("select Count(*) {0}".format(q.split('ORDER BY')[0])).fetchone()[0])
            rows = job.execute_query("select {0} {1}".format(','.join(columns + related_columns), q))

        # -----------------------------------------------------------------------------
        # Index each row in the table. If there are relates, index the related records.
        # -----------------------------------------------------------------------------
        cur_id = -1
        entry = {}
        link = {}
        wkt_col = -1
        action_type = job.action_type
        discovery_id = job.discovery_id
        location_id = job.location_id
        columns = [c.split('.')[1] for c in columns]
        mapped_fields = job.map_fields(tbl, columns, column_types)
        increment = job.get_increment(row_count)
        if 'WKT' in columns:
            has_shape = True
            try:
                wkt_col = mapped_fields.index('fs_WKT')
            except ValueError:
                wkt_col = mapped_fields.index('WKT')
        geometry_ops = worker_utils.GeometryOps()
        generalize_value = job.generalize_value

        # -----------------------------------------------
        # Add an entry for the table itself with schema.
        # -----------------------------------------------
        mapped_cols = {}
        table_entry = {}
        table_entry['id'] = '{0}_{1}'.format(location_id, tbl)
        table_entry['location'] = location_id
        table_entry['action'] = action_type
        table_entry['relation'] = 'contains'
        table_entry['entry'] = {'fields': {'format': 'schema', 'format_type': 'Schema',
                                           '_discoveryID': discovery_id, 'name': tbl, 'fi_rows': int(row_count),
                                           'path': job.sql_server_connection_str}}
        table_entry['entry']['fields']['schema'] = schema
        sql_links.append(table_entry)
        if job.schema_only:
            job.send_entry(table_entry)
            continue
        else:
            job.send_entry(table_entry)

        for i, row in enumerate(rows):
            if not cur_id == row[0] or not job.related_tables:
                if entry:
                    try:
                        job.send_entry(entry)
                    except Exception as ex:
                        entry = {}
                        continue
                    entry = {}
                if has_shape:
                    if is_point:
                        geo['lon'] = row[1]
                        geo['lat'] = row[0]
                        mapped_cols = dict(zip(mapped_fields[2:], row[2:]))
                        mapped_cols['geometry_type'] = 'Point'
                    else:
                        if generalize_value == 0 or generalize_value == 0.0:
                            if wkt_col >= 0:
                                geo['wkt'] = row[wkt_col]
                                mapped_cols = dict(zip(mapped_fields, row))
                            else:
                                geo['wkt'] = row[0]
                        elif generalize_value > 0.9:
                            if wkt_col >= 0:
                                geo['wkt'] = row[wkt_col]
                                mapped_cols = dict(zip(mapped_fields, row))
                            else:
                                geo['xmin'] = row[1]
                                geo['ymin'] = row[2]
                                geo['xmax'] = row[3]
                                geo['ymax'] = row[4]
                        else:
                            if wkt_col >= 0:
                                geo['wkt'] = geometry_ops.generalize_geometry(str(row[wkt_col]), generalize_value)
                                mapped_cols = dict(zip(mapped_fields, row))
                            else:
                                geo['wkt'] = geometry_ops.generalize_geometry(str(row[0]), generalize_value)
                        if not mapped_cols:
                            mapped_cols = dict(zip(mapped_fields[5:], row[5:]))
                        if 'Polygon' in geom_type:
                            mapped_cols['geometry_type'] = 'Polygon'
                        elif 'Polyline' in geom_type:
                            mapped_cols['geometry_type'] = 'Polyline'
                        else:
                            mapped_cols['geometry_type'] = 'Point'
                else:
                    mapped_cols = dict(zip(mapped_fields, row))

                # Create an entry to send to ZMQ for indexing.
                mapped_cols['format_type'] = 'Record'
                mapped_cols['format'] = 'application/vnd.sqlserver.record'
                if 'id' in mapped_cols:
                    mapped_cols['id'] = '{0}{1}'.format(random.randint(0, 1000000), mapped_cols['id'])
                else:
                    mapped_cols['id'] = "{0}{1}".format(random.randint(0, 1000000), i)
                entry['id'] = '{0}_{1}_{2}'.format(location_id, tbl, i)
                entry['location'] = location_id
                entry['action'] = action_type

                # If the table supports relates/joins, handle them and add them as links.
                if job.related_tables:
                    links = []
                    related_field_names = [d[0] for d in row.cursor_description[len(columns):]]
                    related_field_types = dict(zip(related_field_names, [d[1] for d in row.cursor_description[len(columns):]]))
                    mapped_related_fields = []
                    for related_table in job.related_tables:
                        mapped_related_fields += job.map_fields(related_table, related_field_names, related_field_types)
                    link['relation'] = 'contains'
                    link = dict(zip(mapped_related_fields, row[len(columns):]))
                    try:
                        link['id'] = "{0}{1}".format(random.randint(0, 1000000), link['id'])
                    except KeyError:
                        link['id'] = "{0}{1}".format(random.randint(0, 1000000), i)

                    # Send this link as an entry and set extract to true.
                    link_entry = {}
                    link_entry['id'] = "{0}{1}".format(link['id'], location_id)
                    link_entry['action'] = action_type
                    link_entry['entry'] = {"fields": link}
                    if job.format:
                        link_entry['entry']['fields']['__to_extract'] = True
                    job.send_entry(link_entry)
                    # Append the link to a list that will be part of the main entry.
                    links.append(link)
                    if geo:
                        entry['entry'] = {'geo': geo, 'fields': mapped_cols, 'links': links}
                    else:
                        entry['entry'] = {'fields': mapped_cols, 'links': links}
                else:
                    if geo:
                        entry['entry'] = {'geo': geo, 'fields': mapped_cols}
                    else:
                        entry['entry'] = {'fields': mapped_cols}
                    entry['entry']['fields']['_discoveryID'] = discovery_id
                entry['entry']['fields']['_discoveryID'] = discovery_id
                cur_id = row[0]
            else:
                link['relation'] = 'contains'
                link = dict(zip(mapped_related_fields, row[len(columns):]))
                try:
                    link['id'] = "{0}{1}".format(random.randint(0, 1000000), link['id'])
                except KeyError:
                    link['id'] = "{0}{1}".format('0000', i)
                link_entry = {}
                link_entry['id'] = "{0}{1}".format(link['id'], location_id)
                link_entry['action'] = action_type
                link_entry['entry'] = {"fields": link}
                if job.format:
                    link_entry['entry']['fields']['__to_extract'] = True
                job.send_entry(link_entry)

                links.append(link)
                entry['entry']['links'] = entry['entry'].pop('links', links)

            # Report status percentage.
            if (i % increment) == 0:
                status_writer.send_percent(i / row_count, '{0}: {1:%}'.format(tbl, i / row_count), 'sql_server')

        # Send final entry.
        job.send_entry(entry)
        status_writer.send_percent(1, '{0}: {1:%}'.format(tbl, 1), 'sql_server')

    sql_entry = {}
    sql_properties = {}
    sql_entry['id'] = job.location_id + str(random.randint(0, 1000))
    sql_entry['location'] = job.location_id
    sql_entry['action'] = job.action_type
    sql_properties['_discoveryID'] = job.discovery_id
    sql_properties['name'] = job.sql_connection_info['connection']['database']
    sql_properties['fs_driver'] = job.sql_connection_info['connection']['driver']
    sql_properties['fs_server'] = job.sql_connection_info['connection']['server']
    sql_properties['fs_database'] = job.sql_connection_info['connection']['database']
    sql_properties['format'] = 'SQL Database'
    sql_entry['entry'] = {'fields': sql_properties}
    sql_entry['entry']['links'] = sql_links
    job.send_entry(sql_entry)
