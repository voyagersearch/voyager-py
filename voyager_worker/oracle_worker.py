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
    """Create a global job object for multiprocessing. (NOT YET SUPPORTED HERE)"""
    global job
    job = args


def worker():
    """Worker function to index each row in each table in the database."""
    status_writer = status.Writer()
    job.connect_to_zmq()
    job.connect_to_database()
    tables = []

    # Create the list of tables to index (based on the user connected to the database).
    if '*' in job.tables_to_skip:
        pass
    elif not job.tables_to_keep == ['*']:
        for tk in job.tables_to_keep:
            statement = "select table_name from user_tables where table_name like '{0}'".format(tk)
            [tables.append(t[0]) for t in job.db_cursor.execute(statement).fetchall()]
    else:
        [tables.append(t[0]) for t in job.db_cursor.execute("select table_name from user_tables").fetchall()]

    # Remove any tables from the list meant to be excluded.
    if job.tables_to_skip:
        for tk in job.tables_to_skip:
            statement = "select table_name from user_tables where table_name like '{0}'".format(tk)
            [tables.remove(t[0]) for t in job.db_cursor.execute(statement).fetchall()]

    # Create the list of layers/views to index based on Owner.
    if '*' in job.layers_to_skip:
        pass
    elif not job.layers_to_keep[0][0] == '*':
        for lk in job.layers_to_keep:
            if job.sql_schema:
                statement = "select table_name, owner from {0}.layers where table_name like '{1}' and owner = '{2}'".format(job.sql_schema, lk[0], lk[1].upper())
            else:
                statement = "select table_name, owner from sde.layers where table_name like '{0}' and owner = '{1}'".format(lk[0], lk[1].upper())
            [tables.append(l) for l in job.db_cursor.execute(statement).fetchall()]
    else:
        try:
            # If there is no owner, catch the error and continue.
            owner = job.layers_to_keep[0][1]
            if job.sql_schema:
                statement = "select table_name, owner from {0}.layers where owner = '{1}'".format(job.sql_schema, owner.upper())
            else:
                statement = "select table_name, owner from sde.layers where owner = '{0}'".format(owner.upper())
            [tables.append(l) for l in job.db_cursor.execute(statement).fetchall()]
        except IndexError:
            pass

    # Remove any layers/views from the list meant to be excluded.
    if '*' not in job.layers_to_skip:
        for lk in job.layers_to_skip:
            statement = "select table_name, owner from sde.layers where owner = '{0}'".format(lk[1])
            [tables.remove(l) for l in job.db_cursor.execute(statement).fetchall()]

    if not tables:
        status_writer.send_state(status.STAT_FAILED, "No tables or views found.")
        return

    # Begin indexing.
    for tbl in set(tables):
        geo = {}
        columns = []
        column_types = {}
        is_point = False
        has_shape = False
        geometry_field = None
        shape_type = None

        # Check if the table is a layer/view and set name as "owner.table".
        if isinstance(tbl, tuple):
            column_query = "select column_name, data_type from all_tab_cols where " \
                           "table_name = '{0}' and column_name like".format(tbl[0])
            tbl = "{0}.{1}".format(tbl[1], tbl[0])
        else:
            column_query = "select column_name, data_type from all_tab_cols where " \
                           "table_name = '{0}' and column_name like".format(tbl)

        # Create the list of columns and column types to include in the index.
        if not job.fields_to_keep == ['*']:
            for col in job.fields_to_keep:
                for c in job.db_cursor.execute("{0} '{1}'".format(column_query, col)).fetchall():
                    columns.append(c[0])
                    column_types[c[0]] = c[1]
                    if c[1] in ('SDO_GEOMETRY', 'ST_GEOMETRY'):
                        has_shape = True
                        geometry_field = c[0]
                        geometry_type = c[1]
        else:
            for i, c in enumerate(job.db_cursor.execute("select * from {0}".format(tbl)).description):
                columns.append(c[0])
                column_types[c[0]] = c[1]
                try:
                    if c[1] in ('SDO_GEOMETRY', 'ST_GEOMETRY'):
                        has_shape = True
                        geometry_field = c[0]
                        geometry_type = c[1]
                    elif job.db_cursor.fetchvars[i].type.name == 'ST_GEOMETRY':
                        has_shape = True
                        geometry_field = c[0]
                        geometry_type = 'ST_GEOMETRY'
                    elif job.db_cursor.fetchvars[i].type.name == 'SDO_GEOMETRY':
                        has_shape = True
                        geometry_field = c[0]
                        geometry_type = 'SDO_GEOMETRY'
                except AttributeError:
                    continue

        # Remove fields meant to be excluded.
        if job.fields_to_skip:
            for col in job.fields_to_skip:
                [columns.remove(c[0]) for c in job.execute_query("{0} '{1}'".format(column_query, col)).fetchall()]

        # If there is a shape column, get the geographic information.
        if geometry_field:
            columns.remove(geometry_field)

            if job.db_cursor.execute("select SHAPE from {0}".format(tbl)).fetchone() is None:
                status_writer.send_status("Skipping {0} - no records.".format(tbl))
                continue
            else:
                schema = job.db_cursor.execute("select SHAPE from {0}".format(tbl)).fetchone()[0].type.schema

            # Figure out if geometry type is ST or SDO.
            if geometry_type == 'SDO_GEOMETRY':
                geo['code'] = job.db_cursor.execute("select c.{0}.SDO_SRID from {1} c".format(geometry_field, tbl)).fetchone()[0]
                dimension = job.db_cursor.execute("select c.shape.Get_Dims() from {0} c".format(tbl)).fetchone()[0]
                if not job.db_cursor.execute("select c.{0}.SDO_POINT from {1} c".format(geometry_field, tbl)).fetchone()[0] is None:
                    is_point = True
                    if geo['code'] == 4326:
                        columns.insert(0, '{0}.{1}.SDO_POINT.Y'.format(schema, geometry_field))
                        columns.insert(0, '{0}.{1}.SDO_POINT.X'.format(schema, geometry_field))
                    else:
                        job.db_cursor.execute("SDO_CS.TRANSFORM({0}.{1}, 4326)".format(schema, geometry_field))
                else:
                    columns.insert(0, "sdo_geom.sdo_mbr({0}).sdo_ordinates".format(geometry_field))

            else:  # ST_GEOMETRY
                shape_type = job.db_cursor.execute("select {0}.ST_GEOMETRYTYPE({1}) from {2}".format(schema, geometry_field, tbl)).fetchone()[0]
                geo['code'] = int(job.db_cursor.execute("select {0}.ST_SRID({1}) from {2}".format(schema, geometry_field, tbl)).fetchone()[0])
                if 'POINT' in shape_type:
                    is_point = True
                    if geo['code'] == 4326 or geo['code'] == 3:
                        for x in ('y', 'x', 'astext'):
                            columns.insert(0, '{0}.st_{1}({2})'.format(schema, x, geometry_field))
                    else:
                        for x in ('y', 'x', 'astext'):
                            columns.insert(0, '{0}.st_{1}({0}.st_transform({2}, 4326))'.format(schema, x, geometry_field))
                else:
                    if geo['code'] == 4326:
                        for x in ('maxy', 'maxx', 'miny', 'minx', 'astext'):
                            columns.insert(0, '{0}.st_{1}({2})'.format(schema, x, geometry_field))
                    else:
                        try:
                            job.db_cursor.execute("select {0}.st_maxy(SDE.st_transform({1}, 4326)) from {2}".format(schema, geometry_field, tbl))
                            for x in ('maxy', 'maxx', 'miny', 'minx', 'astext'):
                                columns.insert(0, '{0}.st_{1}({0}.st_transform({2}, 4326))'.format(schema, x, geometry_field))
                        except Exception:
                            for x in ('maxy', 'maxx', 'miny', 'minx', 'astext'):
                                columns.insert(0, '{0}.st_{1}({2})'.format(schema, x, geometry_field))

        # Drop astext from columns if WKT is not requested.
        if not job.include_wkt:
            columns.pop(0)

        # Get the count of all the rows to use for reporting progress.
        row_count = job.db_cursor.execute("select count(*) from {0}".format(tbl)).fetchall()[0][0]
        if row_count == 0 or row_count is None:
            continue

        # Get the rows.
        try:
            if geometry_type == 'SDO_GEOMETRY':
                rows = job.db_cursor.execute("select {0} from {1} {2}".format(','.join(columns), tbl, schema))
            else:
                # Quick check to ensure ST_GEOMETRY operations are supported.
                row = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl)).fetchone()
                del row
                rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl))
        except Exception:
            # This can occur for ST_GEOMETRY when spatial operators are un-available (See: http://tinyurl.com/lvvhwyl)
            columns.pop(0)
            geo['wkt'] = None
            rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl))

        # Continue if the table has zero records.
        if not rows:
            status_writer.send_status("Skipping {0} - no records.".format(tbl))
            continue

        # Index each row.
        increment = job.get_increment(row_count)
        for i, row in enumerate(rows):
            entry = {}
            if has_shape:
                if job.include_wkt:
                    geo['wkt'] = row[0]
                    if is_point:
                        geo['lon'] = row[1]
                        geo['lat'] = row[2]
                    else:
                        geo['xmin'] = row[1]
                        geo['ymin'] = row[2]
                        geo['xmax'] = row[3]
                        geo['ymax'] = row[4]
                else:
                    if is_point:
                        geo['lon'] = row[0]
                        geo['lat'] = row[1]
                    else:
                        if geometry_type == 'SDO_GEOMETRY':
                            if dimension == 3:
                                geo['xmin'] = row[0][0]
                                geo['ymin'] = row[0][1]
                                geo['xmax'] = row[0][3]
                                geo['ymax'] = row[0][4]
                            elif dimension == 2:
                                geo['xmin'] = row[0][0]
                                geo['ymin'] = row[0][1]
                                geo['xmax'] = row[0][2]
                                geo['ymax'] = row[0][3]
                        else:
                            geo['xmin'] = row[0]
                            geo['ymin'] = row[1]
                            geo['xmax'] = row[2]
                            geo['ymax'] = row[3]

            # Map column names to Voyager fields.
            mapped_cols = dict(zip(job.map_fields(tbl, columns, column_types), row))
            if has_shape:
                [mapped_cols.pop(name) for name in mapped_cols.keys() if '{0}'.format(geometry_field) in name]
                if is_point:
                    mapped_cols['geometry_type'] = 'Point'
                elif 'POLYGON' in shape_type:
                    mapped_cols['geometry_type'] = 'Polygon'
                else:
                    mapped_cols['geometry_type'] = 'Polyline'
            mapped_cols['_discoveryID'] = job.discovery_id
            mapped_cols['title'] = tbl
            entry['id'] = '{0}_{1}_{2}'.format(job.location_id, tbl, i)
            entry['location'] = job.location_id
            entry['action'] = job.action_type
            entry['entry'] = {'geo': geo, 'fields': mapped_cols}
            job.send_entry(entry)
            i += 1
            if (i % increment) == 0:
                status_writer.send_percent(float(i) / row_count,
                                           "{0}: {1:%}".format(tbl, float(i) / row_count),
                                           'oracle_worker')


def assign_job(job_info):
    """Connects to ZMQ, connects to the database, and assigns the job."""
    job = base_job.Job(job_info)
    global_job(job)
    worker()
