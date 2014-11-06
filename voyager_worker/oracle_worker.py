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
    """Worker function to index each row in each table in the database.

    Example select statements:
    #cur.execute("select spatial_column FROM layers where table_name = 'FC_COUNTRIES'")
    #cur.execute("select column_name from all_tab_cols where table_name = 'FC_COUNTRIES' AND column_name like 'CNTRY%'")

    """
    status_writer = status.Writer()
    job.connect_to_zmq()
    job.connect_to_database()
    tables = []

    # Create the list of tables to index based on the user connected to the database.
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
            statement = "select table_name, owner from sde.layers where table_name like '{0}' and owner = '{1}'".format(lk[0], lk[1])
            [tables.append(l) for l in job.db_cursor.execute(statement).fetchall()]
    else:
        try:
            # If there is no owner, catch the error and continue.
            owner = job.layers_to_keep[0][1]
            statement = "select table_name, owner from sde.layers where owner = '{0}'".format(owner)
            [tables.append(l) for l in job.db_cursor.execute(statement).fetchall()]
        except IndexError:
            pass

    # Remove any layers/views from the list meant to be excluded.
    if job.layers_to_skip:
        for lk in job.layers_to_skip:
            statement = "select table_name, owner from sde.layers where owner = '{0}'".format(lk[1])
            [tables.remove(l) for l in job.db_cursor.execute(statement).fetchall()]


    # Begin indexing.
    for tbl in set(tables):
        geo = {}
        has_shape = False
        is_point = False
        columns = []
        column_types = {}

        # Check if the table is a layer/view and set name as "owner.tablename".
        if isinstance(tbl, tuple):
            tbl = "{0}.{1}".format(tbl[1], tbl[0])

        # Create the list of columns to include in the index.
        if not job.fields_to_keep == ['*']:
            for col in job.fields_to_keep:
                qry = "SELECT COLUMN_NAME FROM all_tab_cols WHERE table_name = '{0}' AND column_name LIKE '{1}'".format(tbl, col)
                for c in job.execute_query(qry).fetchall():
                    columns.append(c[0])
                    column_types[c[0]] = c[1]
        else:
            job.db_cursor.execute("SELECT * FROM {0}".format(tbl))
            # Get column types -- needed to map fields to voyager fields.
            for c in job.db_cursor.description:
                columns.append(c[0])
                column_types[c[0]] = c[1]

        # Remove fields meant to be excluded.
        if job.fields_to_skip:
            for col in job.fields_to_skip:
                qry = "SELECT COLUMN_NAME FROM all_tab_cols WHERE table_name = '{0}' AND column_name LIKE '{1}'".format(tbl, col)
                [columns.remove(c[0]) for c in job.execute_query(qry).fetchall()]

        # If there is a shape column, get the geographic information.
        if 'SHAPE' in columns:
            has_shape = True
            columns.remove('SHAPE')
            if job.db_cursor.execute("select SHAPE from {0}".format(tbl)).fetchone() == None:
                status_writer.send_status("Skipping {0} - no records.".format(tbl))
                continue
            else:
                schema = job.db_cursor.execute("select SHAPE from {0}".format(tbl)).fetchone()[0].type.schema

            shape_type = job.db_cursor.execute("select {0}.ST_GEOMETRYTYPE(SHAPE) from {1}".format(schema, tbl)).fetchone()[0]
            geo['code'] = int(job.db_cursor.execute("select {0}.ST_SRID(SHAPE) from {1}".format(schema, tbl)).fetchone()[0])
            if 'POINT' in shape_type:
                is_point = True
                if geo['code'] == 4326:
                    for x in ('y', 'x', 'astext'):
                        columns.insert(0, '{0}.st_{1}(SHAPE)'.format(schema, x))
                else:
                    for x in ('y', 'x', 'astext'):
                        columns.insert(0, '{0}.st_{1}({0}.st_transform(SHAPE, 4326))'.format(schema, x))
            else:
                if geo['code'] == 4326:
                    for x in ('maxy', 'maxx', 'miny', 'minx', 'astext'):
                        columns.insert(0, '{0}.st_{1}(SHAPE)'.format(schema, x))
                else:
                    try:
                        job.db_cursor.execute("select {0}.st_maxy(SDE.st_transform(SHAPE, 4326)) from {1}".format(schema, tbl))
                        for x in ('maxy', 'maxx', 'miny', 'minx', 'astext'):
                            columns.insert(0, '{0}.st_{1}({0}.st_transform(SHAPE, 4326))'.format(schema, x))
                    except Exception:
                        for x in ('maxy', 'maxx', 'miny', 'minx', 'astext'):
                            columns.insert(0, '{0}.st_{1}(SHAPE)'.format(schema, x))


        # Select all the rows.
        try:
            rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl)).fetchall()
        except Exception:
            # This can occur for ST_GEOMETRY when spatial operators are un-available (See: http://tinyurl.com/lvvhwyl)
            columns.pop(0)
            geo['wkt'] = None
            rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl)).fetchall()

        # Continue if the table has zero records.
        if not rows:
            status_writer.send_status("Skipping {0} - no records.".format(tbl))
            continue

        # Index each row.
        increment = job.get_increment(len(rows))
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
                        geo['xmin'] = row[0]
                        geo['ymin'] = row[1]
                        geo['xmax'] = row[2]
                        geo['ymax'] = row[3]

            mapped_cols = job.map_fields(tbl, columns, column_types)
            mapped_cols = dict(zip(mapped_cols, row))

            if has_shape:
                shape_columns = [name for name in mapped_cols.keys() if '(SHAPE)' in name]
                for x in shape_columns:
                    mapped_cols.pop(x)

            mapped_cols['_discoveryID'] = job.discovery_id
            entry['id'] = '{0}_{1}_{2}'.format(job.location_id, tbl, i)
            entry['location'] = job.location_id
            entry['action'] = job.action_type
            entry['entry'] = {'geo': geo, 'fields': mapped_cols}
            job.send_entry(entry)
            i += 1
            if (i % increment) == 0:
                status_writer.send_percent(float(i)/len(rows),
                                           "{0}: {1:%}".format(tbl, float(i)/len(rows)),
                                           'oracle_worker')


def assign_job(job_info):
    """Connects to ZMQ, connects to the database, and assigns the job."""
    job = base_job.Job(job_info)
    global_job(job)
    worker()
