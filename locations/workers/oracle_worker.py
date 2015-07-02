
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
from itertools import izip
import json
from utils import status
from utils import worker_utils
import cx_Oracle


status_writer = status.Writer()

field_types =  {cx_Oracle.STRING: 'STRING',
                cx_Oracle.FIXED_CHAR: 'CHAR',
                cx_Oracle.NUMBER: 'NUMBER',
                cx_Oracle.DATETIME: 'DATE',
                cx_Oracle.TIMESTAMP: 'TIMESTAMP',
                cx_Oracle.UNICODE: 'STRING',
                cx_Oracle.CLOB: 'CLOB',
                cx_Oracle.BLOB: 'BLOB'}


class ComplexEncoder(json.JSONEncoder):
    """To handle decimal types for json encoding."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def get_layers(job):
    """Return the list of layers to index (based on Owner)."""
    layers = []
    layers_to_keep = job.layers_to_keep()
    layers_to_skip = job.layers_to_skip()

    if layers_to_skip and '*' in layers_to_skip[0]:
        return layers

    if layers_to_keep:
        if '*' not in layers_to_keep[0]:
            for lk in layers_to_keep:
                if job.sql_schema:
                    statement = "select table_name, owner from {0}.layers where table_name like '{1}' and owner = '{2}'".format(job.sql_schema, lk[0], lk[1].upper())
                else:
                    statement = "select table_name, owner from sde.layers where table_name like '{0}' and owner = '{1}'".format(lk[0], lk[1].upper())
                [layers.append(l) for l in job.db_cursor.execute(statement).fetchall()]
        else:
            try:
                # If there is no owner, catch the error and continue.
                owner = layers_to_keep[0][1]
                if job.sql_schema:
                    statement = "select table_name, owner from {0}.layers where owner = '{1}'".format(job.sql_schema, owner.upper())
                else:
                    statement = "select table_name, owner from sde.layers where owner = '{0}'".format(owner.upper())
                [layers.append(l) for l in job.db_cursor.execute(statement).fetchall()]
            except IndexError:
                pass

    # Remove any layers from the list meant to be excluded.
    if layers_to_skip and '*' not in layers_to_skip[0]:
            for lk in layers_to_skip:
                if job.sql_schema:
                    statement = "select table_name, owner from {0}.layers where table_name like '{1}' and owner = '{2}'".format(job.sql_schema, lk[0], lk[1])
                else:
                    statement = "select table_name, owner from sde.layers where table_name like '{0}' and owner = '{1}'".format(lk[0], lk[1])
                [layers.remove(l) for l in job.db_cursor.execute(statement).fetchall()]

    return layers


def get_tables(job):
    """Return the list of tables to index (based on the user connected to the database)."""
    tables = []
    tables_to_skip = job.tables_to_skip()
    tables_to_keep = job.tables_to_keep()

    # There are no tables to index.
    if tables_to_skip and '*' in tables_to_skip[0]:
        return tables

    if tables_to_keep:
        if '*' not in tables_to_keep[0]:
            for tk in tables_to_keep:
                if isinstance(tk, tuple):
                    statement = "select table_name from all_tables where table_name like '{0}' and owner = '{1}'".format(tk[0], tk[1])
                    [tables.append((t[0], tk[1])) for t in job.db_cursor.execute(statement).fetchall()]
                else:
                    statement = "select table_name from user_tables where table_name like '{0}'".format(tk)
                    [tables.append(t[0]) for t in job.db_cursor.execute(statement).fetchall()]
        else:
            tk = tables_to_keep[0]
            if isinstance(tk, tuple):
                [tables.append((t[0], tk[1])) for t in job.db_cursor.execute("select table_name from all_tables where owner = '{0}'".format(tk[1]))]
            else:
                [tables.append(t[0]) for t in job.db_cursor.execute("select table_name from user_tables").fetchall()]

    # Remove any tables from the list meant to be excluded.
    if tables_to_skip and '*' not in tables_to_skip[0]:
        for tk in tables_to_skip:
            if isinstance(tk, tuple):
                statement = "select table_name from all_tables where table_name like '{0}' and owner = '{1}'".format(tk[0], tk[1])
                [tables.remove((t[0], tk[1])) for t in job.db_cursor.execute(statement).fetchall()]
            else:
                statement = "select table_name from user_tables where table_name like '{0}'".format(tk)
                [tables.remove(t[0]) for t in job.db_cursor.execute(statement).fetchall()]

    return tables


def get_views(job):
    """Return the list of views to index."""
    views = []
    views_to_keep = job.views_to_keep()
    views_to_skip = job.views_to_skip()

    if views_to_skip and '*' in views_to_skip[0]:
        pass

    if views_to_keep:
        if not views_to_keep[0][0] == '*':
            for vk in views_to_keep:
                if vk[2].lower() == 'all':
                    statement = "select view_name from all_views where view_name like '{0}' and owner = '{1}'".format(vk[0], vk[1].upper())
                    [views.append((v[0], vk[1])) for v in job.db_cursor.execute(statement).fetchall()]
                else:
                    statement = "select view_name from user_views where view_name like '{0}'".format(vk[0])
                    [views.append(v[0]) for v in job.db_cursor.execute(statement).fetchall()]
        else:
            try:
                # If there is no owner, catch the error and continue.
                owner = views_to_keep[0][1]
                if views_to_keep[0][2].lower() == 'all':
                    statement = "select view_name, owner from all_views where owner = '{0}'".format(owner.upper())
                    [views.append(v) for v in job.db_cursor.execute(statement).fetchall()]
                else:
                    statement = "select view_name from user_views"
                    [views.append(v[0]) for v in job.db_cursor.execute(statement).fetchall()]
            except IndexError:
                pass

    # Remove any views from the list meant to be excluded.
    if views_to_skip and '*' not in views_to_skip[0]:
        try:
            for vk in views_to_skip:
                if vk[2].lower() == 'all':
                    statement = "select view_name, owner from all_views where view_name like '{0}' and owner = '{1}'".format(vk[0], vk[1])
                    [views.remove((v[0], vk[1])) for v in job.db_cursor.execute(statement).fetchall()]
                else:
                    statement = "select view_name from user_views where view_name like '{0}'".format(vk[0])
                    [views.remove(v[0]) for v in job.db_cursor.execute(statement).fetchall()]
        except ValueError:
            status_writer.send_state(status.STAT_FAILED, "Schema must be the same for all view configuration.")
            return

    return views


def run_job(oracle_job):
    """Worker function to do the indexing."""
    job = oracle_job
    job.connect_to_zmq()
    job.connect_to_database()
    job.db_cursor.arraysize = 250

    all_tables = []
    all_tables += get_tables(job)
    all_tables += get_layers(job)
    all_tables += get_views(job)

    if not all_tables:
        status_writer.send_state(status.STAT_FAILED, "No tables, views or layers found. Check the configuration.")
        return

    # Begin indexing.
    for tbl in set(all_tables):
        geo = {}
        columns = []
        column_types = {}
        is_point = False
        has_shape = False
        geometry_field = None
        shape_type = None

        # ----------------------------------------------------------------------------
        # Check if the table is a layer/view and set name as "owner.table".
        # ----------------------------------------------------------------------------
        if isinstance(tbl, tuple):
            column_query = "select column_name, data_type from all_tab_cols where " \
                           "table_name = '{0}' and column_name like".format(tbl[0])
            query = job.get_table_query(tbl[0])
            tbl = "{0}.{1}".format(tbl[1], tbl[0])
        else:
            query = job.get_table_query(tbl)
            column_query = "select column_name, data_type from all_tab_cols where " \
                           "table_name = '{0}' and column_name like".format(tbl)

        # -----------------------------------------------------------------------------------------------------
        # Get the table schema.
        # -----------------------------------------------------------------------------------------------------
        table_schema = {}
        schema_columns = []
        table_schema['name'] = tbl

        # Get primary key and foreign keys
        primary_key_col = ''
        foreign_key_col = ''
        primary_key_qry = "select c.table_name, c.column_name from all_constraints cons, all_cons_columns c " \
            "where c.table_name = '{0}' and cons.constraint_type like 'P%' " \
            "and cons.constraint_name = c.constraint_name".format(tbl)
        primary_cols = job.execute_query(primary_key_qry).fetchone()
        if primary_cols:
            primary_key_col = primary_cols[1]
        foreign_key_qry = "select c.table_name, c.column_name from all_constraints cons, all_cons_columns c " \
            "where c.table_name = '{0}' and cons.constraint_type like 'F%' " \
            "and cons.constraint_name = c.constraint_name".format(tbl)
        foreign_cols = job.execute_query(foreign_key_qry).fetchone()
        if foreign_cols:
            foreign_key_col = foreign_cols[1]

        # Get columns that are indexed.
        if "." in tbl:
            owner, table = tbl.split('.')
            index_query = "select column_name from all_ind_columns where table_name = '{0}' and index_owner = '{1}'".format(table, owner)
        else:
            index_query = "select column_name from all_ind_columns where table_name = '{0}'".format(tbl)
        index_columns = [c[0] for c in job.execute_query(index_query).fetchall()]

        for i, c in enumerate(job.execute_query("select * from {0}".format(tbl)).description):
            schema_col = {}
            schema_props = []
            schema_col['name'] = c[0]
            try:
                schema_col['type'] = field_types[c[1]]
            except (AttributeError, KeyError):
                schema_col['type'] = 'OBJECTVAR'
            try:
                if c[1] in ('SDO_GEOMETRY', 'ST_GEOMETRY'):
                    schema_col['isGeo'] = True
                elif job.db_cursor.fetchvars[i].type.name == 'ST_GEOMETRY':
                    schema_col['isGeo'] = True
                elif job.db_cursor.fetchvars[i].type.name == 'SDO_GEOMETRY':
                    schema_col['isGeo'] = True
            except AttributeError:
                pass
            if c[6] == 1:
                schema_props.append('NULLABLE')
            else:
                schema_props.append('NOTNULLABLE')
            if c[0] == primary_key_col:
                schema_props.append('PRIMARY KEY')
            if c[0] == foreign_key_col:
                schema_props.append('FOREIGN KEY')
            if c[0] in index_columns:
                schema_props.append('INDEXED')
            if schema_props:
                schema_col['properties'] = schema_props
            schema_columns.append(schema_col)
        table_schema['fields'] = schema_columns

        # ---------------------------------------------------------------------------------------
        # Create the list of columns and column types to include in the index.
        # ---------------------------------------------------------------------------------------
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

        # -----------------------------------
        # Remove fields meant to be excluded.
        # -----------------------------------
        if job.fields_to_skip:
            for col in job.fields_to_skip:
                [columns.remove(c[0]) for c in job.execute_query("{0} '{1}'".format(column_query, col)).fetchall()]

        # -----------------------------------------------------------
        # If there is a shape column, get the geographic information.
        # -----------------------------------------------------------
        if geometry_field:
            columns.remove(geometry_field)

            if job.db_cursor.execute("select {0} from {1}".format(geometry_field, tbl)).fetchone() is None:
                status_writer.send_status("Skipping {0} - no records.".format(tbl))
                continue
            else:
                schema = job.db_cursor.execute("select {0} from {1}".format(geometry_field, tbl)).fetchone()[0].type.schema

            # Figure out if geometry type is ST or SDO.
            if geometry_type == 'SDO_GEOMETRY':
                geo['code'] = job.db_cursor.execute("select c.{0}.SDO_SRID from {1} c".format(geometry_field, tbl)).fetchone()[0]
                # dimension = job.db_cursor.execute("select c.shape.Get_Dims() from {0} c".format(tbl)).fetchone()[0]
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

        # -------------------------------------------------
        # Drop astext from columns if WKT is not requested.
        # -------------------------------------------------
        # include_wkt = job.include_wkt
        # if not include_wkt and not geometry_type == 'SDO_GEOMETRY':
        #     columns.pop(0)

        # ------------------------------------------------------------
        # Get the count of all the rows to use for reporting progress.
        # ------------------------------------------------------------
        if query:
            row_count = job.db_cursor.execute("select count(*) from {0} where {1}".format(tbl, query)).fetchall()[0][0]
        else:
            row_count = job.db_cursor.execute("select count(*) from {0}".format(tbl)).fetchall()[0][0]
        if row_count == 0 or row_count is None:
            continue
        else:
            row_count = float(row_count)

        # ---------------------------
        # Get the rows to be indexed.
        # ---------------------------
        try:
            if geometry_type == 'SDO_GEOMETRY':
                rows = job.db_cursor.execute("select {0} from {1} {2}".format(','.join(columns), tbl, schema))
            else:
                # Quick check to ensure ST_GEOMETRY operations are supported.
                row = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl)).fetchone()
                del row
                if query:
                    rows = job.db_cursor.execute("select {0} from {1} where {2}".format(','.join(columns), tbl, query))
                else:
                    rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl))
        except Exception:
            # This can occur for ST_GEOMETRY when spatial operators are un-available (See: http://tinyurl.com/lvvhwyl)
            columns.pop(0)
            geo['wkt'] = None
            if query:
                rows = job.db_cursor.execute("select {0} from {1} where {2}".format(','.join(columns), tbl, query))
            else:
                rows = job.db_cursor.execute("select {0} from {1}".format(','.join(columns), tbl))

        # Continue if the table has zero records.
        if not rows:
            status_writer.send_status("Skipping {0} - no records.".format(tbl))
            continue

        # ---------------------------------------------------------
        # Index each row.
        # ---------------------------------------------------------
        mapped_fields = job.map_fields(tbl, columns, column_types)
        increment = job.get_increment(row_count)
        location_id = job.location_id
        action_type = job.action_type
        discovery_id = job.discovery_id
        entry = {}

        # First, add an entry for the table itself with schema.
        table_schema['rows'] = row_count
        table_entry = {}
        table_entry['id'] = '{0}_{1}'.format(location_id, tbl)
        table_entry['location'] = location_id
        table_entry['action'] = action_type
        table_entry['entry'] = {'fields': {'_discoveryID': discovery_id, 'name': tbl, 'path': rows.connection.dsn}}
        table_entry['entry']['fields']['schema'] = table_schema
        job.send_entry(table_entry)

        if not has_shape:
            for i, row in enumerate(rows):
                try:
                    # Map column names to Voyager fields.
                    mapped_cols = dict(izip(mapped_fields, row))
                    if has_shape:
                        [mapped_cols.pop(name) for name in geom_fields]
                    mapped_cols['_discoveryID'] = discovery_id
                    mapped_cols['meta_table_name'] = tbl
                    entry['id'] = '{0}_{1}_{2}'.format(location_id, tbl, i)
                    entry['location'] = location_id
                    entry['action'] = action_type
                    entry['entry'] = {'fields': mapped_cols}
                    job.send_entry(entry)
                    if (i % increment) == 0:
                        status_writer.send_percent(i / row_count, "{0}: {1:%}".format(tbl, i / row_count), 'oracle_worker')
                except Exception as ex:
                    status_writer.send_status(ex)
                    continue
        else:
            geom_fields = [name for name in mapped_fields if '{0}'.format(geometry_field) in name]
            geometry_ops = worker_utils.GeometryOps()
            generalize_value = job.generalize_value
            for i, row in enumerate(rows):
                try:
                    # if include_wkt:
                    if is_point:
                        geo['lon'] = row[1]
                        geo['lat'] = row[2]
                    else:
                        if generalize_value == 0 or generalize_value == 0.0:
                            geo['wkt'] = row[0]
                        elif generalize_value > 0.9:
                            geo['xmin'] = row[1]
                            geo['ymin'] = row[2]
                            geo['xmax'] = row[3]
                            geo['ymax'] = row[4]
                        else:
                            geo['wkt'] = geometry_ops.generalize_geometry(str(row[0]), generalize_value)

                # else:
                #     if is_point:
                #         geo['lon'] = row[0]
                #         geo['lat'] = row[1]
                #     else:
                #         if geometry_type == 'SDO_GEOMETRY':
                #             if dimension == 3:
                #                 geo['xmin'] = row[0][0]
                #                 geo['ymin'] = row[0][1]
                #                 geo['xmax'] = row[0][3]
                #                 geo['ymax'] = row[0][4]
                #             elif dimension == 2:
                #                 geo['xmin'] = row[0][0]
                #                 geo['ymin'] = row[0][1]
                #                 geo['xmax'] = row[0][2]
                #                 geo['ymax'] = row[0][3]
                #         else:
                #             geo['xmin'] = row[0]
                #             geo['ymin'] = row[1]
                #             geo['xmax'] = row[2]
                #             geo['ymax'] = row[3]

                    # Map column names to Voyager fields.
                    mapped_cols = dict(izip(mapped_fields, row))
                    [mapped_cols.pop(name) for name in geom_fields]
                    mapped_cols['_discoveryID'] = discovery_id
                    mapped_cols['meta_table_name'] = tbl
                    entry['id'] = '{0}_{1}_{2}'.format(location_id, tbl, i)
                    entry['location'] = location_id
                    entry['action'] = action_type
                    entry['entry'] = {'geo': geo, 'fields': mapped_cols}
                    job.send_entry(entry)
                    if (i % increment) == 0:
                        status_writer.send_percent(i / row_count, "{0}: {1:%}".format(tbl, i / row_count), 'oracle_worker')
                except Exception as ex:
                    status_writer.send_status(ex)
                    continue
