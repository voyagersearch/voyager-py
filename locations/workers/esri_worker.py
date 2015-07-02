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
from __future__ import division
import os
import sys
import copy
import datetime
import itertools
from collections import OrderedDict
import logging
import multiprocessing
import arcpy
import _server_admin as arcrest
from utils import status, worker_utils

status_writer = status.Writer()


class NullGeometry(Exception):
    pass


def global_job(*args):
    """Create a global job object."""
    global job
    job = args[0]


def make_feature(feature):
    """Makes a feature from a arcrest.geometry object."""
    geometry = None
    sr = arcpy.SpatialReference(4326)
    if 'paths' in feature['geometry']:
        paths = feature['geometry']['paths']
        if len(paths) == 1:
            geometry = arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in paths[0]]), sr)
        else:
            parts = []
            for path in paths:
                parts.append(arcpy.Array([arcpy.Point(*coords) for coords in path]))
            geometry = arcpy.Polyline(arcpy.Array(parts), sr)
    elif 'rings' in feature['geometry']:
        rings = feature['geometry']['rings']
        if len(rings) == 1:
            geometry = arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in rings[0]]), sr)
        else:
            parts = []
            for ring in rings:
                parts.append(arcpy.Array([arcpy.Point(*coords) for coords in ring]))
            geometry = arcpy.Polygon(arcpy.Array(parts), sr)
    elif 'points' in feature['geometry']:
        points = feature['geometry']['points']
        if len(points) == 1:
            geometry = arcpy.Multipoint(arcpy.Array([arcpy.Point(*coords) for coords in points[0]]), sr)
        else:
            parts = []
            for point in points:
                parts.append(arcpy.Array([arcpy.Point(*coords) for coords in point]))
            geometry = arcpy.Multipoint(arcpy.Array(parts), sr)

    if geometry:
        return geometry
    else:
        raise NullGeometry


def query_layer(layer, spatial_rel='esriSpatialRelIntersects', count_only=False, where='1=1',
                out_fields='*', out_sr=4326, return_geometry=True, token=''):
    """Returns a GPFeatureRecordSetLayer from a feature service layer or
    a GPRecordSet form a feature service table.
    """
    if count_only:
        objectids = layer._get_subfolder('./query', arcrest.JsonResult, {'where': where, 'returnIdsOnly':True, 'token': token})._json_struct['objectIds']
        if objectids:
            args = [iter(objectids)] * 100
            id_groups = itertools.izip_longest(fillvalue=None, *args)
            return id_groups, len(objectids)
        else:
            return None, None

    if layer.type == 'Feature Layer':
        query = {'spatialRel': spatial_rel, 'where': where,
                 'outFields': out_fields, 'returnGeometry': return_geometry, 'outSR': out_sr}
        out = layer._get_subfolder('./query', arcrest.JsonResult, query)
        # Because of a bug on line 209 of _sever_admin.gptypes where it fails to check for empty geometry.
        features = [feat for feat in out._json_struct['features'] if 'geometry' in feat]
        out._json_struct['features'] = features
        qry_layer = arcrest.gptypes.GPFeatureRecordSetLayer.fromJson(out._json_struct)
    else:
        query = {'where': where, 'outFields': out_fields, 'returnGeometry': return_geometry}
        out = layer._get_subfolder('./query', arcrest.JsonResult, query)
        qry_layer = arcrest.gptypes.GPRecordSet.fromJson(out._json_struct)
    return qry_layer


def update_row(fields, rows, row):
    """Updates the coded values in a row with the coded value descriptions."""
    field_domains = {f.name: f.domain for f in fields if f.domain}
    fields_values = zip(rows.fields, row)
    for j, x in enumerate(fields_values):
        if x[0] in field_domains:
            domain_name = field_domains[x[0]]
            row[j] = job.domains[domain_name][x[1]]
    return row


def index_service(connection_info):
    """Index the records in Map and Feature Services."""
    geometry_ops = worker_utils.GeometryOps()
    job.connect_to_zmq()
    entry = {}
    items = {}
    url = ''

    if 'portal_url'  in connection_info:
        connection_url = connection_info['portal_url']
    else:
        connection_url = connection_info['server_url']
    user_name = connection_info['user_name']
    password = connection_info['password']
    token = connection_info['token']
    generate_token = connection_info['generate_token']
    service_name = connection_info['service_name']
    service_type = connection_info['service_type']
    folder_name = connection_info['folder_name']

    # Create the ArcGIS service helper and get the service url and the service items (layers/tables).
    ags_helper = worker_utils.ArcGISServiceHelper(connection_url, user_name, password)
    try:
        if token == '' and generate_token == 'false':
            url, items = ags_helper.find_item_url(service_name, service_type, folder_name)
        elif token:
            url, items = ags_helper.find_item_url(service_name, service_type, token=token)
        elif generate_token == 'true':
            url, items = ags_helper.find_item_url(service_name, service_type, token=ags_helper.token)
    except IndexError:
        status_writer.send_state(status.STAT_FAILED, "Cannot locate {0}.".format(service_name))
        return

    # Support wildcards for filtering layers and views in the service.
    layers = items['layers'] + items['tables']
    layers_to_keep = job.tables_to_keep()
    for layer in layers_to_keep:
        lk = layer.split('*')
        if len(lk) == 3 and layer.startswith('*') and layer.endswith('*'):
            layers = [l['id'] for l in layers if lk[1] in l['name']]
        elif layer.endswith('*'):
            layers = [l for l in layers if lk[0] in l['name']]
        elif layer.startswith('*'):
            layers = [l['id'] for l in layers if lk[1] in l['name']]
        else:
            layers = [l['id'] for l in layers if lk[0] == l['name']]

    # Index the records for each layer and table within a feature or map service.
    for layer in layers:
        i = 0.
        geo = {}
        layer_id = layer['id']
        layer_name = layer['name']
        mapped_attributes = OrderedDict()

        status_writer.send_status('Indexing {0}...'.format((url, layer_name)))

        # Get the list of fields and field types.
        fields_types = {}
        fields = ags_helper.get_item_fields(url, layer_id, ags_helper.token)
        for f in fields:
            fields_types[f['name']] = f['type']

        # Check if the layer is empty and ensure to get all features, not just first 1000 (esri default).
        objectid_groups, row_count = ags_helper.get_item_row_count(url, layer_id, ags_helper.token)
        oid_field_name = ags_helper.oid_field_name
        if not row_count:
            status_writer.send_status("Layer {0} has no features.".format(layer_name))
            continue
        else:
            increment = float(job.get_increment(row_count))

        for group in objectid_groups:
            group = [oid for oid in group if not oid == None]
            rows = ags_helper.get_item_rows(url, layer_id, ags_helper.token, where='{0} IN {1}'.format(oid_field_name, tuple(group)))
            features = rows['features']
            if not features:
                status_writer.send_status("Layer {0} has no features.".format(layer_name))
                continue
            if 'attributes' in features[0]:
                attributes = OrderedDict(features[0]['attributes'])
            else:
                status_writer.send_status("Layer {0} has no attributes.".format(layer_name))

            if 'geometryType' in rows:
                geometry_type = rows['geometryType']
            else:
                geometry_type = 'Table'
            if 'spatialReference' in rows:
                geo['srid'] = rows['spatialReference']['wkid']

            # Map the field and it's value.
            if not job.fields_to_keep == ['*']:
                for fk in job.fields_to_keep:
                    mapped_fields = dict((name, val) for name, val in attributes.items() if fk in name)
                    if job.fields_to_skip:
                        for fs in job.fields_to_skip:
                            [mapped_fields.pop(name) for name in attributes if name in fs]
            else:
                mapped_fields = copy.deepcopy(attributes)

            # This will generate the field mapping dictionary.
            job.tables_to_keep()

            date_fields = set()
            field_map = None
            for mapping in job.field_mapping:
                if mapping['name'] == layer_name:
                    field_map = mapping['map']
                    break

            if not field_map:
                for mapping in job.field_mapping:
                    if mapping['name'] == '*':
                        field_map = mapping['map']
                        break

            if field_map:
                for k, v in mapped_fields.items():
                    if k in field_map:
                        new_field = field_map[k]
                        mapped_attributes[new_field] = mapped_fields.pop(k)
                    else:
                        field_type = job.default_mapping(fields_types[k])
                        if field_type == 'fd_':
                            # Because dates are being returned as longs.
                            mapped_attributes[field_type + k] = v
                            date_fields.add(field_type + k)
                        else:
                            mapped_attributes[field_type + k] = mapped_fields.pop(k)
            else:
                for k, v in mapped_fields.items():
                    field_type = job.default_mapping(fields_types[k])
                    if field_type == 'fd_':
                        # Because dates are being returned as longs.
                        mapped_attributes[field_type + k] = v
                        date_fields.add(field_type + k)
                    else:
                        mapped_attributes[field_type + k] = mapped_fields.pop(k)

            i += len(features)
            if geometry_type == 'Table':
                for x, row in enumerate(features):
                    entry['id'] = '{0}_{1}_{2}_{3}'.format(job.location_id, layer_name, i, x)
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    mapped_fields = dict(zip(mapped_attributes.keys(), row['attributes'].values()))
                    # Convert longs to datetime.
                    for df in date_fields:
                        try:
                            mapped_fields[df] = datetime.datetime.fromtimestamp(mapped_fields[df] / 1e3)
                        except (KeyError, TypeError):
                            pass
                    mapped_fields['title'] = layer_name
                    mapped_fields['meta_table_name'] = layer_name
                    mapped_fields['_discoveryID'] = job.discovery_id
                    entry['entry'] = {'fields': mapped_fields}
                    job.send_entry(entry)
                    if (i % increment) == 0:
                        status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer_name, i / row_count), 'esri_worker')
            else:
                # Faster to do one if check for geometry type then to condense code and check in every iteration.
                if geometry_type == 'esriGeometryPoint':
                    for x, feature in enumerate(features):
                        pt = feature['geometry']
                        geo['lon'] = pt['x']
                        geo['lat'] = pt['y']
                        entry['id'] = '{0}_{1}_{2}_{3}'.format(job.location_id, layer_name, int(i), x)
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        mapped_fields = dict(zip(mapped_attributes.keys(), feature['attributes'].values()))
                        # Convert longs to datetime.
                        for df in date_fields:
                            try:
                                mapped_fields[df] = datetime.datetime.fromtimestamp(mapped_fields[df] / 1e3)
                            except (KeyError, TypeError):
                                pass
                        mapped_fields['_discoveryID'] = job.discovery_id
                        mapped_fields['title'] = layer_name
                        mapped_fields['geometry_type'] = 'Point'
                        mapped_fields['meta_table_name'] = layer_name
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)
                        if (i % increment) == 0:
                            status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer_name, int(i) / row_count), 'esri_worker')
                else:
                    generalize_value = job.generalize_value
                    for x, feature in enumerate(features):
                        try:
                            geometry = make_feature(feature)  # Catch possible null geometries.
                        except NullGeometry:
                            continue

                        if generalize_value > 0.9:
                            geo['xmin'], geo['xmax'] = geometry.extent.XMin, geometry.extent.XMax
                            geo['ymin'], geo['ymax'] = geometry.extent.YMin, geometry.extent.YMax
                        elif generalize_value == 0 or generalize_value == 0.0:
                            geo['wkt'] = geometry.WKT
                        else:
                            if geometry_ops:
                                geo['wkt'] = geometry_ops.generalize_geometry(geometry.WKT, generalize_value)
                            else:
                                geo['xmin'], geo['xmax'] = geometry.extent.XMin, geometry.extent.XMax
                                geo['ymin'], geo['ymax'] = geometry.extent.YMin, geometry.extent.YMax

                        entry['id'] = '{0}_{1}_{2}_{3}'.format(job.location_id, layer_name, int(i), x)
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        mapped_fields = dict(zip(mapped_attributes.keys(), OrderedDict(feature['attributes']).values()))
                        # Convert longs to datetime.
                        for df in date_fields:
                            try:
                                mapped_fields[df] = datetime.datetime.fromtimestamp(mapped_fields[df] / 1e3)
                            except (KeyError, TypeError):
                                pass
                        mapped_fields['title'] = layer_name
                        mapped_fields['geometry_type'] = geometry.type
                        mapped_fields['meta_table_name'] = layer_name
                        mapped_fields['_discoveryID'] = job.discovery_id
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)
                        if (i % increment) == 0:
                            status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer_name, i / row_count), 'esri_worker')


def worker(data_path, esri_service=False):
    """The worker function to index feature data and tabular data."""
    if esri_service:
        index_service(job.service_connection)
    else:
        job.connect_to_zmq()
        geo = {}
        entry = {}
        schema = {}
        dsc = arcpy.Describe(data_path)

        try:
            from utils import worker_utils
            geometry_ops = worker_utils.GeometryOps()
        except ImportError:
            geometry_ops = None

        try:
            global_id_field = dsc.globalIDFieldName
        except AttributeError:
            global_id_field = None

        try:
            shape_field_name = dsc.shapeFieldName
        except AttributeError:
            shape_field_name = None

        # Get the table schema.
        table_entry = {}
        schema['name'] = dsc.name
        try:
            alias = dsc.aliasName
        except AttributeError:
            alias = dsc.name
        if not dsc.name == alias:
            schema['alias'] = alias
        schema['OIDFieldName'] = dsc.OIDFieldName
        if shape_field_name:
            schema['shapeFieldName'] = shape_field_name
            schema['wkid'] = dsc.spatialReference.factoryCode
        if global_id_field:
            schema['globalIDField'] = global_id_field
        schema_fields = []
        for fld in dsc.fields:
            field = {}
            props = []
            field['name'] = fld.name
            field['alias'] = fld.aliasName
            field['type'] = fld.type
            field['domain'] = fld.domain
            if fld.isNullable:
                props.append('nullable')
            else:
                props.append('notnullable')
            indexes = dsc.indexes
            if indexes:
                for index in indexes:
                    if fld.name in [f.name for f in index.fields]:
                        props.append('indexed')
                        break
                    else:
                        props.append('notindexed')
                        break
            field['properties'] = props
            schema_fields.append(field)
        schema['fields'] = schema_fields

        if dsc.dataType == 'Table':
            # Get join information.
            table_join = job.get_join(dsc.name)
            if table_join:
                table_view = arcpy.MakeTableView_management(dsc.catalogPath, 'view')
                arcpy.AddJoin_management(table_view, table_join['field'], os.path.join(job.path, table_join['table']), table_join['field'], 'KEEP_COMMON')
            else:
                table_view = dsc.catalogPath

            # Get any query or constraint.
            query = job.get_table_query(dsc.name)
            constraint = job.get_table_constraint(dsc.name)
            if query and constraint:
                expression = """{0} AND {1}""".format(query, constraint)
            else:
                if query:
                    expression = query
                else:
                    expression = constraint

            field_types = job.search_fields(table_view)
            fields = field_types.keys()
            row_count = float(arcpy.GetCount_management(table_view).getOutput(0))
            if row_count == 0.0:
                return

            with arcpy.da.SearchCursor(table_view, fields, expression) as rows:
                mapped_fields = job.map_fields(dsc.name, fields, field_types)
                new_fields = job.new_fields
                ordered_fields = OrderedDict()
                for f in mapped_fields:
                    ordered_fields[f] = None
                increment = job.get_increment(row_count)
                for i, row in enumerate(rows, 1):
                    if job.domains:
                        row = update_row(dsc.fields, rows, list(row))
                    mapped_fields = dict(zip(ordered_fields.keys(), row))
                    mapped_fields['_discoveryID'] = job.discovery_id
                    mapped_fields['meta_table_name'] = dsc.name
                    # mapped_fields['format'] = "{0} Record".format(dsc.dataType)
                    for nf in new_fields:
                        if nf['name'] == '*' or nf['name'] == dsc.name:
                            for k, v in nf['new_fields'].iteritems():
                                mapped_fields[k] = v
                    oid_field = filter(lambda x: x in ('FID', 'OID', 'OBJECTID'), rows.fields)
                    if oid_field:
                        fld_index = rows.fields.index(oid_field[0])
                    else:
                        fld_index = i
                    if global_id_field:
                         mapped_fields['meta_{0}'.format(global_id_field)] = mapped_fields.pop('fi_{0}'.format(global_id_field))
                    entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.basename(data_path), row[fld_index])
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    entry['entry'] = {'fields': mapped_fields}
                    job.send_entry(entry)
                    if (i % increment) == 0:
                        status_writer.send_percent(i / row_count, "{0} {1:%}".format(dsc.name, i / row_count), 'esri_worker')
        else:
            generalize_value = job.generalize_value
            sr = arcpy.SpatialReference(4326)
            geo['spatialReference'] = dsc.spatialReference.name
            geo['code'] = dsc.spatialReference.factoryCode

            # Get join information.
            table_join = job.get_join(dsc.name)
            if table_join:
                lyr = arcpy.MakeFeatureLayer_management(dsc.catalogPath, 'lyr')
                arcpy.AddJoin_management(lyr, table_join['input_join_field'], os.path.join(job.path, table_join['table']), table_join['output_join_field'], 'KEEP_COMMON')
            else:
                lyr = dsc.catalogPath

            field_types = job.search_fields(lyr)
            fields = field_types.keys()
            query = job.get_table_query(dsc.name)
            constraint = job.get_table_constraint(dsc.name)
            if query and constraint:
                expression = """{0} AND {1}""".format(query, constraint)
            else:
                if query:
                    expression = query
                else:
                    expression = constraint
            if dsc.shapeFieldName in fields:
                fields.remove(dsc.shapeFieldName)
                field_types.pop(dsc.shapeFieldName)
            elif table_join:
                fields.remove(arcpy.Describe(lyr).shapeFieldName)
                field_types.pop(arcpy.Describe(lyr).shapeFieldName)
            row_count = float(arcpy.GetCount_management(lyr).getOutput(0))
            if row_count == 0.0:
                return
            if dsc.shapeType == 'Point':
                with arcpy.da.SearchCursor(lyr, ['SHAPE@'] + fields, expression, sr) as rows:
                    mapped_fields = job.map_fields(dsc.name, list(rows.fields[1:]), field_types)
                    new_fields = job.new_fields
                    ordered_fields = OrderedDict()
                    for f in mapped_fields:
                        ordered_fields[f] = None
                    increment = job.get_increment(row_count)
                    for i, row in enumerate(rows):
                        if job.domains:
                            row = update_row(dsc.fields, rows, list(row))
                        geo['lon'] = row[0].firstPoint.X
                        geo['lat'] = row[0].firstPoint.Y
                        mapped_fields = dict(zip(ordered_fields.keys(), row[1:]))
                        mapped_fields['_discoveryID'] = job.discovery_id
                        mapped_fields['meta_table_name'] = dsc.name
                        mapped_fields['geometry_type'] = 'Point'
                        # mapped_fields['format'] = "{0} Record".format(dsc.dataType)
                        for nf in new_fields:
                            if nf['name'] == '*' or nf['name'] == dsc.name:
                                for k, v in nf['new_fields'].iteritems():
                                    mapped_fields[k] = v
                        if global_id_field:
                             mapped_fields['meta_{0}'.format(global_id_field)] = mapped_fields.pop('fi_{0}'.format(global_id_field))
                        entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.basename(data_path), i)
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)
                        if (i % increment) == 0:
                            status_writer.send_percent(i / row_count, "{0} {1:%}".format(dsc.name, i / row_count), 'esri_worker')
            else:
                with arcpy.da.SearchCursor(lyr, ['SHAPE@'] + fields, expression, sr) as rows:
                    increment = job.get_increment(row_count)
                    mapped_fields = job.map_fields(dsc.name, list(rows.fields[1:]), field_types)
                    new_fields = job.new_fields
                    ordered_fields = OrderedDict()
                    for f in mapped_fields:
                        ordered_fields[f] = None
                    for i, row in enumerate(rows):
                        if job.domains:
                            row = update_row(dsc.fields, rows, list(row))
                        if generalize_value == 0 or generalize_value == 0.0:
                            geo['wkt'] = row[0].WKT
                        else:
                            if geometry_ops:
                                geo['wkt'] = geometry_ops.generalize_geometry(row[0].WKT, generalize_value)
                            else:
                                geo['xmin'] = row[0].extent.XMin
                                geo['xmax'] = row[0].extent.XMax
                                geo['ymin'] = row[0].extent.YMin
                                geo['ymax'] = row[0].extent.YMax
                        mapped_fields = dict(zip(ordered_fields.keys(), row[1:]))
                        mapped_fields['_discoveryID'] = job.discovery_id
                        mapped_fields['meta_table_name'] = dsc.name
                        for nf in new_fields:
                            if nf['name'] == '*' or nf['name'] == dsc.name:
                                for k, v in nf['new_fields'].iteritems():
                                    mapped_fields[k] = v
                        if global_id_field:
                            mapped_fields['meta_{0}'.format(global_id_field)] = mapped_fields.pop('fi_{0}'.format(global_id_field))
                        mapped_fields['geometry_type'] = dsc.shapeType
                        # mapped_fields['format'] = "{0} Record".format(dsc.dataType)
                        entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.splitext(os.path.basename(data_path))[0], i)
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)
                        if (i % increment) == 0:
                            status_writer.send_percent(i / row_count, "{0} {1:%}".format(dsc.name, i / row_count), 'esri_worker')

        # Add and entry for the table and it's schema.
        schema['rows'] = row_count
        table_entry['id'] = '{0}_{1}'.format(job.location_id, dsc.name)
        table_entry['location'] = job.location_id
        table_entry['action'] = job.action_type
        table_entry['entry'] = {'fields': {'_discoveryID': job.discovery_id, 'name': dsc.name, 'path': dsc.catalogPath}}
        table_entry['entry']['fields']['schema'] = schema
        job.send_entry(table_entry)


def run_job(esri_job):
    """Determines the data type and each dataset is sent to the worker to be processed."""
    status_writer.send_percent(0.0, "Initializing... 0.0%", 'esri_worker')
    job = esri_job

    # if job.path.startswith('http'):
    if job.service_connection:
        global_job(job)
        worker(job.service_connection, esri_service=True)
        return

    dsc = arcpy.Describe(job.path)
    # A single feature class or table.
    if dsc.dataType in ('DbaseTable', 'FeatureClass', 'ShapeFile', 'Shapefile', 'Table'):
        global_job(job, int(arcpy.GetCount_management(job.path).getOutput(0)))
        job.tables_to_keep()  # This will populate field mapping.
        worker(job.path)
        return

    # A geodatabase (.mdb, .gdb, or .sde).
    elif dsc.dataType == 'Workspace':
        arcpy.env.workspace = job.path
        feature_datasets = arcpy.ListDatasets('*', 'Feature')
        tables = []
        tables_to_keep = job.tables_to_keep()
        tables_to_skip = job.tables_to_skip()
        if job.tables_to_keep:
            for t in tables_to_keep:
                [tables.append(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables(t)]
                [tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                for fds in feature_datasets:
                    [tables.append(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]
        else:
            [tables.append(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables()]
            [tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses()]
            for fds in feature_datasets:
                [tables.append(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(feature_dataset=fds)]

        if tables_to_skip:
            for t in tables_to_keep:
                [tables.remove(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables(t)]
                [tables.remove(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                for fds in feature_datasets:
                    [tables.remove(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]

    # A geodatabase feature dataset, SDC data, or CAD dataset.
    elif dsc.dataType == 'FeatureDataset' or dsc.dataType == 'CadDrawingDataset':
        tables_to_keep = job.tables_to_keep()
        tables_to_skip = job.tables_to_skip()
        arcpy.env.workspace = job.path
        if tables_to_keep:
            tables = []
            for tbl in tables_to_keep:
                [tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(tbl)]
                tables = list(set(tables))
        else:
            tables = [os.path.join(job.path, fc) for fc in arcpy.ListFeatureClasses()]
        if tables_to_skip:
            for tbl in tables_to_skip:
                [tables.remove(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(tbl) if fc in tables]

    # Not a recognized data type.
    else:
        sys.exit(1)

    if job.multiprocess:
        # Multiprocess larger databases and feature datasets.
        multiprocessing.log_to_stderr()
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.INFO)
        pool = multiprocessing.Pool(initializer=global_job, initargs=(job,))
        for i, _ in enumerate(pool.imap_unordered(worker, tables), 1):
            status_writer.send_percent(i / len(tables), "{0:%}".format(i / len(tables)), 'esri_worker')
        # Synchronize the main process with the job processes to ensure proper cleanup.
        pool.close()
        pool.join()
    else:
        for i, tbl in enumerate(tables, 1):
            global_job(job)
            worker(tbl)
            status_writer.send_percent(i / len(tables), "{0} {1:%}".format(tbl, i / len(tables)), 'esri_worker')
    return
