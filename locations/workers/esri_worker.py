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
import datetime
from collections import OrderedDict
import logging
import multiprocessing
import arcpy
import _server_admin as arcrest
from utils import status

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
    if isinstance(feature['geometry'], arcrest.geometry.Polyline):
        if feature['geometry'].paths:
            for path in feature['geometry'].paths:
                point_coords = [(pt.x, pt.y) for pt in path]
                point_array = arcpy.Array([arcpy.Point(*coords) for coords in point_coords])
                point_array.add(arcpy.Point())
            geometry = arcpy.Polyline(point_array)
    elif isinstance(feature['geometry'], arcrest.geometry.Polygon):
        if feature['geometry'].rings:
            for ring in feature['geometry'].rings:
                point_coords = [(pt.x, pt.y) for pt in ring]
                point_array = arcpy.Array([arcpy.Point(*coords) for coords in point_coords])
                point_array.add(arcpy.Point())
            geometry = arcpy.Polygon(point_array)
    elif isinstance(feature['geometry'], arcrest.geometry.Multipoint):
        if feature['geometry'].points:
            for point in feature['geometry'].points:
                point_coords = [(pt.x, pt.y) for pt in point]
                point_array = arcpy.Array([arcpy.Point(*coords) for coords in point_coords])
                point_array.add(arcpy.Point())
            geometry = arcpy.Multipoint(point_array)

    if geometry:
        return geometry
    else:
        raise NullGeometry


def query_layer(layer, spatial_rel='esriSpatialRelIntersects', where='1=1',
                out_fields='*', out_sr=4326, return_geometry=True):
    """Returns a GPFeatureRecordSetLayer from a feature service layer or
    a GPRecordSet form a feature service table.
    """
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


def index_service(url):
    """Index the records in Map and Feature Services."""
    job.connect_to_zmq()
    geo = {}
    entry = {}
    mapped_attributes = OrderedDict()
    service = arcrest.FeatureService(url)

    layers = service.layers + service.tables

    # Support wildcards for filtering layers and views in the service.
    layers_to_keep = job.tables_to_keep()
    for layer in layers_to_keep:
        lk = layer.split('*')
        if len(lk) == 3 and layer.startswith('*') and layer.endswith('*'):
            layers = [l for l in layers if lk[1] in l.name]
        elif layer.endswith('*'):
            layers = [l for l in layers if lk[0] in l.name]
        elif layer.startswith('*'):
            layers = [l for l in layers if lk[1] in l.name]
        else:
            layers = [l for l in layers if lk[0] == l.name]

    for layer in layers:
        fields_types = {}
        for f in layer.fields:
            fields_types[f['name']] = f['type']

        # Check if the layer is empty.
        ql = query_layer(layer)
        if not ql.features:
            status_writer.send_status("Layer {0} has no features.".format(layer.name))
            continue
        if 'attributes' in ql.features[0]:
            attributes = ql.features[0]['attributes']
        else:
            status_writer.send_status("Layer {0} has no attributes.".format(layer.name))

        status_writer.send_status('Indexing {0}...'.format(layer.name))
        if layer.type == 'Feature Layer':
            geo['srid'] = ql.spatialReference.wkid

        # Map the field and it's value.
        if not job.fields_to_keep == ['*']:
            for fk in job.fields_to_keep:
                mapped_fields = dict((name, val) for name, val in attributes.items() if fk in name)
                if job.fields_to_skip:
                    for fs in job.fields_to_skip:
                        [mapped_fields.pop(name) for name in attributes if name in fs]
        else:
            mapped_fields = attributes

        # This will generate the field mapping dictionary.
        job.tables_to_keep()

        date_fields = set()
        if 'map' in job.field_mapping[0]:
            field_map = job.field_mapping[0]['map']
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

        row_count = len(ql.features)
        increment = float(job.get_increment(row_count))
        if layer.type == 'Table':
            for i, row in enumerate(ql.features):
                entry['id'] = '{0}_{1}_{2}'.format(job.location_id, layer.name, i)
                entry['location'] = job.location_id
                entry['action'] = job.action_type
                mapped_fields = dict(zip(mapped_attributes.keys(), row['attributes'].values()))
                # Convert longs to datetime.
                for df in date_fields:
                    try:
                        mapped_fields[df] = datetime.datetime.fromtimestamp(mapped_fields[df] / 1e3)
                    except KeyError:
                        pass
                mapped_fields['title'] = layer.name
                mapped_fields['_discoveryID'] = job.discovery_id
                entry['entry'] = {'fields': mapped_fields}
                job.send_entry(entry)
                if (i % increment) == 0:
                    status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer.name, i / row_count), 'esri_worker')
        else:
            # Faster to do one if check for geometry type then to condense code and check in every iteration.
            if isinstance(ql.features[0]['geometry'], arcrest.geometry.Point):
                for i, feature in enumerate(ql.features):
                    pt = feature['geometry']
                    geo['lon'] = pt.x
                    geo['lat'] = pt.y
                    entry['id'] = '{0}_{1}_{2}'.format(job.location_id, layer.name, i)
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    mapped_fields = dict(zip(mapped_attributes.keys(), feature['attributes'].values()))
                    # Convert longs to datetime.
                    for df in date_fields:
                        try:
                            mapped_fields[df] = datetime.datetime.fromtimestamp(mapped_fields[df] / 1e3)
                        except KeyError:
                            pass
                    mapped_fields['_discoveryID'] = job.discovery_id
                    mapped_fields['title'] = layer.name
                    entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                    job.send_entry(entry)
                    if (i % increment) == 0:
                        status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer.name, i / row_count), 'esri_worker')
            else:
                for i, feature in enumerate(ql.features):
                    try:
                        geometry = make_feature(feature)  # Catch possible null geometries.
                    except NullGeometry:
                        continue
                    geo['xmin'], geo['xmax'] = geometry.extent.XMin, geometry.extent.YMax
                    geo['ymin'], geo['ymax'] = geometry.extent.YMin, geometry.extent.YMax
                    entry['id'] = '{0}_{1}_{2}'.format(job.location_id, layer.name, i)
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    mapped_fields = dict(zip(mapped_attributes.keys(), feature['attributes'].values()))
                    # Convert longs to datetime.
                    for df in date_fields:
                        try:
                            mapped_fields[df] = datetime.datetime.fromtimestamp(mapped_fields[df] / 1e3)
                        except KeyError:
                            pass
                    mapped_fields['title'] = layer.name
                    mapped_fields['_discoveryID'] = job.discovery_id
                    entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                    job.send_entry(entry)
                    if (i % increment) == 0:
                        status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer.name, i / row_count), 'esri_worker')


def worker(data_path, esri_service=False):
    """The worker function to index feature data and tabular data."""
    if esri_service:
        index_service(data_path)
    else:
        job.connect_to_zmq()
        geo = {}
        entry = {}
        dsc = arcpy.Describe(data_path)

        if job.include_wkt:
            from utils import worker_utils
            geometry_ops = worker_utils.GeometryOps()

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
                    mapped_fields['title'] = dsc.name
                    for nf in new_fields:
                        if nf['name'] == '*' or nf['name'] == dsc.name:
                            for k, v in nf['new_fields'].iteritems():
                                mapped_fields[k] = v
                    oid_field = filter(lambda x: x in ('FID', 'OID', 'OBJECTID'), rows.fields)
                    if oid_field:
                        fld_index = rows.fields.index(oid_field[0])
                    else:
                        fld_index = i
                    entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.basename(data_path), fld_index)
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
                        if job.include_wkt:
                            geo['wkt'] = row[0].WKT
                        mapped_fields = dict(zip(ordered_fields.keys(), row[1:]))
                        mapped_fields['_discoveryID'] = job.discovery_id
                        mapped_fields['title'] = dsc.name
                        for nf in new_fields:
                            if nf['name'] == '*' or nf['name'] == dsc.name:
                                for k, v in nf['new_fields'].iteritems():
                                    mapped_fields[k] = v
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
                        geo['xmin'] = row[0].extent.XMin
                        geo['xmax'] = row[0].extent.XMax
                        geo['ymin'] = row[0].extent.YMin
                        geo['ymax'] = row[0].extent.YMax
                        if job.include_wkt:
                            if generalize_value == 0:
                                geo['wkt'] = row[0].WKT
                            else:
                                geo['wkt'] = geometry_ops.generalize_geometry(row[0].WKT, generalize_value)
                        mapped_fields = dict(zip(ordered_fields.keys(), row[1:]))
                        mapped_fields['_discoveryID'] = job.discovery_id
                        mapped_fields['title'] = dsc.name
                        for nf in new_fields:
                            if nf['name'] == '*' or nf['name'] == dsc.name:
                                for k, v in nf['new_fields'].iteritems():
                                    mapped_fields[k] = v
                        entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.basename(data_path), i)
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)
                        if (i % increment) == 0:
                            status_writer.send_percent(i / row_count, "{0} {1:%}".format(dsc.name, i / row_count), 'esri_worker')


def run_job(esri_job):
    """Determines the data type and each dataset is sent to the worker to be processed."""
    status_writer.send_percent(0.0, "Initializing... 0.0%", 'esri_worker')
    job = esri_job

    if job.path.startswith('http'):
        global_job(job)
        worker(job.path, esri_service=True)
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
