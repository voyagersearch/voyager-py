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
import copy
import datetime
from collections import OrderedDict
import hashlib
from utils import status

status_writer = status.Writer()


class NullGeometry(Exception):
    pass


def global_job(*args):
    """Create a global job object."""
    global job
    job = args[0]


def get_date(date_long):
    """Convert longs to datetime."""
    dt = None
    try:
        dt = datetime.datetime.fromtimestamp(date_long / 1e3)
    except (KeyError, TypeError, ValueError):
        try:
            dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=(date_long / 1e3))
        except TypeError:
            pass
    return dt


def index_service(connection_info):
    """Index the records in Map and Feature Services."""
    from utils import worker_utils
    geometry_ops = worker_utils.GeometryOps()
    geo_json_converter = worker_utils.GeoJSONConverter()
    job.connect_to_zmq()
    entry = {}
    items = {}
    url = ''

    if 'portal_url' in connection_info:
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
    if 'instance' in connection_info:
        instance = connection_info['instance']
    else:
        instance = 'arcgis'
    if 'verify_ssl' in connection_info and connection_info['verify_ssl'].lower() in ['false', 'n']:
        verify_ssl = False
    else:
        verify_ssl = True

    # Create the ArcGIS service helper and get the service url and the service items (layers/tables).
    ags_helper = worker_utils.ArcGISServiceHelper(connection_url, user_name, password, verify_ssl, instance=instance)
    try:
        if token == '' and generate_token == 'false':
            url, items = ags_helper.find_item_url(service_name, service_type, folder_name)
        elif token and folder_name:
            url, items = ags_helper.find_item_url(service_name, service_type, folder_name, token=token)
        elif token:
            url, items = ags_helper.find_item_url(service_name, service_type, token=token)
        elif generate_token and folder_name:
            url, items = ags_helper.find_item_url(service_name, service_type, folder_name, token=token)
        elif generate_token == 'true':
            url, items = ags_helper.find_item_url(service_name, service_type, token=ags_helper.token)
    except IndexError:
        status_writer.send_state(status.STAT_FAILED, "Cannot locate {0}.".format(service_name))
        return
    except worker_utils.InvalidToken as invalid_token:
        status_writer.send_state(status.STAT_FAILED, invalid_token.message)
        return
    except Exception as ex:
        status_writer.send_state(status.STAT_FAILED, ex.message)
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
            layers = [l for l in layers if lk[1] in l['name']]
        else:
            layers = [l for l in layers if lk[0] == l['name']]

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
        try:
            fields = ags_helper.get_item_fields(url, layer_id, ags_helper.token)
        except KeyError:
            status_writer.send_status("Layer {0} has no fields.".format(layer_name))
            continue
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
            rows = ags_helper.get_item_rows(url, layer_id, ags_helper.token, where='{0} IN {1}'.format(oid_field_name, tuple(group)), response_format='geojson')
            features = None
            if 'features' in rows:
                features = rows['features']
            if not features:
                status_writer.send_status("Layer {0} has no features.".format(layer_name))
                continue
            if 'properties' in features[0]:
                attributes = OrderedDict(features[0]['properties'])
            elif 'attributes' in features[0]:
                attributes = OrderedDict(features[0]['attributes'])
            else:
                status_writer.send_status("Layer {0} has no attributes.".format(layer_name))

            if 'geometry' in features[0] and features[0]['geometry']:
                if 'type' in features[0]['geometry']:
                    geometry_type = features[0]['geometry']['type']
                elif 'rings' in features[0]['geometry']:
                    geometry_type = 'Rings'
                elif 'paths' in features[0]['geometry']:
                    geometry_type = 'Paths'
                elif 'x' in features[0]['geometry'] and 'y' in features[0]['geometry']:
                    geometry_type = 'Point'
            else:
                geometry_type = 'Table'
            if 'crs' in rows:
                if ':' in rows['crs']['properties']['name']:
                    geo['srid'] = rows['crs']['properties']['name'].split(':')[1]
                else:
                    geo['srid'] = rows['crs']['properties']['name']
            elif 'spatialReference' in rows:
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
                    hash_obj = hashlib.md5(os.path.join(url, '{0}_{1}_{2}_{3}'.format(job.location_id, layer_name, i, x)))
                    entry['id'] = hash_obj.hexdigest()
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    if 'properties' in row:
                        mapped_fields = dict(zip(mapped_attributes.keys(), row['properties'].values()))
                    else:
                        mapped_fields = dict(zip(mapped_attributes.keys(), row['attributes'].values()))
                    # Convert longs to datetime.
                    for df in date_fields:
                        mapped_fields[df] = get_date(mapped_fields[df])
                    mapped_fields['id'] = entry['id']
                    mapped_fields['meta_table_name'] = layer_name
                    mapped_fields['_discoveryID'] = job.discovery_id
                    mapped_fields['format_category'] = 'GIS'
                    mapped_fields['format_type'] = 'Service Layer Record'
                    mapped_fields['format'] = 'application/vnd.esri.service.layer.record'
                    entry['entry'] = {'fields': mapped_fields}
                    job.send_entry(entry)
                    if (x % increment) == 0:
                        status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer_name, i / row_count), 'agol_worker')
            else:
                # Faster to do one if check for geometry type then to condense code and check in every iteration.
                if geometry_type == 'Point':
                    for x, feature in enumerate(features, 1):
                        if 'type' not in feature['geometry']:
                            pt = feature['geometry']
                            geo['lon'] = pt['x']
                            geo['lat'] = pt['y']
                        else:
                            geo['wkt'] = geo_json_converter.convert_to_wkt(feature['geometry'], 6)
                        hash_obj = hashlib.md5(os.path.join(url, '{0}_{1}_{2}_{3}'.format(job.location_id, layer_name, int(i), x)))
                        entry['id'] = hash_obj.hexdigest()
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        if 'properties' in feature:
                            mapped_fields = dict(zip(mapped_attributes.keys(), feature['properties'].values()))
                        else:
                            mapped_fields = dict(zip(mapped_attributes.keys(), feature['attributes'].values()))
                        # Convert longs to datetime.
                        for df in date_fields:
                            mapped_fields[df] = get_date(mapped_fields[df])
                        mapped_fields['id'] = entry['id']
                        mapped_fields['_discoveryID'] = job.discovery_id
                        mapped_fields['geometry_type'] = 'Point'
                        mapped_fields['meta_table_name'] = layer_name
                        mapped_fields['format_category'] = 'GIS'
                        mapped_fields['format_type'] = 'Service Layer Feature'
                        mapped_fields['format'] = 'application/vnd.esri.service.layer.record'
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)
                        if (x % increment) == 0:
                            status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer_name, i / row_count), 'agol_worker')
                else:
                    generalize_value = job.generalize_value
                    for x, feature in enumerate(features, 1):
                        try:
                            # geometry = make_feature(feature)  # Catch possible null geometries.
                            if geometry_type == 'Rings':
                                geometry = geo_json_converter.create_polygon(feature['geometry']['rings'][0])
                            elif geometry_type == 'Paths':
                                geometry = geo_json_converter.create_polyline(feature['geometry']['paths'][0])
                            else:
                                geometry = geo_json_converter.convert_to_wkt(feature['geometry'], 3)
                        except RuntimeError:
                            continue

                        # if generalize_value > 0.9:
                        #     geo['xmin'], geo['xmax'] = geometry.extent.XMin, geometry.extent.XMax
                        #     geo['ymin'], geo['ymax'] = geometry.extent.YMin, geometry.extent.YMax
                        if generalize_value == 0 or generalize_value == 0.0:
                            geo['wkt'] = geometry.WKT
                        else:
                            if geometry_ops:
                                geo['wkt'] = geometry_ops.generalize_geometry(geometry, generalize_value)
                            else:
                                geo['wkt'] = geometry.WKT
                                # geo['xmin'], geo['xmax'] = geometry.extent.XMin, geometry.extent.XMax
                                # geo['ymin'], geo['ymax'] = geometry.extent.YMin, geometry.extent.YMax

                        hash_obj = hashlib.md5(os.path.join(url, '{0}_{1}_{2}_{3}'.format(job.location_id, layer_name, int(i), x)))
                        entry['id'] = hash_obj.hexdigest()
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        if 'properties' in feature:
                            mapped_fields = dict(zip(mapped_attributes.keys(), OrderedDict(feature['properties']).values()))
                        else:
                            mapped_fields = dict(
                                zip(mapped_attributes.keys(), OrderedDict(feature['attributes']).values()))
                        try:
                            # Convert longs to datetime.
                            for df in date_fields:
                                mapped_fields[df] = get_date(mapped_fields[df])
                            if geometry_type == 'Paths':
                                mapped_fields['geometry_type'] = 'Polyline'
                            elif geometry_type == 'Rings':
                                mapped_fields['geometry_type'] = 'Polygon'
                            else:
                                mapped_fields['geometry_type'] = geometry_type
                            mapped_fields['meta_table_name'] = layer_name
                            try:
                                mapped_fields['meta_table_path'] = layer['path']
                            except KeyError:
                                layer['path'] = url + '/' + str(layer_id)
                                mapped_fields['meta_table_path'] = layer['path']
                            mapped_fields['id'] = entry['id']
                            mapped_fields['meta_table_location'] = os.path.dirname(layer['path'])
                            mapped_fields['format_category'] = 'GIS'
                            mapped_fields['format_type'] = 'Service Layer Feature'
                            mapped_fields['format'] = 'application/vnd.esri.service.layer.record'
                            mapped_fields['_discoveryID'] = job.discovery_id
                            entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                            job.send_entry(entry)
                        except KeyError:
                            job.send_entry(entry)
                        if (x % increment) == 0:
                            status_writer.send_percent(i / row_count, "{0} {1:%}".format(layer_name, i / row_count), 'agol_worker')


def run_job(esri_job):
    """Determines the data type and each dataset is sent to the worker to be processed."""
    status_writer.send_percent(0.0, "Initializing... 0.0%", 'agol_worker')
    job = esri_job

    if job.service_connection:
        global_job(job)
        index_service(job.service_connection)
        return
