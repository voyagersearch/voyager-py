import os
import sys
import json
from bottle import route, run, request
import settings
import logging


sys.path.append(os.path.join(os.path.abspath(os.path.join(__file__, "../../../../..")), 'arch/win32_x86/py'))
try:
    import zmq
    import arcpy
    import requests
    from requests_ntlm import HttpNtlmAuth
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    session = requests.Session()
    session.auth = HttpNtlmAuth(settings.USERNAME,settings.PASSWORD)

except ImportError as ie:
    sys.stdout.write(ie.message)
    sys.exit(1)


logging.basicConfig(filename=r"D:\voyager\voyager_testing_temp\layer_service.log", level=logging.DEBUG)


# Set up the layer folder path.
meta_folder = None
if 'VOYAGER_META_DIR' in os.environ:
    meta_folder = os.environ['VOYAGER_META_DIR']
else:
    if os.name == 'nt':
        meta_folder = 'D:/voyager/voyager_data/meta'
    else:
        meta_folder = '/var/lib/voyager/data/meta'

# Template layer files required for map and feature service layers.
feature_service_point_template = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'supportfiles',
                                              'point_feature_service_template.lyr')
feature_service_polyline_template = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'supportfiles',
                                                 'line_feature_service_template.lyr')
feature_service_polygon_template = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'supportfiles',
                                                'poly_feature_service_template.lyr')
template_mxd = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'supportfiles',
                                                'FeatureServiceLayersTemplate.mxd')

mxd = arcpy.mapping.MapDocument(template_mxd)
df = mxd.activeDataFrame


class ObjectEncoder(json.JSONEncoder):
    """
    Support non-native Python types for JSON serialization.
    """
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)


def update_index(layer_file, item_id, location, name, endpoint):
    """
    Update the index by re-indexng an item.
    """
    indexer = settings.INDEXER
    zmq_socket = zmq.Context.instance().socket(zmq.PUSH)
    zmq_socket.connect(indexer)
    entry = {"action": "UPDATE", "id": item_id, "path": endpoint, "location": location, "entry": {"fields": {"path_to_lyr": layer_file, "hasLayerFile": True, "name": name}}}
    zmq_socket.send_json(entry, cls=ObjectEncoder)


def create_layer(new_entry):
    """
    Creates layer files for map and feature services.
    """
    new_entry = json.loads(new_entry)
    if 'job' in new_entry and 'id' in new_entry['entry']['fields']:
        # Required fields
        id = new_entry['entry']['fields']['id']
        if 'rest_endpoint' not in new_entry['entry']['fields'] and 'path' not in new_entry['entry']['fields']:
            return
        try:
            rest_endpoint = new_entry['entry']['fields']['rest_endpoint']
        except KeyError:
            rest_endpoint = ''
        try:
            service_type = new_entry['entry']['fields']['format']
        except KeyError:
            return

    # Create a layer file for the Map Server item.
    if ('application/x-arcgis-map-server', 'application/x-arcgis-image-server') in service_type:
        layer_folder = os.path.join(meta_folder, id[0], id[1:4])
        if not os.path.exists(layer_folder):
            os.makedirs(layer_folder)
        if not os.path.exists(os.path.join(layer_folder, '{0}.layer.lyr'.format(id))):
            rest_endpoint = rest_endpoint.replace('?f=json', '?f=lyr')
            lyr = session.get(rest_endpoint, verify=False)
            with open(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)), 'wb') as fp:
                fp.write(lyr.content)
            new_entry['entry']['fields']['hasLayerFile'] = True
            new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)

        # Create layer files for the map server layers (children of the map server item).
        try:
            children = new_entry['entry']['children']
            for child in children:
                child_id = child['fields']['id']
                name = child['fields']['name']
                location = child['fields']['location']
                layer_folder = os.path.join(meta_folder, child_id[0], child_id[1:4])
                if not os.path.exists(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id))):
                    lyr = session.get(rest_endpoint, verify=False)
                    with open(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id)), 'wb') as fp:
                        fp.write(lyr.content)
                    update_index(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id)),
                                 child_id, location, name, child['fields']['path'])
                try:
                    sub_children = child['children']
                    for sub_child in sub_children:
                        child_id = sub_child['fields']['id']
                        name = sub_child['fields']['name']
                        location = sub_child['fields']['location']
                        layer_folder = os.path.join(meta_folder, child_id[0], child_id[1:4])
                        if not os.path.exists(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id))):
                            lyr = session.get(rest_endpoint, verify=False)
                            with open(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id)), 'wb') as fp:
                                fp.write(lyr.content)
                            update_index(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id)), child_id,
                                         location, name, sub_child['fields']['path'])
                except KeyError:
                    continue
        except KeyError:
            pass

    # Create layer files for the Feature Server layers.
    elif service_type == 'application/x-arcgis-feature-server':
        try:
            children = new_entry['entry']['children']
            for child in children:
                child_id = child['fields']['id']
                location = child['fields']['location']
                name = child['fields']['name']
                geometry_type = child['fields']['geometry_type']
                if geometry_type == 'Point':
                    template_layer = arcpy.mapping.Layer(feature_service_point_template)
                elif geometry_type == 'Polyline':
                    template_layer = arcpy.mapping.Layer(feature_service_polyline_template)
                else:
                    template_layer = arcpy.mapping.Layer(feature_service_polygon_template)
                template_layer.replaceDataSource(os.path.dirname(rest_endpoint), 'NONE',
                                                 os.path.basename(rest_endpoint), validate=False)
                template_layer.name = name
                layer_folder = os.path.join(meta_folder, child_id[0], child_id[1:4])
                template_layer.saveACopy(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id)))
                update_index(os.path.join(layer_folder, '{0}.layer.lyr'.format(child_id)), child_id,
                                         location, name, child['fields']['path'])
        except KeyError:
            pass

    # Create layer files when location type is an ArcGIS Online/Portal connection.
    elif service_type in ('application/x-arcgis-online-service', 'application/x-arcgis-online-map'):
        try:
            service_url = new_entry['entry']['fields']['service_url']
            id = new_entry['entry']['fields']['id']
            location = new_entry['entry']['fields']['location']
            path = new_entry['entry']['fields']['path']
            name = new_entry['entry']['fields']['name']
            layer_folder = os.path.join(meta_folder, id[0], id[1:4])
            if not os.path.exists(os.path.join(layer_folder, '{0}.layer.lyr'.format(id))):
                if not os.path.exists(layer_folder):
                    os.mkdir(layer_folder)
                if 'MapServer' in service_url:
                    try:
                        lyr = session.get(service_url + '?f=lyr', verify=False)
                        with open(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)), 'wb') as fp:
                            fp.write(lyr.content)
                        update_index(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)), id, location, name, path)
                    except Exception as ex:
                        logging.info(ex.message)
                        logging.info('Cannot download MapServer: {0}'.format(service_url + '?f=lyr'))
                elif 'FeatureServer/' in service_url:
                    res = session.get(service_url, verify=False)
                    text = res.text # res.read()
                    if 'esriGeometryPoint' in text:
                        template_layer = arcpy.mapping.Layer(feature_service_point_template)
                    elif 'esriGeometryPolyline' in text:
                        template_layer = arcpy.mapping.Layer(feature_service_polyline_template)
                    else:
                        template_layer = arcpy.mapping.Layer(feature_service_polygon_template)
                    template_layer.replaceDataSource(os.path.dirname(service_url), 'NONE',
                                                     os.path.basename(service_url), validate=False)
                    template_layer.name = name
                    layer_folder = os.path.join(meta_folder, id[0], id[1:4])
                    template_layer.saveACopy(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                    update_index(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)), id, location, name, path)
                elif 'FeatureServer' in service_url:
                    res = session.get("{0}?f=json".format(service_url), verify=False)
                    info = res.json()
                    if 'layers' in info:
                        service_layers = info['layers']
                        for sl in service_layers:
                            sl_id = sl['id']
                            sl_name = sl['name']
                            res = session.get("{0}/{1}?f=json".format(service_url, sl_id), verify=False)
                            info = res.json()
                            geometry_type = info['geometryType']
                            if geometry_type == 'esriGeometryPoint':
                                template_layer = arcpy.mapping.Layer(feature_service_point_template)
                            elif geometry_type == 'esriGeometryPolyline':
                                template_layer = arcpy.mapping.Layer(feature_service_polyline_template)
                            else:
                                template_layer = arcpy.mapping.Layer(feature_service_polygon_template)
                            template_layer.replaceDataSource(service_url, 'NONE', sl_id, validate=False)
                            template_layer.name = sl_name
                            group_layer = arcpy.mapping.ListLayers(mxd, 'Service Layers')[0]
                            arcpy.mapping.AddLayerToGroup(df, group_layer, template_layer)
                        group_layer.saveACopy(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
                        arcpy.mapping.RemoveLayer(df, template_layer)
                        update_index(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)), id, location, name, path)
        except KeyError:
            pass

    return json.dumps(new_entry)


def create_image_layer(new_entry):
    """
    Creates layer files for imager service layers.
    """
    new_entry = json.loads(new_entry)
    if 'job' in new_entry and 'id' in new_entry['entry']['fields'] and 'rest_endpoint' in new_entry['entry']['fields']:
        rest_endpoint = new_entry['entry']['fields']['rest_endpoint']
        id = new_entry['entry']['fields']['id']
        if 'fl_objectid' in new_entry['entry']['fields']:
            oid = new_entry['entry']['fields']['fl_objectid']
        else:
            oid = ''
        layer_folder = os.path.join(meta_folder, id[0], id[1:4])
        if not os.path.exists(layer_folder):
            os.makedirs(layer_folder)

        if not os.path.exists(os.path.join(layer_folder, '{0}.layer.lyr'.format(id))):
            if not oid:
                rest_endpoint = rest_endpoint.replace('?f=json', '?f=lyr')
                lyr = requests.get(rest_endpoint)
                with open(os.path.join(layer_folder, '{0}.layer.lyr'.format(id)), 'wb') as fp:
                     fp.write(lyr.content)
            else:
                image_layer = arcpy.MakeImageServerLayer_management(in_image_service="{0}".format(rest_endpoint),
                                                                    out_imageserver_layer="{0}".format(oid),
                                                                    band_index="", mosaic_method="LOCK_RASTER",
                                                                    order_field="Best", order_base_value="0",
                                                                    lock_rasterid="{0}".format(oid),
                                                                    where_clause="OBJECTID = {0}".format(oid))
                arcpy.SaveToLayerFile_management(image_layer, os.path.join(layer_folder, '{0}.layer.lyr'.format(id)))
            new_entry['entry']['fields']['hasLayerFile'] = True
            new_entry['entry']['fields']['path_to_lyr'] = '{0}/{1}/{2}.layer.lyr'.format(id[0], id[1:4], id)

    return json.dumps(new_entry)


@route('/createvectorlayers', method='POST')
def vectorlayerservice():
    entry = request.body.read()
    return create_layer(entry)


@route('/createimagelayers', method='POST')
def imagelayerservice():
    entry = request.body.read()
    return create_image_layer(entry)


run(host=settings.SERVICE_ADDRESS, port=settings.SERVICE_PORT)
