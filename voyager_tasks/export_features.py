# -*- coding: utf-8 -*-
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
import os
import voyager_tasks
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils
from voyager_tasks import _


locations = set()
titles = set()
status_writer = status.Writer()


def add_fields(layer_name, fields):
    """Add fields to a geodatabaes feature class.
    :param layer_name: layer name
    :param fields: list of fields to add
    """
    import arcpy
    esri_field_types = {'fs': 'TEXT',
                        'fl': 'LONG',
                        'fi': 'SHORT',
                        'ff': 'LONG',
                        'fu': 'DOUBLE',
                        'fd': 'DATE',
                        'meta': 'TEXT'}

    new_fields = {}
    for name, value in fields.iteritems():
        try:
            field_type = esri_field_types[name.split('_')[0]]
            real_name = '_'.join(name.split('_')[1:])
            arcpy.AddField_management(layer_name, real_name, field_type)
            new_fields[real_name] = value
        except KeyError:
            arcpy.AddField_management(layer_name, name, 'TEXT')
            new_fields[name] = value
            continue
        except arcpy.ExecuteError as ee:
            if "already exists" in ee.message:
                continue
            else:
                status_writer.send_state(status.STAT_FAILED, _(ee))
                return
    return new_fields


def create_shapefile(path, layer_name, fields, shape_type, epsg_code):
    """Create a shapefile using OGR.
    :param path: full shapefile path
    :param layer_name: shapefile name without .shp extention
    :param fields: dictionary of name and type
    :param shape_type: the type of geometry
    :param epsg_code: Spatial refernce code (i.e. 4326)
    :return real_names: valid field names
    """
    import ogr
    ogr_field_types = {'fs': ogr.OFTString,
                       'fl': ogr.OFTInteger,
                        'fi': ogr.OFTInteger,
                        'ff': ogr.OFTInteger,
                        'fu': ogr.OFTReal,
                        'fd': ogr.OFTDateTime,
                        'meta': ogr.OFTString}
    driver = ogr.GetDriverByName("ESRI Shapefile")
    shape_file = driver.CreateDataSource(path)
    if epsg_code == 3:
        epsg_code = 4326
    srs = ogr.osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    layer = shape_file.CreateLayer(layer_name, srs, shape_type)

    real_names = {}
    i = 0
    for name, val in fields.iteritems():
        try:
            field_type = ogr_field_types[name.split('_')[0]]
            if field_type == 0:
                new_field = ogr.FieldDefn('_'.join(name.split('_')[1:]), ogr.OFTInteger)
            else:
                new_field = ogr.FieldDefn('_'.join(name.split('_')[1:]), field_type)
        except KeyError:
            new_field = ogr.FieldDefn(name, ogr.OFTString)
        layer.CreateField(new_field)
        real_names[layer.schema[i].name] = val
        i += 1
    shape_file.Destroy()
    return real_names


def export_to_shapefiles(jobs, output_folder):
    """Exports features to a shapefile.
    :param jobs: list of jobs (a job contains the feature information)
    :param output_folder: the output task folder
    """
    import ogr
    shape_file = None
    num_jobs = len(jobs)
    increment = task_utils.get_increment(num_jobs)
    for cnt, job in enumerate(jobs, 1):
        job_info = job['_job']
        job_info = eval(job_info.replace('null', 'None'))
        location = job_info['location']
        fields = job_info['entry']['fields']

        # Get the geographic information (if available)
        if 'geo' in job_info['entry']:
            geo = job_info['entry']['geo']
        else:
            status_writer.send_state(status.STAT_WARNING, _("{0} has no geographic information").format(job['id']))
            continue

        geometry_type = None
        if 'wkt' in geo:
            wkt = geo['wkt']
            if 'MULTIPOINT' in wkt:
                geometry_type = ogr.wkbMultiPoint
            elif 'MULTILINESTRING' in wkt:
                geometry_type = ogr.wkbMultiLineString
            elif "MULTIPOLYGON" in wkt:
                geometry_type = ogr.wkbMultiPolygon
            elif 'POINT' in wkt:
                geometry_type = ogr.wkbPoint
            elif 'LINESTRING' in wkt:
                geometry_type = ogr.wkbLineString
            elif "POLYGON" in wkt:
                geometry_type = ogr.wkbPolygon
        elif 'lon' in geo:
            wkt = "POINT ({0} {1})".format(geo['lon'], geo['lat'])
            geometry_type = ogr.wkbPoint
        else:
            wkt = "POLYGON (({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))".format(geo['xmin'], geo['ymin'], geo['xmax'], geo['ymax'])
            geometry_type = ogr.wkbPolygon

        # Drop this field if it exists (not required).
        if 'geometry_type' in fields:
            fields.pop('geometry_type')

        # Geometry type and title (if available).
        title = None
        if 'title' in fields:
            title = fields['title']
            fields.pop('title')

        # Each unique location is a new shapefile or feature class.
        if location not in locations:
            locations.add(location)
            titles.add(title)
            location_dir = os.path.join(output_folder, location)
            os.mkdir(location_dir)
            field_names = create_shapefile(location_dir, title, fields, geometry_type, geo['code'])
        # Create a new shapefile for each different title.
        elif title not in titles:
            titles.add(title)
            field_names = create_shapefile(location_dir, title, fields, geometry_type, geo['code'])
        else:
            # Open existing shapefile.
            shape_file = ogr.Open(os.path.join(location_dir, title + '.shp'), 1)
            layer = shape_file.GetLayer()
            layer_def = layer.GetLayerDefn()
            names = [layer_def.GetFieldDefn(i).GetName() for i in range(layer_def.GetFieldCount())]
            field_names = {}
            for k, v in fields.iteritems():
                if [any(pre) for pre in ('ff_', 'fl_', 'fi_', 'fu_', 'fd_', 'fs_', 'meta_') if k.startswith(pre)]:
                    n = '_'.join(k.split('_')[1:])[0:10]  # Shapefile field names cannot be longer than 10 chars.
                    if n in names:
                        field_names[n] = v
                elif k in names:
                    field_names[k] = v

        if not shape_file:
            shape_file = ogr.Open(os.path.join(location_dir, title + '.shp'), 1)
            layer = shape_file.GetLayer()
            layer_def = layer.GetLayerDefn()

        # Point or bbox or WKT?
        geom = ogr.CreateGeometryFromWkt(wkt)
        feature = ogr.Feature(layer_def)
        feature.SetGeometry(geom)
        for field, value in field_names.iteritems():
            i = feature.GetFieldIndex(field)
            feature.SetField(i, value)
        layer.CreateFeature(feature)
        shape_file.Destroy()
        shape_file = None
        if (cnt % increment) == 0:
            status_writer.send_percent(float(cnt)/num_jobs,
                                       '{0}: {1:%}'.format("exported", float(cnt)/num_jobs),
                                       'export_features')


def export_to_geodatabase(jobs, output_workspace):
    """Export features to a geodatabase.
    :param jobs: list of jobs (a job contains the feature information)
    :param output_workspace:
    """
    import arcpy
    layers = []
    num_jobs = len(jobs)
    increment = task_utils.get_increment(num_jobs)
    for cnt, job in enumerate(jobs, 1):
        job_info = job['_job']
        job_info = eval(job_info.replace('null', 'None'))
        location = job_info['location']

        # Get the field information.
        fields = job_info['entry']['fields']

        # Get the geographic information.
        if 'geo' in job_info['entry']:
            geo = job_info['entry']['geo']
        else:
            status_writer.send_state(status.STAT_WARNING, _("{0} has no geographic information").format(job['id']))
            continue

        if 'wkt' in geo:
            feature = geo['wkt']
            if 'MULTIPOINT' in feature:
                geometry_type = "MULTIPOINT"
            elif 'MULTILINESTRING' in feature or 'LINESTRING' in feature:
                geometry_type = "POLYLINE"
            elif "MULTIPOLYGON" in feature or "POLYGON" in feature:
                geometry_type = "POLYGON"
            elif 'POINT' in feature:
                geometry_type = "POINT"
        elif 'lon' in geo:
            feature = "POINT ({0} {1})".format(geo['lon'], geo['lat'])
            geometry_type = 'POINT'
        else:
            geometry_type = 'POLYGON'
            feature = "POLYGON (({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))".format(geo['xmin'], geo['ymin'], geo['xmax'], geo['ymax'])

        if 'geometry_type' in fields:
            fields.pop('geometry_type')

        # Get the title (if available).
        title = None
        if 'title' in fields:
            title = fields['title']
            fields.pop('title')

        # Each unique location is a new geodatabase.
        if location not in locations:
            locations.add(location)
            titles.add(title)
            gdb = arcpy.CreateFileGDB_management(output_workspace, "{0}.gdb".format(location))
            fc = arcpy.CreateFeatureclass_management(gdb, title, geometry_type, spatial_reference=int(geo['code']))
            layer = arcpy.MakeFeatureLayer_management(fc, title)
            field_names = add_fields(layer, fields)
            layers.append(layer)
        # Create a feature class for each new title.
        elif title not in titles:
            titles.add(title)
            fc = arcpy.CreateFeatureclass_management(gdb, title, geometry_type, spatial_reference=int(geo['code']))
            layer = arcpy.MakeFeatureLayer_management(fc, title)
            field_names = add_fields(layer, fields)
            layers.append(layer)
        else:
            layer = [l for l in layers if l[0].name == title][0]
            names = [f.name for f in arcpy.ListFields(layer)]
            field_names = {}
            for k, v in fields.iteritems():
                if [any(pre) for pre in ('ff_', 'fl_', 'fi_', 'fu_', 'fd_', 'fs_', 'meta_') if k.startswith(pre)]:
                    n = '_'.join(k.split('_')[1:])
                    if n in names:
                        field_names[n] = v
                elif k in names:
                    field_names[k] = v

        with arcpy.da.InsertCursor(layer, ['SHAPE@WKT'] + field_names.keys()) as icur:
            icur.insertRow([feature] + field_names.values())

        if (cnt % increment) == 0:
            status_writer.send_percent(float(cnt)/num_jobs,
                                       '{0}: {1:%}'.format("exported", float(cnt)/num_jobs),
                                       'export_features')


def execute(request):
    """Exports features and rows to a shapefile or geodatabase.
    :param request: json as a dict.
    """
    all_jobs = request['params'][0]['response']['docs']
    out_format = task_utils.get_parameter_value(request['params'], 'output_format', 'value')

    # Create a task folder if it does not exist.
    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    if out_format == 'SHP':
        export_to_shapefiles(all_jobs, task_folder)
    else:
        export_to_geodatabase(all_jobs, task_folder)

    # Zip up outputs.
    task_utils.zip_data(task_folder, 'output.zip')
