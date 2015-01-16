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
import sys
import urllib2
import tasks
from utils import status
from utils import task_utils
from tasks import _


gdb = ''
layers = []
locations = set()
location_dir = ''
titles = set()

SHAPE_FIELD_LENGTH = slice(0, 10)

status_writer = status.Writer()


def add_fields(layer_name, fields):
    """Add fields to a geodatabase feature class.
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
    :param layer_name: shapefile name without .shp extension
    :param fields: dictionary of name and type
    :param shape_type: the type of geometry
    :param epsg_code: Spatial reference code (i.e. 4326)
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
    global location_dir
    shape_file = None

    for cnt, job in enumerate(jobs, 1):
        location = job['location']

        # Get the geographic information (if available)
        if 'srs_code' in job:
            srs_code = int(job['srs_code'])
            job.pop('srs_code')
        else:
            status_writer.send_state(status.STAT_WARNING, _("{0} has no geographic information").format(job['id']))
            continue

        job.pop('id')
        geometry_type = None
        if 'wkt' in job:
            wkt = job['wkt']
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
        elif 'pointDD' in job:
            lon, lat = job['pointDD'].split()
            wkt = "POINT ({0} {1})".format(float(lon), float(lat))
            geometry_type = ogr.wkbPoint
            job.pop('pointDD')
            job.pop('bbox')
        else:
            xmin, ymin, xmax, ymax = [float(x) for x in job['bbox'].split()]
            wkt = "POLYGON (({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))".format(xmin, ymin, xmax, ymax)
            geometry_type = ogr.wkbPolygon
            job.pop('bbox')

        # Drop this field if it exists (not required).
        if 'geometry_type' in job:
            job.pop('geometry_type')

        # Geometry type and title (if available).
        title = None
        if 'title' in job:
            title = job['title']
            job.pop('title')

        # Each unique location is a new shapefile or feature class.
        if location not in locations:
            locations.add(location)
            titles.add(title)
            location_dir = os.path.join(output_folder, location)
            os.mkdir(location_dir)
            field_names = create_shapefile(location_dir, title, job, geometry_type, srs_code)
        # Create a new shapefile for each different title.
        elif title not in titles:
            titles.add(title)
            field_names = create_shapefile(location_dir, title, job, geometry_type, srs_code)
        else:
            # Open existing shapefile.
            shape_file = ogr.Open(os.path.join(location_dir, title + '.shp'), 1)
            layer = shape_file.GetLayer()
            layer_def = layer.GetLayerDefn()
            names = [layer_def.GetFieldDefn(i).GetName() for i in range(layer_def.GetFieldCount())]
            field_names = {}
            for k, v in job.iteritems():
                if [any(pre) for pre in ('ff_', 'fl_', 'fi_', 'fu_', 'fd_', 'fs_', 'meta_') if k.startswith(pre)]:
                    # Shapefile field names cannot be longer than 10 chars. Slice first 10 characters.
                    n = '_'.join(k.split('_')[1:])[SHAPE_FIELD_LENGTH]
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


def export_to_geodatabase(jobs, output_workspace):
    """Export features to a geodatabase.
    :param jobs: list of jobs (a job contains the feature information)
    :param output_workspace:
    """
    import arcpy
    global gdb

    for cnt, job in enumerate(jobs, 1):
        location = job['location']

        # Get the geographic information.
        if 'srs_code' in job:
            srs_code = int(job['srs_code'])
            job.pop('srs_code')
        else:
            status_writer.send_state(status.STAT_WARNING, _("{0} has no geographic information").format(job['id']))
            continue
        job.pop('id')
        if 'wkt' in job:
            feature = job['wkt']
            if 'MULTIPOINT' in feature:
                geometry_type = "MULTIPOINT"
            elif 'MULTILINESTRING' in feature or 'LINESTRING' in feature:
                geometry_type = "POLYLINE"
            elif "MULTIPOLYGON" in feature or "POLYGON" in feature:
                geometry_type = "POLYGON"
            elif 'POINT' in feature:
                geometry_type = "POINT"
        elif 'pointDD' in job:
            lon, lat = job['pointDD'].split()
            feature = "POINT ({0} {1})".format(float(lon), float(lat))
            geometry_type = 'POINT'
            job.pop('pointDD')
            job.pop('bbox')
        else:
            geometry_type = 'POLYGON'
            xmin, ymin, xmax, ymax = [float(x) for x in job['bbox'].split()]
            feature = "POLYGON (({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))".format(xmin, ymin, xmax, ymax)
            job.pop('bbox')

        # Get the title (if available).
        title = None
        if 'title' in job:
            title = job['title']
            job.pop('title')

        # Each unique location is a new geodatabase.
        if location not in locations:
            locations.add(location)
            titles.add(title)
            gdb = arcpy.CreateFileGDB_management(output_workspace, "{0}.gdb".format(location))
            fc = arcpy.CreateFeatureclass_management(gdb, title, geometry_type, spatial_reference=srs_code)
            layer = arcpy.MakeFeatureLayer_management(fc, title)
            field_names = add_fields(layer, job)
            layers.append(layer)
        # Create a feature class for each new title.
        elif title not in titles:
            titles.add(title)
            fc = arcpy.CreateFeatureclass_management(gdb, title, geometry_type, spatial_reference=srs_code)
            layer = arcpy.MakeFeatureLayer_management(fc, title)
            field_names = add_fields(layer, job)
            layers.append(layer)
        else:
            layer = [l for l in layers if l[0].name == title][0]
            names = [f.name for f in arcpy.ListFields(layer)]
            field_names = {}
            for k, v in job.iteritems():
                if [any(pre) for pre in ('ff_', 'fl_', 'fi_', 'fu_', 'fd_', 'fs_', 'meta_') if k.startswith(pre)]:
                    n = '_'.join(k.split('_')[1:])
                    if n in names:
                        field_names[n] = v
                elif k in names:
                    field_names[k] = v

        with arcpy.da.InsertCursor(layer, ['SHAPE@WKT'] + field_names.keys()) as icur:
            icur.insertRow([feature] + field_names.values())


def execute(request):
    """Exports features and rows to a shapefile or geodatabase.
    :param request: json as a dict.
    """
    chunk_size = 25
    out_format = task_utils.get_parameter_value(request['params'], 'output_format', 'value')

    # Create a task folder if it does not exist.
    task_folder = request['folder']
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    num_results = request['params'][0]['response']['numFound']
    if num_results > chunk_size:
        query = '{0}{1}'.format(sys.argv[2].split('=')[1], '/select?&wt=json&fl=id,location,name,title,fs_*,fl_*,fi_*,ff_*,fu_*,fd*_,meta_*,pointDD,bbox,srs_code')
        if 'query' in request['params'][0]:
            # Voyager Search Traditional UI
            request_qry = request['params'][0]['query']
            if 'voyager.list' in request_qry:
                query += '&{0}'.format(request_qry['voyager.list'])
            if 'fq' in request_qry:
                if isinstance(request_qry['fq'], list):
                    query += '&fq={0}'.format('&fq='.join(request_qry['fq']).replace('\\', ''))
                    query = query.replace(' ', '%20')
                else:
                    # Replace spaces with %20 & remove \\ to avoid HTTP Error 400.
                    query += '&fq={0}'.format(request_qry['fq'].replace("\\", ""))
                    query = query.replace(' ', '%20')
            query += '&rows={0}&start={1}'
            for i in xrange(0, num_results, chunk_size):
                for n in urllib2.urlopen(query.format(chunk_size, i)):
                    jobs = eval(n)['response']['docs']
                    if out_format == 'SHP':
                        export_to_shapefiles(jobs, task_folder)
                    else:
                        export_to_geodatabase(jobs, task_folder)
                #if (cnt % increment) == 0:
                status_writer.send_percent(float(i) / num_results,
                                           '{0}: {1:%}'.format("exported", float(i) / num_results), 'export_features')
        else:
            # Voyager Search Portal/Cart UI
            ids = request['params'][0]['ids']
            groups = task_utils.grouper(list(ids), chunk_size, '')
            i = 0
            for group in groups:
                i += len(group)
                results = urllib2.urlopen(query + '&ids={0}'.format(','.join(group)))
                jobs = eval(results.read())['response']['docs']
                if out_format == 'SHP':
                    export_to_shapefiles(jobs, task_folder)
                else:
                    export_to_geodatabase(jobs, task_folder)
                status_writer.send_percent(float(i) / num_results,
                                           '{0}: {1:%}'.format("exported", float(i) / num_results), 'export_features')
    else:
        jobs = request['params'][0]['response']['docs']
        if out_format == 'SHP':
            export_to_shapefiles(jobs, task_folder)
        else:
            export_to_geodatabase(jobs, task_folder)

    # Zip up outputs.
    task_utils.zip_data(task_folder, 'output.zip')
