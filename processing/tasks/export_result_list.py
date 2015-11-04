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
import itertools
import csv
import shutil
import urllib2
import datetime
import xml.etree.cElementTree as et
from utils import status
from utils import task_utils


SHAPE_FIELD_LENGTH = slice(0, 10)
errors_reasons = {}
skipped_reasons = {}
exported_count = 0.
errors_count = 0.
status_writer = status.Writer()


def export_to_shp(jobs, file_name, output_folder):
    """Exports results to a shapefile.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    global exported_count
    global errors_count
    try:
        import ogr
    except ImportError as ie:
        errors_count += 1
        exported_count = 0
        errors_reasons['Import Error'] = ie.message
        status_writer.send_state(status.STAT_FAILED, repr(ie))
        return

    driver = ogr.GetDriverByName("ESRI Shapefile")
    for job in jobs:
        try:
            geo_json = job['[geo]']
            if geo_json['type'].lower() == 'polygon':
                geometry_type = ogr.wkbPolygon
            elif geo_json['type'].lower() == 'geometrycollection':
                geom = ogr.CreateGeometryFromJson("{0}".format(job['[geo]']))
                if geom.GetDimension() == 0:
                    geometry_type = ogr.wkbPoint
                elif geom.GetDimension() == 1:
                    geometry_type = ogr.wkbLineString
                else:
                    geometry_type = ogr.wkbPolygon
            elif geo_json['type'].lower() == 'multipolygon':
                geometry_type = ogr.wkbMultiPolygon
            elif geo_json['type'].lower() == 'linestring':
                geometry_type = ogr.wkbLineString
            elif geo_json['type'].lower() == 'multilinestring':
                geometry_type = ogr.wkbMultiLineString
            elif geo_json['type'].lower() == 'point':
                geometry_type = ogr.wkbPoint
            elif geo_json['type'].lower() == 'multipoint':
                geometry_type = ogr.wkbMultiPoint
        except KeyError as ke:
            errors_count += 1
            errors_reasons[job.values()[0]] = repr(ke)
            status_writer.send_state(status.STAT_WARNING)
            continue
        except TypeError as te:
            errors_count += 1
            errors_reasons[job.values()[0]] = repr(te)
            status_writer.send_state(status.STAT_WARNING)
            continue


        if os.path.exists(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type']))):
            shape_file = ogr.Open(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type'])), 1)
            layer = shape_file.GetLayer()
        else:
            shape_file = driver.CreateDataSource(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type'])))
            epsg_code = 4326
            srs = ogr.osr.SpatialReference()
            srs.ImportFromEPSG(epsg_code)
            layer = shape_file.CreateLayer('{0}_{1}'.format(file_name, geo_json['type']), srs, geometry_type)
            for name in jobs[0].keys():
                if not name == '[geo]':
                    name = str(name)
                    if name.startswith('fu_'):
                        new_field = ogr.FieldDefn(name, ogr.OFTReal)
                    elif name.startswith('fi_'):
                        new_field = ogr.FieldDefn(name, ogr.OFTInteger)
                    elif name.startswith('fl_'):
                        new_field = ogr.FieldDefn(name, ogr.OFTInteger64)
                    elif name.startswith('fd_'):
                        new_field = ogr.FieldDefn(name, ogr.OFTDateTime)
                    else:
                        new_field = ogr.FieldDefn(name, ogr.OFTString)
                    layer.CreateField(new_field)

        try:
            layer_def = layer.GetLayerDefn()
            feature = ogr.Feature(layer_def)
            geom = ogr.CreateGeometryFromJson("{0}".format(job['[geo]']))
            feature.SetGeometry(geom)
        except KeyError:
            feature.SetGeometry(None)
            pass
        try:
            job.pop('[geo]')
        except KeyError:
            pass

        try:
            for field, value in job.iteritems():
                field, value = str(field), str(value)
                i = feature.GetFieldIndex(field[0:10])
                feature.SetField(i, value)
            layer.CreateFeature(feature)
            shape_file.Destroy()
            shape_file = None
            exported_count += 1
        except Exception as ex:
            errors_count += 1
            errors_reasons[job.values()[0]] = repr(ex)
            shape_file = None
            continue


def export_to_csv(jobs, file_name, output_folder, fields):
    """
    Exports result to a CSV file.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    global exported_count
    global  errors_count
    write_keys = True
    if os.path.exists(os.path.join(output_folder, '{0}.csv'.format(file_name))):
        write_keys = False
    with open(os.path.join(output_folder, '{0}.csv'.format(file_name)), 'ab') as csv_file:
        if 'location:[localize]' in fields:
            i = fields.index('location:[localize]')
            fields.remove('location:[localize]')
            fields.insert(i, 'location')
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        if write_keys:
            writer.writeheader()
        for cnt, job in enumerate(jobs, 1):
            try:
                writer.writerow(job)
                exported_count += 1
            except Exception as ex:
                errors_count += 1
                errors_reasons[job.keys()[0]] = repr(ex)
                continue


def export_to_xml(jobs, file_name, output_folder):
    """
    Exports results to a XML file.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    global  exported_count
    global  errors_count
    comment = et.Comment('{0}'.format(datetime.datetime.today().strftime('Exported: %c')))
    if not os.path.exists(os.path.join(output_folder, "{0}.xml".format(file_name))):
        results = et.Element('results')
        for job in jobs:
            try:
                result = et.SubElement(results, 'result')
                for key, val in job.items():
                    if key == '[geo]':
                        child = et.SubElement(result, 'geo')
                        if 'geometries' in val:
                            geom_collection = et.SubElement(child, val['type'])
                            for geom in val['geometries']:
                                geom_part = et.SubElement(geom_collection, geom['type'])
                                for part in list(itertools.chain(*geom['coordinates'])):
                                    point = et.SubElement(geom_part, 'point')
                                    point.text = str(part).replace('[', '').replace(']', '')
                        else:
                            geom_parent = et.SubElement(child, val['type'])
                            try:
                                list_coords = list(itertools.chain(*val['coordinates']))
                            except TypeError:
                                list_coords = [val['coordinates']]
                            if list_coords:
                                for coords in list_coords:
                                    point = et.SubElement(geom_parent, 'point')
                                    point.text = str(coords).replace('[', '').replace(']', '')
                            else:
                                for coords in val['coordinates']:
                                    point.text = str(coords).replace('[', '').replace(']', '')
                        continue
                    child = et.SubElement(result, key)
                    child.text = str(val)

            except Exception as ex:
                errors_count += 1
                errors_reasons[job.keys()[0]] = repr(ex)
                continue
        exported_count += len(results)
        tree = et.ElementTree(results)
    else:
        tree = et.parse(os.path.join(output_folder, "{0}.xml".format(file_name)))
        root = tree.getroot()
        for job in jobs:
            try:
                result = et.SubElement(root, 'result')
                for key, val in job.items():
                    if key == '[geo]':
                        child = et.SubElement(result, 'geo')
                        if 'geometries' in val:
                            geom_collection = et.SubElement(child, val['type'])
                            for geom in val['geometries']:
                                geom_part = et.SubElement(geom_collection, geom['type'])
                                for part in list(itertools.chain(*geom['coordinates'])):
                                    point = et.SubElement(geom_part, 'point')
                                    point.text = str(part).replace('[', '').replace(']', '')
                        else:
                            geom_parent = et.SubElement(child, val['type'])
                            try:
                                list_coords = list(itertools.chain(*val['coordinates']))
                            except TypeError:
                                list_coords = [val['coordinates']]
                            if list_coords:
                                for coords in list_coords:
                                    point = et.SubElement(geom_parent, 'point')
                                    point.text = str(coords).replace('[', '').replace(']', '')
                            else:
                                for coords in val['coordinates']:
                                    point.text = str(coords).replace('[', '').replace(']', '')
                        continue
                    child = et.SubElement(result, key)
                    child.text = str(val)
                exported_count += 1
            except Exception as ex:
                errors_count += 1
                errors_reasons[job.keys()[0]] = repr(ex)
                continue
    tree.getroot().insert(0, comment)
    tree.write(os.path.join(output_folder, "{0}.xml".format(file_name)), encoding='UTF-8')

def execute(request):
    """Exports search results a CSV, shapefile or XML document.
    :param request: json as a dict.
    """
    chunk_size = task_utils.CHUNK_SIZE
    file_name = task_utils.get_parameter_value(request['params'], 'file_name', 'value')
    fields = task_utils.get_parameter_value(request['params'], 'fields', 'value')
    out_format = task_utils.get_parameter_value(request['params'], 'output_format', 'value')

    # Create the temporary workspace.
    task_folder = os.path.join(request['folder'], 'temp')
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    num_results, response_index = task_utils.get_result_count(request['params'])
    query = '{0}/select?&wt=json&fl={1}'.format(sys.argv[2].split('=')[1], ','.join(fields))
    if 'query' in request['params'][response_index]:
        # Voyager Search Traditional UI
        for p in request['params']:
            if 'query' in p:
                request_qry = p['query']
                break
        if 'voyager.list' in request_qry:
            query += '&voyager.list={0}'.format(request_qry['voyager.list'])

        # Replace spaces with %20 & remove \\ to avoid HTTP Error 400.
        if 'fq' in request_qry:
            try:
                query += '&fq={0}'.format(request_qry['fq'].replace("\\", ""))
                query = query.replace(' ', '%20')
            except AttributeError:
                for qry in request_qry['fq']:
                    query += '&fq={0}'.format(qry).replace("\\", "").replace(' ', '%20')
        if 'q' in request_qry:
            try:
                query += '&q={0}'.format(request_qry['q'].replace("\\", ""))
                query = query.replace(' ', '%20')
            except AttributeError:
                for qry in request_qry['q']:
                    query += '&q={0}'.format(qry).replace("\\", "").replace(' ', '%20')
        if 'place' in request_qry:
            try:
                query += '&place={0}'.format(request_qry['place'].replace("\\", ""))
                query = query.replace(' ', '%20')
            except AttributeError:
                for qry in request_qry['place']:
                    query += '&place={0}'.format(qry).replace("\\", "").replace(' ', '%20')
        if 'place.op' in request_qry:
            query += '&place.op={0}'.format(request_qry['place.op'])

        query += '&rows={0}&start={1}'
        exported_cnt = 0.
        for i in xrange(0, num_results, chunk_size):
            req = urllib2.Request(query.format(chunk_size, i), headers=headers)
            for n in urllib2.urlopen(req):
                jobs = eval(n.replace('null', '"null"'))['response']['docs']
                if out_format == 'CSV':
                    export_to_csv(jobs, file_name, task_folder, fields)
                elif out_format == 'XML':
                    export_to_xml(jobs, file_name, task_folder)
                elif out_format == 'SHP':
                    export_to_shp(jobs, file_name, task_folder)
                exported_cnt += chunk_size
                if exported_cnt > num_results:
                    status_writer.send_percent(100, '{0}: {1:%}'.format("exported", 1.0), 'export_results')
                else:
                    status_writer.send_percent(exported_cnt / num_results,
                                               '{0}: {1:%}'.format("exported", exported_cnt / num_results), 'export_results')
    else:
        # Voyager Search Portal/Cart UI
        ids = []
        for p in request['params']:
            if 'ids' in p:
                ids = p['ids']
                break
        groups = task_utils.grouper(list(ids), chunk_size, '')
        i = 0
        for group in groups:
            i += len([v for v in group if not v == ''])
            req = urllib2.Request(query + '&ids={0}'.format(','.join(group)), headers=headers)
            results = urllib2.urlopen(req)
            jobs = eval(results.read())['response']['docs']
            if out_format == 'CSV':
                export_to_csv(jobs, file_name, task_folder, fields)
            elif out_format == 'XML':
                export_to_xml(jobs, file_name, task_folder)
            elif out_format == 'SHP':
                export_to_shp(jobs, file_name, task_folder)
            status_writer.send_percent(float(i) / num_results,
                                       '{0}: {1:%}'.format("exported", float(i) / num_results), 'export_features')

    # Zip up outputs.
    if exported_count == 0:
        task_utils.report(os.path.join(request['folder'], '__report.json'), exported_count, 0, errors_count, errors_reasons)
    else:
        task_utils.report(os.path.join(request['folder'], '__report.json'), exported_count, 0, errors_count, errors_reasons)
    zip_file = task_utils.zip_data(task_folder, 'output.zip')
    shutil.move(zip_file, os.path.join(os.path.dirname(task_folder), os.path.basename(zip_file)))
