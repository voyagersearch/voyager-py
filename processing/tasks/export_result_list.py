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
import re
import sys
import json
import itertools
import csv
import shutil
import datetime
import string
import xml.etree.cElementTree as et
import requests
import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

from utils import status
from utils import task_utils


SHAPE_FIELD_LENGTH = slice(0, 10)
errors_reasons = {}
skipped_reasons = {}
exported_count = 0.
errors_count = 0.
status_writer = status.Writer()

try:
    text_characters = "".join(map(chr, range(32, 127)) + list("\n\r\t\b"))
except TypeError:
    text_characters = "".join(list(map(chr, range(32, 127))) + list("\n\r\t\b"))
try:
    _null_trans = string.maketrans("", "")
except AttributeError:
    pass

field_prefixes = ('fs_', 'fi_', 'fd_', 'fl_', 'fb_', 'fu_', 'ff_', 'fss_', 'meta_')


def remove(key):
    if key.startswith(field_prefixes):
        return key.replace(filter(key.startswith, field_prefixes)[0], '')
    else:
        return key


def change(val, encoding_type):
    try:
        if isinstance(val, (str, unicode)):
            return val.encode(encoding_type)
    except Exception:
        return val.encode(encoding_type)
    else:
        if isinstance(val, list):
            val = ';'.join(val).encode(encoding_type)
        return val


def is_ascii(filename, blocksize=512):
    return is_text(open(filename).read(blocksize))


def is_text(s):
    if "\0" in s:
        return 0
    if not s:  # Empty files are considered text
        return 1
    # Get the non-text characters (maps a character to itself then
    # use the 'remove' option to get rid of the text characters.)
    try:
        t = s.translate(_null_trans, text_characters)
    except TypeError:
        t = ''.join(c for c in s if c.isalpha())
    # If more than 30% non-text characters, then
    # this is considered a binary file
    if len(t) / len(s) > 0.30:
        return 0
    return 1


def export_to_shp(jobs, file_name, output_folder):
    """Exports results to a shapefile.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    global exported_count
    global errors_count

    from osgeo import ogr
    from osgeo import osr
    # os.environ['GDAL_DATA'] = r'C:\voyager\server_2-1381\app\gdal'
    driver = ogr.GetDriverByName("ESRI Shapefile")
    for job in jobs:
        try:
            geometry_type = None
            if '[geo]' not in job:
                errors_count += 1
                status_writer.send_state(status.STAT_WARNING, 'No Geometry field')
                status_writer.send_state(status.STAT_WARNING)
                continue
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
            status_writer.send_state(status.STAT_WARNING, 'No Geometry field')
            continue
        except TypeError as te:
            errors_count += 1
            status_writer.send_state(status.STAT_WARNING, 'No Geometry field')
            status_writer.send_state(status.STAT_WARNING)
            continue

        if os.path.exists(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type']))):
            shape_file = ogr.Open(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type'])), 1)
            layer = shape_file.GetLayer()
        else:
            shape_file = driver.CreateDataSource(os.path.join(output_folder, '{0}_{1}.shp'.format(file_name, geo_json['type'])))
            epsg_code = 4326
            srs = osr.SpatialReference()
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
            if not geom:
                geom = ogr.CreateGeometryFromJson("{0}".format(json.dumps(job['[geo]'])))
            feature.SetGeometry(geom)
        except KeyError:
            feature.SetGeometry(None)
            pass
        try:
            job.pop('[geo]')
        except KeyError:
            pass

        try:
            for field, value in job.items():
                field, value = str(field), str(value)
                i = feature.GetFieldIndex(field[0:10])
                feature.SetField(i, value)
            layer.CreateFeature(feature)
            exported_count += 1
            shape_file.Destroy()
        except Exception as ex:
            errors_count += 1
            status_writer.send_state(status.STAT_WARNING, 'No Geometry field')
            shape_file.Destroy()
            continue

def export_to_csv(jobs, file_name, output_folder, fields):
    """
    Exports result to a CSV file.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    global exported_count
    global errors_count

    file_name_new = file_name.encode('ascii', 'ignore')
    if not file_name_new:
        file_path = output_folder + os.sep + file_name + '.csv'
    else:
        file_path = os.path.join(output_folder, '{0}.csv'.format(file_name_new))
    if os.path.exists(file_path):
        write_keys = False
    else:
        write_keys = True
    with open(file_path, 'a') as csv_file:
        if 'location:[localize]' in fields:
            i = fields.index('location:[localize]')
            fields.remove('location:[localize]')
            fields.insert(i, 'location')
        if 'path[absolute]' in fields:
            i = fields.index('path[absolute]')
            fields.remove('path[absolute]')
            fields.insert(i, '[absolute]')

        for f in fields:
            pre = filter(f.startswith, field_prefixes)
            try:
                prefix = next(pre)
            except StopIteration:
                continue
            except TypeError:
                if pre:
                    prefix = pre[0]
                else:
                    prefix = None
            if prefix:
                i = fields.index(f)
                fields.remove(f)
                fields.insert(i, f.replace(prefix, ''))
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        if write_keys:
            writer.writeheader()
        for cnt, job in enumerate(jobs, 1):
            try:
                try:
                    encoded_job = {remove(k): v for (k, v) in job.items()}
                except Exception:
                    encoded_job = {remove(k): change(v, 'utf-8') for (k, v) in job.items()}
                writer.writerow(encoded_job)
                exported_count += 1
            except Exception as ex:
                errors_count += 1
                errors_reasons['error'] = repr(ex)
                continue


def export_to_xml(jobs, file_name, output_folder):
    """
    Exports results to a XML file.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    global exported_count
    global errors_count
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
                errors_reasons['error'] = repr(ex)
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
                errors_reasons['error'] = repr(ex)
                continue
    tree.getroot().insert(0, comment)
    tree.write(os.path.join(output_folder, "{0}.xml".format(file_name)), encoding='UTF-8')


def execute(request):
    """Exports search results a CSV, shapefile or XML document.
    :param request: json as a dict.
    """

    # Get SSL trust setting.
    verify_ssl = task_utils.get_ssl_mode()

    chunk_size = task_utils.CHUNK_SIZE

    file_name = task_utils.get_parameter_value(request['params'], 'file_name', 'value')
    fields = task_utils.get_parameter_value(request['params'], 'fields', 'value')
    out_format = task_utils.get_parameter_value(request['params'], 'output_format', 'value')

    if not 'path' in fields and 'path:[absolute]' in fields:
        fields.append('path')

    if 'geo' in fields:
        i_geo = fields.index('geo')
        fields.remove('geo')
        fields.insert(i_geo, '[geo]')

    # Create the temporary workspace.
    task_folder = os.path.join(request['folder'], 'temp')
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)

    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    num_results, response_index = task_utils.get_result_count(request['params'])

    if len(sys.argv) == 2:
        query = '{0}/solr/v0/select?&wt=json&fl={1}'.format('http://localhost:8888', ','.join(fields))
    else:
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
                if isinstance(request_qry['fq'], list):
                    for fq in request_qry['fq']:
                        try:
                            query += '&fq={0}'.format(str(fq))
                        except UnicodeEncodeError:
                            query += '&fq={0}'.format(str(fq.encode('utf-8')))
                else:
                    query += '&fq={0}'.format(request_qry['fq'])
                if '{!expand}' in query:
                    query = query.replace('{!expand}', '')
                if '{!tag' in query:
                    tag = re.findall('{!(.*?)}', query)
                    if tag:
                        tag_str = "{!" + tag[0] + "}"
                        query = query.replace(tag_str, '')
                query = query.replace(' ', '%20')
            except AttributeError:
                for qry in request_qry['fq']:
                    query += '&fq={0}'.format(qry).replace("\\", "").replace(' ', '%20')
        if 'q' in request_qry:
            try:
                query += '&q={0}'.format(request_qry['q'].replace("\\", ""))
                query = query.replace(' ', '%20')
            except UnicodeEncodeError:
                query += '&q={0}'.format(request_qry['q'].encode('utf-8').replace("\\", ""))
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
        for i in range(0, num_results, chunk_size):
            url = query.replace('{0}', str(chunk_size)).replace('{1}', str(i))
            res = requests.get(url, verify=verify_ssl, headers=headers)

            jobs = res.json()['response']['docs']
            if out_format == 'CSV':
                export_to_csv(jobs, file_name, task_folder, fields)
            elif out_format == 'XML':
                export_to_xml(jobs, file_name, task_folder)
            elif out_format == 'SHP':
                export_to_shp(jobs, file_name, task_folder)
            exported_cnt += chunk_size
            if exported_cnt > num_results:
                status_writer.send_percent(100, 'exported: 100%', 'export_results')
            else:
                percent_done = exported_cnt / num_results
                status_writer.send_percent(percent_done, '{0}: {1:.0f}%'.format("exported", percent_done * 100), 'export_results')
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
            results = requests.get(query + '&ids={0}'.format(','.join(group)), verify=verify_ssl, headers=headers)
            jobs = eval(results.text)['response']['docs']
            if out_format == 'CSV':
                export_to_csv(jobs, file_name, task_folder, fields)
            elif out_format == 'XML':
                export_to_xml(jobs, file_name, task_folder)
            elif out_format == 'SHP':
                export_to_shp(jobs, file_name, task_folder)
            percent_done = float(i) / num_results
            status_writer.send_percent(percent_done, '{0}: {1:.0f}%'.format("exported", percent_done * 100), 'export_results')

    # Zip up outputs.
    if exported_count == 0:
        status_writer.send_state(status.STAT_FAILED)
        task_utils.report(os.path.join(request['folder'], '__report.json'), exported_count, 0, errors_count, errors_reasons)
    else:
        task_utils.report(os.path.join(request['folder'], '__report.json'), exported_count, 0, errors_count, errors_reasons)
        zip_file = task_utils.zip_data(task_folder, '{0}.zip'.format(file_name))
        shutil.move(zip_file, os.path.join(os.path.dirname(task_folder), os.path.basename(zip_file)))
