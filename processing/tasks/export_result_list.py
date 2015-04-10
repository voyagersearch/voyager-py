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
import csv
import shutil
import urllib2
import datetime
import xml.etree.cElementTree as et
from utils import status
from utils import task_utils
from tasks import _

SHAPE_FIELD_LENGTH = slice(0, 10)

status_writer = status.Writer()


def export_to_shp(jobs, file_name, output_folder):
    """Exports results to a shapefile.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    import ogr
    driver = ogr.GetDriverByName("ESRI Shapefile")
    shape_file = driver.CreateDataSource(os.path.join(output_folder, '{0}.shp'.format(file_name)))
    epsg_code = 4326
    srs = ogr.osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    layer = shape_file.CreateLayer(str(file_name), srs, ogr.wkbPolygon)
    for name in jobs[0].keys():
        if not name == '[geo]':
            name = str(name)
            new_field = ogr.FieldDefn(name, ogr.OFTString)
            layer.CreateField(new_field)

    layer_def = layer.GetLayerDefn()
    for job in jobs:
        try:
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
        for field, value in job.iteritems():
            field, value = str(field), str(value)
            i = feature.GetFieldIndex(field)
            feature.SetField(i, value)
        layer.CreateFeature(feature)
    shape_file.Destroy()
    shape_file = None


def export_to_csv(jobs, file_name, output_folder):
    """
    Exports result to a CSV file.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    with open(os.path.join(output_folder, '{0}.csv'.format(file_name)), 'wb') as csv_file:
        field_names = jobs[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        for cnt, job in enumerate(jobs, 1):
            writer.writerow(job)


def export_to_xml(jobs, file_name, output_folder):
    """
    Exports results to a XML file.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    """
    comment = et.Comment('{0}'.format(datetime.datetime.today().strftime('Exported: %c')))
    results = et.Element('results')
    for job in jobs:
        result = et.SubElement(results, 'result')
        for key, val in job.items():
            child = et.SubElement(result, key)
            child.text = str(val)

    tree = et.ElementTree(results)
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

    num_results, response_index = task_utils.get_result_count(request['params'])
    if out_format in ('CSV', 'XML'):
        fields.remove('[geo]')
        query = '{0}/select?&wt=json&fl={1}'.format(sys.argv[2].split('=')[1], ','.join(fields))
    else:
        query = '{0}/select?&wt=json&fl={1}'.format(sys.argv[2].split('=')[1], ','.join(fields))
    if 'query' in request['params'][response_index]:
        # Voyager Search Traditional UI
        for p in request['params']:
            if 'query' in p:
                request_qry = p['query']
                break
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
        exported_cnt = 0.
        for i in xrange(0, num_results, chunk_size):
            for n in urllib2.urlopen(query.format(chunk_size, i)):
                jobs = eval(n)['response']['docs']
                if out_format == 'CSV':
                    export_to_csv(jobs, file_name, task_folder)
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
            i += len(group)
            results = urllib2.urlopen(query + '&ids={0}'.format(','.join(group)))
            jobs = eval(results.read())['response']['docs']
            if out_format == 'CSV':
                export_to_csv(jobs, file_name, task_folder)
            elif out_format == 'XML':
                export_to_xml(jobs, file_name, task_folder)
            elif out_format == 'SHP':
                export_to_shp(jobs, file_name, task_folder)
            status_writer.send_percent(float(i) / num_results,
                                       '{0}: {1:%}'.format("exported", float(i) / num_results), 'export_features')

    # Zip up outputs.
    zip_file = task_utils.zip_data(task_folder, 'output.zip')
    shutil.move(zip_file, os.path.join(os.path.dirname(task_folder), os.path.basename(zip_file)))
