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
import shutil
import requests
import csv
import xlrd
import arcpy
from utils import status
from utils import task_utils


class LicenseError(Exception):
    """Custom License error exception."""
    pass


# This code determines if the LocateXT ArcGIS server tool is installed and licensed.
arcgis_version = arcpy.GetInstallInfo()['Version']
if '10.3' in arcgis_version:
    arcpy.ImportToolbox(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'locatext', 'LocateXT103.tbx'))
elif '10.2' in arcgis_version or '10.1' in arcgis_version:
    arcpy.ImportToolbox(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'locatext', 'LocateXT102.tbx'))
else:
    arcpy.ImportToolbox(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'locatext', 'LocateXT10.tbx'))
try:
    arcpy.LocateXT_Tool_lxt
except RuntimeError as re:
    if 'ERROR 000824' in re.message:
        raise task_utils.LicenseError('No LocateXT License')
    else:
        pass

status_writer = status.Writer()
result_count = 0
processed_count = 0.
skipped_reasons = {}
errors_reasons = {}


def xls_to_csv(excel_file):
    """Coverts an excel file to a csv file."""
    workbook = xlrd.open_workbook(excel_file)
    worksheets = workbook.sheet_names()
    for worksheet_name in worksheets:
        worksheet = workbook.sheet_by_name(worksheet_name)
        with open(os.path.join(os.path.dirname(excel_file), '{0}.csv'.format(worksheet_name)), 'wb') as csv_file:
            csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            for row_num in xrange(worksheet.nrows):
                csv_writer.writerow([unicode(entry).encode("utf-8") for entry in worksheet.row_values(row_num)])


def execute(request):
    """Copies files to a target folder.
    :param request: json as a dict.
    """
    extracted = 0
    skipped = 0
    errors = 0
    global result_count
    parameters = request['params']

    output_type = task_utils.get_parameter_value(parameters, 'output_format', 'value')
    task_folder = os.path.join(request['folder'], 'temp')
    if not os.path.exists(task_folder):
        os.makedirs(task_folder)
        if output_type == 'FGDB':
            arcpy.CreateFileGDB_management(task_folder, 'output.gdb')

    result_count, response_index = task_utils.get_result_count(parameters)
    # Query the index for results in groups of 25.
    query_index = task_utils.QueryIndex(parameters[response_index])
    fl = query_index.fl
    query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
    # query = '{0}{1}{2}'.format("http://localhost:8888/solr/v0", '/select?&wt=json', fl)
    fq = query_index.get_fq()
    if fq:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')
        query += fq
    elif 'ids' in parameters[response_index]:
        groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')
    else:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')

    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    status_writer.send_percent(0.0, _('Starting to process...'), 'locate_xt_arcgis_tool')
    for group in groups:
        if fq:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
        elif 'ids' in parameters[response_index]:
            results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
        else:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

        docs = results.json()['response']['docs']
        if not docs:
            docs = parameters[response_index]['response']['docs']

        input_items = task_utils.get_input_items(docs)
        if input_items:
            result = extract(input_items, output_type, task_folder)
            extracted += result[0]
            errors += result[1]
            skipped += result[2]
        else:
            status_writer.send_state(status.STAT_FAILED, _('No items to process. Check if items exist.'))
            return

    # Zip up outputs.
    zip_file = task_utils.zip_data(task_folder, 'output.zip')
    shutil.move(zip_file, os.path.join(os.path.dirname(task_folder), os.path.basename(zip_file)))

    # Update state if necessary.
    if errors > 0 or skipped > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    task_utils.report(os.path.join(request['folder'], '__report.json'), extracted, skipped, errors, errors_reasons, skipped_reasons)


def extract(input_items, out_type, output_dir):
    """Extract geographic information from input items."""
    extracted = 0
    skipped = 0
    errors = 0
    global processed_count

    # Get the LocateXT GP Toolbox
    for src_file in input_items:
        processed_count += 1
        try:
            if os.path.isfile(src_file):
                file_name = arcpy.ValidateTableName(os.path.basename(os.path.splitext(src_file)[0]))
                if out_type == 'CSV':
                    shp_file = os.path.join(output_dir, '{0}.shp'.format(file_name))
                    arcpy.LocateXT_Tool_lxt(src_file, shp_file)
                    xls_file = os.path.join(output_dir, '{0}.xls'.format(file_name))
                    arcpy.TableToExcel_conversion(shp_file, xls_file)
                    xls_to_csv(xls_file)
                    arcpy.Delete_management(shp_file)
                    arcpy.Delete_management(xls_file)
                elif out_type == 'KML':
                    shp_file = os.path.join(output_dir, '{0}.shp'.format(file_name))
                    arcpy.LocateXT_Tool_lxt(src_file, shp_file)
                    layer_name = os.path.basename(shp_file)[:-4]
                    arcpy.MakeFeatureLayer_management(shp_file, layer_name)
                    arcpy.LayerToKML_conversion(layer_name, '{0}.kmz'.format(os.path.join(output_dir, layer_name)), 1)
                    arcpy.Delete_management(shp_file)
                elif out_type == 'SHP':
                    arcpy.LocateXT_Tool_lxt(src_file, os.path.join(output_dir, '{0}.shp'.format(file_name)))
                elif out_type == 'FGDB':
                    arcpy.LocateXT_Tool_lxt(src_file, os.path.join(output_dir, 'output.gdb', file_name))

                status_writer.send_percent(processed_count / result_count, _('Extracted: {0}').format(src_file), 'locate_xt_arcgis_tool')
                extracted += 1
            else:
                status_writer.send_percent(processed_count / result_count, _('{0} is not a supported file type or does no exist').format(src_file), 'locate_xt_arcgis_tool')
                skipped += 1
                skipped_reasons[src_file] = _('{0} is not a supported file type or does no exist').format(os.path.basename(src_file))
        except IOError as io_err:
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(src_file), 'locate_xt_arcgis_tool')
            status_writer.send_status(_('FAIL: {0}').format(repr(io_err)))
            errors += 1
            errors_reasons[src_file] = repr(io_err)
            pass
        except arcpy.ExecuteError:
            status_writer.send_status(_('FAIL: {0}').format(arcpy.GetMessages(2)))
            errors += 1
            errors_reasons[src_file] = arcpy.GetMessages(2)
            pass
    return extracted, errors, skipped
