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
import datetime
import csv
import collections
import shutil
import requests
from utils import status
from utils import task_utils


errors_reasons = {}
skipped_reasons = {}
exported_count = 0.
errors_count = 0.
status_writer = status.Writer()


def get_field_type(value):
    if value.startswith('fs_'):
        value = 'str'
    elif value.startswith('fl_'):
        value = 'int'
    elif value.startswith('fu_'):
        value = 'float'
    elif value.startswith('fd_'):
        value = 'date'
    else:
        try:
            int(value)
            value = 'int'
        except ValueError:
            pass
        try:
            float(value)
            value = 'float'
        except ValueError:
            pass
        try:
            datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
            value = 'date'
        except ValueError:
            try:
                datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
                value = 'date'
            except ValueError:
                pass
        if type(value).__name__ == 'unicode':
            value = 'str'
        else:
            value = 'str'
    return value


def generate_report(results_file, out_file, fields):
    if 'location:[localize]' in fields:
        i = fields.index('location:[localize]')
        fields.remove('location:[localize]')
        fields.insert(i, 'location')
    if 'path[absolute]' in fields:
        i = fields.index('path[absolute]')
        fields.remove('path[absolute]')
        fields.insert(i, '[absolute]')

    nice_names = []
    for f in fields:
        if '_' in f:
            nf = ' '.join(f.split('_'))
            nice_names.append(nf.title())
        else:
            nice_names.append(f.title())

    # Zip field names into a dictionary of raw name and nice name.
    field_names = dict(zip(fields, nice_names))

    # Get unique value counts.
    fp = open(results_file, 'rb')
    reader = csv.DictReader(fp)
    data_capture = [row for row in reader]
    bins = [(j[0], j[1]) for i in data_capture for j in i.items()]

    with open(out_file, 'wb') as cf:
        writer = csv.writer(cf)
        writer.writerow(['Field', 'Raw', 'Type', 'Value', 'Count'])
        dd = collections.defaultdict(list)
        for k in sorted(list(set(bins))):
            cnt = bins.count(k)
            try:
                if not isinstance(eval(k[1]), list):
                    ft = get_field_type(k[1])
                    writer.writerow([field_names[k[0]], k[0], ft, k[1], cnt])
                else:
                    for val in eval(k[1]):
                        dd[k[0]].append(val)
            except (NameError, SyntaxError):
                ft = get_field_type(k[1])
                writer.writerow([field_names[k[0]], k[0], ft, k[1], cnt])

        if dd:
            for k, v in dd.items():
                cnts = collections.Counter(v)
                for x, y in cnts.items():
                    ft = get_field_type(x)
                    writer.writerow([field_names[k], k, ft, x, y])
    fp.close()
    os.remove(results_file)


def export_to_csv(jobs, file_name, output_folder, fields):
    """
    Exports results to a CSV file. This is used for generating the field usage report.
    :param jobs: list of jobs (a job contains the result information)
    :param file_name: the output file name
    :param output_folder: the output task folder
    :param fields: the list of voyager fields
    """
    global exported_count
    global errors_count
    write_keys = True
    if os.path.exists(os.path.join(output_folder, '{0}.csv'.format(file_name))):
        write_keys = False
    with open(os.path.join(output_folder, '{0}.csv'.format(file_name)), 'ab') as csv_file:
        if 'location:[localize]' in fields:
            i = fields.index('location:[localize]')
            fields.remove('location:[localize]')
            fields.insert(i, 'location')
        if 'path[absolute]' in fields:
            i = fields.index('path[absolute]')
            fields.remove('path[absolute]')
            fields.insert(i, '[absolute]')
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


def execute(request):
    """Generates field usage report as a CSV file.
    :param request: json as a dict.
    """
    chunk_size = task_utils.CHUNK_SIZE
    file_name = task_utils.get_parameter_value(request['params'], 'file_name', 'value')
    fields = task_utils.get_parameter_value(request['params'], 'fields', 'value')

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

    query = '{0}/select?&wt=json&fl={1}'.format(sys.argv[2].split('=')[1], ','.join(fields))
    # query = '{0}/select?&wt=json&fl={1}'.format("http://localhost:8888/solr/v0", ','.join(fields))
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
            res = requests.get(query.replace('{0}', str(chunk_size)).replace('{1}', str(i)), headers=headers)
            jobs = res.json()['response']['docs']
            export_to_csv(jobs, 'temp_report', task_folder, fields)
            exported_cnt += chunk_size
            if exported_cnt > num_results:
                status_writer.send_status('Finalizing field usage report...')
                generate_report(os.path.join(task_folder, 'temp_report.csv'),
                                os.path.join(task_folder, '{0}.csv'.format(file_name)), fields)
                status_writer.send_percent(100, 'reported: 100%', 'generate_report')
            else:
                percent_done = exported_cnt / num_results
                status_writer.send_percent(percent_done, '{0}: {1:.0f}%'.format("reported", percent_done * 100), 'generate_report')
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
            results = requests.get(query + '&ids={0}'.format(','.join(group)), headers=headers)
            jobs = eval(results.text)['response']['docs']
            export_to_csv(jobs, 'temp_report', task_folder, fields)
            percent_done = float(i) / num_results
            status_writer.send_percent(percent_done, '{0}: {1:.0f}%'.format("reported", percent_done * 100), 'generate_report')
        status_writer.send_status('Finalizing field usage report...')
        generate_report(os.path.join(task_folder, 'temp_report.csv'), os.path.join(task_folder, '{0}.csv'.format(file_name)), fields)

    # Zip up outputs.
    if exported_count == 0:
        status_writer.send_state(status.STAT_FAILED)
        task_utils.report(os.path.join(request['folder'], '__report.json'), exported_count, 0, errors_count, errors_reasons)
    else:
        task_utils.report(os.path.join(request['folder'], '__report.json'), exported_count, 0, errors_count, errors_reasons)
        shutil.move(os.path.join(task_folder, '{0}.csv'.format(file_name)),
                    os.path.join(os.path.dirname(task_folder), '{0}.csv'.format(file_name)))
