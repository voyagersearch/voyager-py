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
import math
import collections
import itertools
import requests
from utils import status
from utils import task_utils


status_writer = status.Writer()
skipped_reasons = {}
errors_reasons = {}


def convert_size(size_bytes):
    """Convert bytes.
    :param size_bytes:
    :return: string
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "{0} {1}".format(s, size_name[i])


def grouper(iterable, n, fill_value=None):
    """Collect data into fixed-length chunks or blocks.
    :param iterable: input iterable (list, etc.)
    :param n: number of chunks/blocks
    :param fillvalue: value for remainder values
    :return: izip_longest object

    e.g. grouper([1,2,3,4], 2, 'end') --> (1,2) (2,3) 'end'
    """
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fill_value, *args)


def execute(request):
    """Report duplicate items.
    :param request: json as a dict.
    """
    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])
    date_str = str(datetime.date.today())
    report_file = os.path.join(request['folder'], 'Duplicates_{0}.csv'.format(date_str))

    request_owner = request['owner']
    headers = {'x-access-token': task_utils.get_security_token(request_owner)}

    fields = list(task_utils.get_parameter_value(request['params'], 'fields', 'value'))
    required_fields = ("name", "title", "bytes", "id", "format", "absolute_path")
    [fields.remove(f) for f in required_fields if f in fields]

    fields_str = ""
    if fields:
        fields_str = ",".join(fields)

    voyager_instance = sys.argv[2].split('=')[1]
    query = voyager_instance + "/select?q={!func}joindf(md5,md5)&f.md5.facet.mincount=2&f.contentHash.facet.mincount=2&f.schemaHash.facet.mincount=2&sort=md5 desc&start=0&rows=1&fl=id,title,name:[name],format,fullpath:[absolute],absolute_path:[absolute],download:[downloadURL],format_type,bytes,layerURL:[lyrURL],md5,path,name&fq={!frange l=2}{!func}joindf(md5,md5)&wt=json"
    results = requests.get(query, auth=("admin", "admin"), headers=headers)
    result_count = results.json()['response']['numFound']
    duplicates = collections.defaultdict(list)
    groups = grouper(range(0, result_count), 25, '')

    url = voyager_instance.split("/solr")[0]
    req = requests.get("{0}/api/rest/i18n/field/format".format(url), headers=headers)
    formats = req.json()['VALUE']['format']

    for group in groups:
        query = "%s/select?q={!func}joindf(md5,md5)&f.md5.facet.mincount=2&f.contentHash.facet.mincount=2&f.schemaHash.facet.mincount=2&sort=md5 desc&start=%s&rows=25&fl=id,title,name:[name],format,fullpath:[absolute],absolute_path:[absolute],download:[downloadURL],format_type,bytes,layerURL:[lyrURL],md5,path,name,%s&fq={!frange l=2}{!func}joindf(md5,md5)&wt=json" % (voyager_instance, group[0], fields_str)
        results = requests.get(query, auth=("admin", "admin"), headers=headers)
        docs = results.json()['response']['docs']

        for doc in docs:
            file_size = 0
            if "bytes" in doc:
                file_size = float(doc['bytes'])

            format_type = ""
            if "format" in doc and not doc["format"] in ("application/vnd.esri.gdb.file.data", "application/vnd.esri.gdb.personal.data"):
                try:
                    format_type = formats[doc["format"]]
                except KeyError:
                    format_type = doc["format"]
            else:
                continue

            file_path = ""
            if "absolute_path" in doc:
                file_path = doc["absolute_path"]

            id = ""
            if "id" in doc:
                id = doc["id"]

            name = ""
            if "name" in doc:
                name = doc["name"]
            elif "title" in doc:
                name = doc["title"]

            field_dict = {"FILE NAME": name, "FILE SIZE": file_size,
                          "FORMAT": format_type, "ID": id, "FILE PATH": file_path}

            extra_fields = {}
            if fields:
                for fld in fields:
                    if fld in doc:
                        extra_fields[fld.upper()] = doc[fld]

            field_dict.update(extra_fields)
            duplicates[doc['md5']].append(field_dict)

    # Find total number of items in the index.
    all_query = "%s/select?disp=default&sort=score desc&place.op=within&start=0&fl=id&voyager.config.id=ace4bb77&wt=json" % (voyager_instance)
    results = requests.get(all_query, auth=("admin", "admin"), headers=headers)
    index_count = results.json()['response']['numFound']

    duplicate_count = 0
    total_file_size = 0
    # Write all the duplicates to the report file.
    with open(report_file, "wb") as f:
        if extra_fields:
            keys = ["MD5", "FILE NAME", "FILE SIZE", "FORMAT", "ID", "FILE PATH"] + extra_fields.keys()
        else:
            keys = ["MD5", "FILE NAME", "FILE SIZE", "FORMAT", "ID", "FILE PATH"]
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for md5, values in duplicates.items():
            writer.writerow({})
            val_count = len(values)
            if val_count > 1:
                duplicate_count += val_count - 1
                for val in values:
                    total_file_size += val["FILE SIZE"] * (val_count - 1)
                    val["MD5"] = md5
                    val["FILE SIZE"] = convert_size(val["FILE SIZE"])
                    writer.writerow(val)

    # Report a summary to the report file.
    pct_dups = float(duplicate_count) / index_count
    with open(report_file, "ab") as f:
        writer = csv.DictWriter(f, fieldnames=["DUPLICATE COUNT", "INDEX COUNT", "PERCENT DUPLICATES", "TOTAL DUPLICATE FILE SIZE"])
        writer.writerow({})
        writer.writerow({})
        writer.writeheader()
        writer.writerow({"DUPLICATE COUNT": duplicate_count,
                         "INDEX COUNT": index_count,
                         "PERCENT DUPLICATES": '{:.0f}%'.format(pct_dups * 100),
                         "TOTAL DUPLICATE FILE SIZE": convert_size(total_file_size)})

    status_writer.send_status("DUPLICATE COUNT: {0}".format(duplicate_count))
    status_writer.send_status("INDEX COUNT: {0}".format(index_count))
    status_writer.send_status("PERCENT DUPLICATES: {0}".format('{:.0f}%'.format(pct_dups * 100)))
    status_writer.send_status("TOTAL DUPLICATE FILE SIZE: {0}".format(convert_size(total_file_size)))
    status_writer.send_state(status.STAT_SUCCESS)