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
import string
import arcpy
from utils import status
from utils import task_utils


status_writer = status.Writer()
skipped_reasons = {}
errors_reasons = {}

text_characters = "".join(map(chr, range(32, 127)) + list("\n\r\t\b"))
_null_trans = string.maketrans("", "")
#location_id = '386c466359f3d565'


def change(val, encoding_type):
    if isinstance(val, (str, unicode)):
        return val.encode(encoding_type)
    else:
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
    t = s.translate(_null_trans, text_characters)
    # If more than 30% non-text characters, then
    # this is considered a binary file
    if len(t) / len(s) > 0.30:
        return 0
    return 1


def create_listcount_data_in_doc(voyager_instance, headers):
    solr_query = "%s/select?&q=linkcount_data_in_doc:*&fl=absolute_path,links" % (voyager_instance)

    # Make small request to get the number of voyager items without rowCount.
    res = requests.get(solr_query + "&rows=1&start=150&wt=json", headers=headers)
    result_count = res.json()['response']['numFound']

    parent_links = collections.defaultdict(list)
    groups = grouper(range(0, result_count), 150, '')
    for group in groups:
        results = requests.get(solr_query + "&sort=id asc&rows={0}&start={1}&wt=json".format(150, group[0]), headers=headers)
        docs = results.json()['response']['docs']
        if docs:
            for doc in docs:
                links = eval(doc['links'])['links']
                for link in links:
                    for l in link['link']:
                        parent_links[doc['absolute_path']].append(l['id'])

    return parent_links


def raster_compare(rasterA, rasterB):
    equal = 0
    raster_propertiesA = {}
    raster_propertiesB = {}

    rA = arcpy.Describe(rasterA)
    rB = arcpy.Describe(rasterB)

    raster_propertiesA['bandCount'] = rA.bandCount
    raster_propertiesA['format'] = rA.format
    raster_propertiesB['bandCount'] = rB.bandCount
    raster_propertiesB['format'] = rB.format

    if raster_propertiesA == raster_propertiesB:
        for i in range(rA.bandCount):
            if rA.bandCount == 1:
                rbA = arcpy.Describe(rA.catalogPath)
                rbB = arcpy.Describe(rB.catalogPath)
            else:
                rbA = arcpy.Describe(os.path.join(rA.catalogPath, "band_{0}".format(i + 1)))
                rbB = arcpy.Describe(os.path.join(rB.catalogPath, "band_{0}".format(i + 1)))
            raster_propertiesA['height'] = rbA.height
            raster_propertiesB['height'] = rbB.height
            raster_propertiesA['width'] = rbA.width
            raster_propertiesB['width'] = rbB.width
            raster_propertiesA['meanCellHeight'] = rbA.meanCellHeight
            raster_propertiesB['meanCellHeight'] = rbB.meanCellHeight
            raster_propertiesA['meanCellHeight'] = rbA.meanCellWidth
            raster_propertiesB['meanCellHeight'] = rbB.meanCellWidth
            raster_propertiesA['pixelType'] = rbA.pixelType
            raster_propertiesB['pixelType'] = rbB.pixelType
            if raster_propertiesA == raster_propertiesB:
                continue
            else:
                equal = 1
    else:
        equal = 1
    return equal


def compare_geodatabases(gdbs):
    fdsA, fdsB = [], []
    tblA, tblB = [], []
    severity = 0
    arcpy.env.workspace = gdbs[0]
    tblA = arcpy.ListTables()
    fdsA = arcpy.ListDatasets()
    fcsA = arcpy.ListFeatureClasses()
    if not fcsA:
        fcsA = []
    if fdsA:
        for fd in fdsA:
            arcpy.env.workspace = os.path.join(gdbs[0], fd)
            fcs = arcpy.ListFeatureClasses()
            if fcs:
                for fc in fcs:
                    fcsA.append(fc)

    arcpy.env.workspace = gdbs[1]
    tblB = arcpy.ListTables()
    fdsB = arcpy.ListDatasets()
    fcsB = arcpy.ListFeatureClasses()
    if not fcsB:
        fcsB = []
    if fdsB:
        for fd in fdsB:
            arcpy.env.workspace = os.path.join(gdbs[1], fd)
            fcs = arcpy.ListFeatureClasses()
            if fcs:
                for fc in fcs:
                    fcsB.append(fc)

    if not sorted(tblA) == sorted(tblB):
        severity = 1
    elif not sorted(fdsA) == sorted(fdsB):
        severity = 1
    elif not sorted(fcsA) == sorted(fcsB):
        severity = 1

    arcpy.env.workspace = ''
    return severity


def compare_data(data):
    """TODO: add description"""
    max_severity = 0
    data_paths = [d['FILE PATH'] for d in data if os.path.exists(d['FILE PATH'])]
    if len(data_paths) == 1:
        return 1
    elif len(data_paths) == 0:
        return 1

    common_file_exts = ['.xyz', '.grd', '.txt', '.tab', '.ecw', '.lyr']
    common_raster_exts = ['.tif', '.jpg', '.img', '.png']
    common_gis_exts = ['.shp']
    common_cad_exts = ['.dxf']
    common_fds_exts = ['.sdc']
    common_workspace_exts = ['.gdb', '.mdb']

    ext = os.path.splitext(data_paths[0])[1].lower()
    if ext in common_file_exts:
        data_type = 'file'
    elif ext in common_raster_exts:
        data_type = 'rasterdataset'
    elif ext in common_gis_exts:
        data_type = 'shapefile'
    elif ext in common_cad_exts:
        data_type = 'caddrawingdataset'
    elif ext in common_workspace_exts:
        data_type = 'workspace'
    elif ext in common_fds_exts:
        data_type = 'featuredataset'
    elif ext in ['.mxd']:
        data_type = 'mapdocument'
    else:
        # Determine type of data
        data_type = arcpy.Describe(data_paths[0]).dataType

    # Create combinations of items
    combs = {}
    combs[data_paths[0]] = data_paths[1:]

    for item in combs.values()[0]:
        try:
            if data_type.lower() == 'shapefile':
                sort_field = get_sort_field([item, data_paths[0]])
                if sort_field:
                    result = arcpy.FeatureCompare_management(item, data_paths[0], sort_field, continue_compare=False)
                    max_severity = result.maxSeverity
                    if max_severity > 0:
                        break
            elif data_type.lower() == 'rasterdataset':
                # result = arcpy.RasterCompare_management(item, data_paths[0], continue_compare=False)
                # max_severity = result.maxSeverity
                max_serverity = raster_compare(item, data_paths[0])
                if max_severity > 0:
                    break
            elif data_type.lower() in ('arcinfotable', 'dbasetable'):
                sort_field = get_sort_field([item, data_paths[0]])
                if sort_field:
                    result = arcpy.TableCompare_management(item, data_paths[0], sort_field, continue_compare=False)
                    max_severity = result.maxSeverity
                    if max_severity > 0:
                        break
            elif data_type.lower() in ('file', 'textfile', 'prjfile', 'toolbox', 'layer'):
                if is_ascii(item):
                    result = arcpy.FileCompare_management(item, data_paths[0], "ASCII", False)
                    max_severity = result.maxSeverity
                    if max_severity > 0:
                        break
                else:
                    result = arcpy.FileCompare_management(item, data_paths[0], "BINARY", False)
                    max_severity = result.maxSeverity
                    if max_severity > 0:
                        break

            elif data_type.lower() == 'mapdocument':
                mxdA = arcpy.mapping.MapDocument(data_paths[0])
                data_framesA = arcpy.mapping.ListDataFrames(mxdA)
                mxdB = arcpy.mapping.MapDocument(item)
                data_framesB = arcpy.mapping.ListDataFrames(mxdB)

                # Compare dataframes
                if not sorted([dfA.name for dfA in data_framesA]) == sorted([dfB for dfB in data_framesB]):
                    max_severity = 1
                    break
                else:
                    # Compare layers in each dataframe
                    layersA = []
                    layersB = []
                    for dframe in data_framesA:
                        lyrs = arcpy.mapping.ListLayers(mxdA, data_frame=dframe)
                        if lyrs:
                            for lA in lyrs:
                                layersA.append(lA.name)
                    for dframe in data_framesB:
                        lyrs = arcpy.mapping.ListLayers(mxdB, data_frame=dframe)
                        if lyrs:
                            for lB in lyrs:
                                layersB.append(lB.name)
                    if not sorted(layersA) == sorted(layersB):
                        max_severity = 1
                        break

            elif data_type.lower() == 'workspace':
                max_severity = compare_geodatabases([data_paths[0], item])
                if max_severity > 0:
                    break

            elif data_type.lower() in ('featuredataset', 'caddrawingdataset'):
                arcpy.env.workspace = data_paths[0]
                fcsA = sorted([fc.lower() for fc in arcpy.ListFeatureClasses()])
                arcpy.env.workspace = item
                fcsB = sorted([fc.lower() for fc in arcpy.ListFeatureClasses()])
                if not len(fcsA) == len(fcsB):
                    max_severity = 1
                    break
                else:
                    for i, fc in enumerate(fcsA):
                        sort_field = get_sort_field([os.path.join(data_paths[0], fc), os.path.join(item, fcsB[i])])
                        if sort_field:
                            result = arcpy.FeatureCompare_management(os.path.join(data_paths[0], fc), os.path.join(item, fcsB[i]), sort_field, continue_compare=False)
                            max_severity = result.maxSeverity
                            if max_severity > 0:
                                break
        except arcpy.ExecuteError:
            status_writer.send_status(arcpy.GetMessages())
            max_severity = 0
            break
    return max_severity


def get_sort_field(items):
    itemA = items[0]
    itemB = items[1]
    sort_field = None
    common_fields = None
    c1 = set([fld.name for fld in arcpy.ListFields(itemA, field_type='String')])
    c2 =  set([fld.name for fld in arcpy.ListFields(itemB, field_type='String')])
    common_fields = list(c1.intersection(c2))
    if common_fields:
        sort_field = common_fields[0]
    else:
        c1 = set([fld.name for fld in arcpy.ListFields(itemA, field_type='Integer')])
        c2 = set([fld.name for fld in arcpy.ListFields(itemB, field_type='Integer')])
        common_fields = c1.intersection(c2)
        if common_fields:
            sort_field = common_fields.pop()
    if not sort_field:
        desc = arcpy.Describe(itemA)
        if hasattr(desc, 'OIDFieldName'):
            sort_field = desc.OIDFieldName
        else:
            sort_field = None
    return sort_field


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
    required_fields = ("name", "title", "bytes", "id", "format", "absolute_path", "format_category")
    [fields.remove(f) for f in required_fields if f in fields]

    fields_str = ""
    if fields:
        fields_str = ",".join(fields)

    voyager_instance = 'http://localhost:8888/solr/v0' #sys.argv[2].split('=')[1]
    #voyager_instance = 'http://ec2amaz-7h0t5qu.tnc.sdi.org:8888/solr/v0'
    query =  "%s/select?q={!func}joindf(md5,md5)&f.md5.facet.mincount=2&f.contentHash.facet.mincount=2&f.schemaHash.facet.mincount=2&sort=md5 desc&start=0&rows=1&fl=id,title,name:[name],format,fullpath:[absolute],absolute_path:[absolute],format_type,bytes,md5,path,name&fq={!frange l=2}{!func}joindf(md5,md5)&wt=json" % (voyager_instance)
    results = requests.get(query, auth=('admin', 'admin'))
    result_count = results.json()['response']['numFound']
    if result_count == 0:
        status_writer.send_state(status.STAT_WARNING, "No duplicates found.")
        return
    duplicates = collections.defaultdict(list)
    groups = grouper(range(0, result_count), 150, '')

    url = voyager_instance.split("/solr")[0]
    req = requests.get("{0}/api/rest/i18n/field/format".format(url), headers=headers)
    formats = req.json()['VALUE']['format']

    processed_count = 0

    status_writer.send_status("Generating list of documents with children...")
    parent_docs = create_listcount_data_in_doc(voyager_instance, headers)

    increment = task_utils.get_increment(result_count)
    for group in groups:
        query = "%s/select?q={!func}joindf(md5,md5)&f.md5.facet.mincount=2&f.contentHash.facet.mincount=2&f.schemaHash.facet.mincount=2&sort=md5 desc&start=%s&rows=150&fl=md5,id,title,name:[name],format,fullpath:[absolute],absolute_path:[absolute],format_type,format_category,bytes,linkcount_data,linkcount_md5,path,name,%s&fq={!frange l=2}{!func}joindf(md5,md5)&wt=json" % (voyager_instance, group[0], fields_str)
        results = requests.get(query, headers=headers)
        docs = results.json()['response']['docs']
        for doc in docs:
            file_path = ""
            if "absolute_path" in doc:
                file_path = doc["absolute_path"]
                if os.path.splitext(file_path)[1].lower() in ('.cpg', '.ini'):
                    continue

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

            format_category = ""
            if "format_category" in doc:
                format_category = doc["format_category"]

            id = ""
            if "id" in doc:
                id = doc["id"]

            name = ""
            if "name" in doc:
                name = doc["name"]
            elif "title" in doc:
                name = doc["title"]

            field_dict = {"FILE NAME": name, "FILE SIZE": file_size, "FORMAT CATEGORY": format_category,
                          "FORMAT": format_type, "ID": id, "FILE PATH": file_path}

            extra_fields = {}
            if fields:
                for fld in fields:
                    if fld in doc:
                        extra_fields[fld.upper()] = doc[fld]

            field_dict.update(extra_fields)
            duplicates[doc['md5']].append(field_dict)
        processed_count += len(group)
        if (processed_count % increment) == 0:
            status_writer.send_percent(processed_count / float(result_count), 'Grouping duplicates by MD5...', 'report_duplicate_files')

    # Find total number of items in the index.
    all_query = "%s/select?&disp=default&sort=score desc&place.op=within&start=0&fl=id&voyager.config.id=ace4bb77&wt=json" % (voyager_instance)
    results = requests.get(all_query, headers=headers)
    index_count = results.json()['response']['numFound']

    duplicate_count = 0
    total_file_size = 0

    # Write all the duplicates to the report file.
    status_writer.send_percent(0, 'Creating the duplicate report and comparing data...', '')
    processed_count = 0
    md5_count = len(duplicates)
    increment = task_utils.get_increment(md5_count)
    with open(report_file, "wb") as f:
        if extra_fields:
            keys = ["MD5", "FILE NAME", "FILE SIZE", "FORMAT", "FORMAT CATEGORY", "ID", "FILE PATH", "MXD COUNT", "LYR COUNT"] + extra_fields.keys()
        else:
            keys = ["MD5", "FILE NAME", "FILE SIZE", "FORMAT", "FORMAT CATEGORY", "ID", "FILE PATH", "MXD COUNT", "LYR COUNT"]
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()

        # Loop through each group of MD5 (duplicates).
        for md5, values in duplicates.items():
            try:
                val_count = len(values)
                if val_count > 1:
                    # If the files sizes are all 0 bytes, return them as duplicates.
                    file_size = convert_size(values[0]['FILE SIZE'])
                    for z in values:
                        if not convert_size(z['FILE SIZE']) == file_size:
                            values.remove(z)
                    if len(values) == 1:
                        processed_count += 1
                        continue

                    # Perform data comparison (Feature, Table, File, raster, etc.). If different, don't report and continue.
                    max_severity = compare_data(values)
                    if max_severity > 0:
                        continue

                    writer.writerow({})
                    duplicate_count += val_count - 1
                    for val in values:
                        used_in_mxd = 0
                        used_in_lyr = 0
                        total_file_size += val["FILE SIZE"] * (val_count - 1)
                        val["MD5"] = md5
                        val["FILE SIZE"] = convert_size(val["FILE SIZE"])
                        new_val = {k: change(v, 'utf-8') for (k, v) in val.items()}
                        if new_val["FORMAT CATEGORY"].lower() in ("gis", "cad", "imagery"):
                            for k,v in parent_docs.items():
                                if new_val['ID'] in v:
                                    if k.endswith('.mxd'):
                                        used_in_mxd += 1
                                    elif k.endswith('.lyr'):
                                        used_in_lyr += 1
                        new_val['MXD COUNT'] = used_in_mxd
                        val['MXD COUNT'] = used_in_mxd
                        new_val['LYR COUNT'] = used_in_lyr
                        val['LYR COUNT'] = used_in_lyr
                        try:
                            writer.writerow(new_val)
                        except UnicodeEncodeError:
                            try:
                                new_val = {k: change(v, 'latin-1') for (k, v) in new_val.items()}
                                writer.writerow(new_val)
                            except Exception as we:
                                status_writer.send_status('WRITE ERROR: {0}'.format(repr(we)))
                                pass
                processed_count += 1
            except Exception as ex:
                status_writer.send_status(repr(ex))
                processed_count += 1
                continue
            try:
                if (processed_count % increment) == 0:
                    status_writer.send_percent(processed_count / float(md5_count), 'Creating report and comparing data...', 'report_duplicate_files')
            except Exception:
                status_writer.send_status("error reporting progress.")
                continue
    try:
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
    except Exception:
        status_writer.send_status("Error writing summary.")