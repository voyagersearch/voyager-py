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
from __future__ import division
import os
import sys
import logging
import multiprocessing
import arcpy
import status


def global_job(*args):
    """Create a global job object."""
    global job
    job = args[0]


def update_row(fields, rows, row):
    """Updates the coded values in a row with the descriptions."""
    field_domains = {f.name: f.domain for f in fields if f.domain}
    fields_values = zip(rows.fields, row)
    for j, x in enumerate(fields_values):
        if x[0] in field_domains:
            domain_name = field_domains[x[0]]
            row[j] = job.domains[domain_name][x[1]]
    return row


def worker(data_path):
    """The worker function to index feature data and tabular data."""
    if data_path:
        job.connect_to_zmq()
        geo = {}
        entry = {}
        dsc = arcpy.Describe(data_path)

        if dsc.dataType == 'Table':
            fields = job.search_fields(data_path)
            query = job.get_table_query(dsc.name)
            constraint = job.get_table_constraint(dsc.name)
            if query and constraint:
                expression = """{0} AND {1}""".format(query, constraint)
            else:
                if query:
                    expression = query
                else:
                    expression = constraint
            with arcpy.da.SearchCursor(data_path, fields, expression) as rows:
                for i, row in enumerate(rows, 1):
                    if job.domains:
                        row = list(row)
                        field_domains = {f.name: f.domain for f in dsc.fields if f.domain}
                        fields_values = zip(rows.fields, row)
                        for j, x in enumerate(fields_values):
                            if x[0] in field_domains:
                                domain_name = field_domains[x[0]]
                                row[j] = job.domains[domain_name][x[1]]

                    mapped_fields = job.map_fields(dsc.name, fields)
                    mapped_fields = dict(zip(mapped_fields, row))
                    mapped_fields['_discoveryID'] = job.discovery_id
                    oid_field = filter(lambda x: x in ('FID', 'OID', 'OBJECTID'), rows.fields)
                    if oid_field:
                        fld_index = rows.fields.index(oid_field[0])
                    else:
                        fld_index = i
                    entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.basename(data_path), row[fld_index])
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    entry['entry'] = {'fields': mapped_fields}
                    job.send_entry(entry)
        else:
            sr = arcpy.SpatialReference(4326)
            geo['spatialReference'] = dsc.spatialReference.name
            geo['code'] = dsc.spatialReference.factoryCode
            fields = job.search_fields(dsc.catalogPath)
            query = job.get_table_query(dsc.name)
            constraint = job.get_table_constraint(dsc.name)
            if query and constraint:
                expression = """{0} AND {1}""".format(query, constraint)
            else:
                if query:
                    expression = query
                else:
                    expression = constraint
            if dsc.shapeFieldName in fields:
                fields.remove(dsc.shapeFieldName)
            if dsc.shapeType == 'Point':
                with arcpy.da.SearchCursor(dsc.catalogPath, ['SHAPE@XY'] + fields, expression, sr) as rows:
                    for i, row in enumerate(rows):
                        if job.domains:
                            row = list(row)
                            field_domains = {f.name: f.domain for f in dsc.fields if f.domain}
                            field_vals = zip(rows.fields, row)
                            for j, x in enumerate(field_vals):
                                if x[0] in field_domains:
                                    domain_name = field_domains[x[0]]
                                    row[j] = job.domains[domain_name][x[1]]

                        geo['lon'] = row[0][0]
                        geo['lat'] = row[0][1]
                        mapped_fields = job.map_fields(dsc.name, list(rows.fields[1:]))
                        mapped_fields = dict(zip(mapped_fields, row[1:]))
                        mapped_fields['_discoveryID'] = job.discovery_id
                        entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.basename(data_path), i)
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)
            else:
                with arcpy.da.SearchCursor(dsc.catalogPath, ['SHAPE@'] + fields, expression, sr) as rows:
                    for i, row in enumerate(rows):
                        if job.domains:
                            row = list(row)
                            field_domains = {f.name: f.domain for f in dsc.fields if f.domain}
                            field_vals = zip(rows.fields, row)
                            for j, x in enumerate(field_vals):
                                if x[0] in field_domains:
                                    domain_name = field_domains[x[0]]
                                    row[j] = job.domains[domain_name][x[1]]

                        geo['xmin'] = row[0].extent.XMin
                        geo['xmax'] = row[0].extent.XMax
                        geo['ymin'] = row[0].extent.YMin
                        geo['ymax'] = row[0].extent.YMax
                        mapped_fields = job.map_fields(dsc.name, list(rows.fields[1:]))
                        mapped_fields = dict(zip(mapped_fields, row[1:]))
                        mapped_fields['_discoveryID'] = job.discovery_id
                        entry['id'] = '{0}_{1}_{2}'.format(job.location_id, os.path.basename(data_path), i)
                        entry['location'] = job.location_id
                        entry['action'] = job.action_type
                        entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                        job.send_entry(entry)


def assign_work(esri_job):
    """Determines the data type and each dataset is sent to the worker to be processed."""
    status_writer = status.Writer()
    job = esri_job
    dsc = arcpy.Describe(job.path)

    # A single feature class or table.
    if dsc.dataType in ('DbaseTable', 'FeatureClass', 'Shapefile', 'Table'):
        global_job(job, int(arcpy.GetCount_management(job.path).getOutput(0)))
        worker(job.path)
        return

    # A geodatabase (.mdb, .gdb, or .sde).
    elif dsc.dataType == 'Workspace':
        arcpy.env.workspace = job.path
        feature_datasets = arcpy.ListDatasets('*', 'Feature')
        tables = []
        if job.tables_to_keep:
            for t in job.tables_to_keep:
                [tables.append(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables(t)]
                [tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                for fds in feature_datasets:
                    [tables.append(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]
        else:
            [tables.append(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables()]
            [tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses()]
            for fds in feature_datasets:
                [tables.append(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(feature_dataset=fds)]

        if job.tables_to_skip:
            for t in job.tables_to_keep:
                [tables.remove(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables(t)]
                [tables.remove(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                for fds in feature_datasets:
                    [tables.remove(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]

    # A geodatabase feature dataset, SDC data, or CAD dataset.
    elif dsc.dataType == 'FeatureDataset' or dsc.dataType == 'CadDrawingDataset':
        arcpy.env.workspace = job.path
        if job.tables_to_keep:
            tables = []
            for tbl in job.tables_to_keep:
                [tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(tbl)]
                tables = list(set(tables))
        else:
            tables = [os.path.join(job.path, fc) for fc in arcpy.ListFeatureClasses()]
        if job.tables_to_skip:
            for tbl in job.tables_to_skip:
                [tables.remove(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(tbl) if fc in tables]

    # Not a recognized data type.
    else:
        sys.exit(1)

    if job.multiprocess.lower() == 'true':
        # Multiprocess larger databases and feature datasets.
        multiprocessing.log_to_stderr()
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.INFO)
        pool = multiprocessing.Pool(initializer=global_job, initargs=(job,))
        for i, _ in enumerate(pool.imap_unordered(worker, tables), 1):
            status_writer.send_percent(i/len(tables), "{0:%}".format(i/len(tables)), 'esri_worker')
        # Synchronize the main process with the job processes to ensure proper cleanup.
        pool.close()
        pool.join()
    else:
        for tbl in tables:
            global_job(job)
            worker(tbl)
    return
