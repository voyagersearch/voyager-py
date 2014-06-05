from __future__ import division
import os
import sys
import logging
import multiprocessing
import arcpy
import base_job


def worker(data_path):
    #job = base_job.Job(job_file)
    job.connect_to_zmq()
    geo = {}
    entry = {}
    dsc = arcpy.Describe(data_path)

    if dsc.dataType == 'Table':
        fields = job.search_fields(data_path)
        with arcpy.da.SearchCursor(data_path, fields) as rows:
            for i, row in enumerate(rows, 1):
                gfid_index = rows.fields.index('GFID')
                mapped_fields = job.map_fields(dsc.name, fields)
                mapped_fields = dict(zip(mapped_fields, row))
                #oid_field = filter(lambda x: x in ('FID', 'OID', 'OBJECTID'), rows.fields)
                #if oid_field:
                #   fld_index = rows.fields.index(oid_field[0])
                #else:
                #    fld_index = i
                #entry['id'] = '{0}_{1}_{2}'.format(location_id, os.path.basename(data_path), row[fld_index])
                # FOR KYLE...
                entry['id'] = row[gfid_index]
                entry['location'] = job.location_id
                entry['action'] = job.action_type
                entry['entry'] = {'fields': mapped_fields}
                job.send_entry(entry)
    else:
        sr = arcpy.SpatialReference(4326)
        geo['spatialReference'] = dsc.spatialReference.name
        geo['code'] = dsc.spatialReference.factoryCode
        fields = job.search_fields(dsc.catalogPath)
        if dsc.shapeFieldName in fields:
            fields.remove(dsc.shapeFieldName)
        if dsc.shapeType == 'Point':
            with arcpy.da.SearchCursor(dsc.catalogPath, ['SHAPE@XY'] + fields, '', sr) as rows:
                for i, row in enumerate(rows):
                    geo['lon'] = row[0][0]
                    geo['lat'] = row[0][1]
                    gfid_index = rows.fields.index('OBJECTID')
                    mapped_fields = job.map_fields(dsc.name, list(rows.fields[1:]))
                    mapped_fields = dict(zip(mapped_fields, row[1:]))
                    #entry['id'] = '{0}_{1}_{2}'.format(location_id, os.path.basename(data_path), i)
                    # FOR KYLE...
                    entry['id'] = row[gfid_index]
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                    job.send_entry(entry)
        else:
            with arcpy.da.SearchCursor(dsc.catalogPath, ['SHAPE@'] + fields, '', sr) as rows:
                for i, row in enumerate(rows):
                    geo['xmin'] = row[0].extent.XMin
                    geo['xmax'] = row[0].extent.XMax
                    geo['ymin'] = row[0].extent.YMin
                    geo['ymax'] = row[0].extent.YMax
                    gfid_index = rows.fields.index('OBJECTID')
                    mapped_fields = job.map_fields(dsc.name, list(rows.fields[1:]))
                    mapped_fields = dict(zip(mapped_fields, row[1:]))
                    #entry['id'] = '{0}_{1}_{2}'.format(location_id, os.path.basename(data_path), i)
                    # FOR KYLE...
                    entry['id'] = row[gfid_index]
                    entry['location'] = job.location_id
                    entry['action'] = job.action_type
                    entry['entry'] = {'geo': geo, 'fields': mapped_fields}
                    job.send_entry(entry)


def assign_work(job_info):
    """Determines the data type and each dataset is sent to the worker to be processed."""
    #job_file = job.job_file
    job = base_job.Job(job_info)
    dsc = arcpy.Describe(job.path)
    if dsc.dataType in ('DbaseTable', 'FeatureClass', 'Shapefile', 'Table'):
        worker(job.path)
    elif dsc.dataType == 'Workspace':
        arcpy.env.workspace = job.path
        feature_datasets = arcpy.ListDatasets('*', 'Feature')
        all_tables = []
        if job.tables_to_keep:
            for t in job.tables_to_keep:
                [all_tables.append(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables(t)]
                [all_tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                for fds in feature_datasets:
                    [all_tables.append(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]
        else:
            [all_tables.append(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables()]
            [all_tables.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses()]
            for fds in feature_datasets:
                [all_tables.append(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(feature_dataset=fds)]

        if job.tables_to_skip:
            for t in job.tables_to_keep:
                [all_tables.remove(os.path.join(job.path, tbl)) for tbl in arcpy.ListTables(t)]
                [all_tables.remove(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(t)]
                for fds in feature_datasets:
                    [all_tables.remove(os.path.join(job.path, fds, fc)) for fc in arcpy.ListFeatureClasses(wild_card=t, feature_dataset=fds)]

        multiprocessing.log_to_stderr()
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.INFO)
        pool = multiprocessing.Pool(initializer=global_job, initargs = (job, ))
        for i, _ in enumerate(pool.imap_unordered(worker, all_tables, 1)):
            sys.stderr.write('\rdone {0:%}'.format(i/len(all_tables)))
        # Synchronize the main process with the job processes to ensure proper cleanup.
        pool.close()
        pool.join()

    elif dsc.dataType == 'FeatureDataset' or dsc.dataType == 'CadDrawingDataset':
        arcpy.env.workspace = job.path
        if job.tables_to_keep:
            feature_classes = []
            for tbl in job.tables_to_keep:
                [feature_classes.append(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(tbl)]
        else:
            feature_classes = [os.path.join(job.path, fc) for fc in arcpy.ListFeatureClasses()]
        if job.tables_to_skip:
            for tbl in job.tables_to_skip:
                [feature_classes.remove(os.path.join(job.path, fc)) for fc in arcpy.ListFeatureClasses(tbl)]
        for fc in feature_classes:
            worker(os.path.join(job.path, fc))

def global_job(args):
    global job
    job = args