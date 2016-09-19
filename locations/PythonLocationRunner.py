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
# WITHOUT WARRANTIES  CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Submits a indexing job for a data location."""
import sys
import json
import collections
import workers

modules = {'arcpy': 'esri_worker', 'cx_Oracle': 'oracle_worker',
           'pyodbc': 'sql_worker', 'pyodbc': 'mysql_worker',
           'boto3': 'dynamodb_worker', 'pymongo': 'mongodb_worker',
           'ogr': 'gdal_worker'}


if __name__ == '__main__':
    if sys.argv[1] == '--info':
        worker_info = collections.defaultdict(list)
        try:
            __import__('zmq')
        except ImportError as ie:
            sys.stdout.write('{0}. Please contact Voyager Search support.'.format(ie.message))
            sys.exit(1)

        for module, worker in modules.items():
            try:
                __import__(module)
                worker_info['workers'].append({'name': worker, 'available': True})
            except ImportError as ie:
                worker_info['workers'].append({'name': worker, 'available': False, 'warning': str(ie)})
                pass
        sys.stdout.write(json.dumps(worker_info, indent=2))
        sys.stdout.flush()
    else:
        from workers import base_job
        job = base_job.Job(sys.argv[1])
        if job.path or job.service_connection:
            from workers import esri_worker
            esri_worker.run_job(job)
        elif job.url:
            from workers import gdal_worker
            gdal_worker.run_job(job.job_file)
        elif job.dynamodb_region:
            from workers import dynamodb_worker
            dynamodb_worker.run_job(job)
        elif job.mongodb_client_info:
            from workers import mongodb_worker
            mongodb_worker.run_job(job)
        elif job.sql_connection_info:
            if job.sql_driver == 'SQL Server':
                from workers import sql_worker
                sql_worker.run_job(job)
            elif job.sql_driver == 'Oracle':
                from workers import oracle_worker
                oracle_worker.run_job(job)
            elif 'MySQL' in job.sql_driver:
                from workers import mysql_worker
                mysql_worker.run_job(job)
        else:
            sys.stdout.write("No job information.")
            sys.exit(1)
    sys.exit(0)
