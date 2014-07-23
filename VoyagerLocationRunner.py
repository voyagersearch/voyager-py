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
import voyager_worker
from voyager_worker import base_job

if __name__ == '__main__':
    job = base_job.Job(sys.argv[1])
    if job.path:
        from voyager_worker import esri_worker
        esri_worker.assign_work(job)
    elif job.url:
        from voyager_worker import gdal_worker
        gdal_worker.assign_job(job.job_file)
    elif job.sql_connection_info:
        if job.sql_driver == 'SQL Server':
            from voyager_worker import sql_worker
            sql_worker.assign_job(job.job_file)
        elif job.sql_driver == 'Oracle':
            from voyager_worker import oracle_worker
            oracle_worker.assign_job(job.job_file)
    else:
        sys.stdout.write("No job information.")
        sys.exit(1)
    sys.exit(0)
