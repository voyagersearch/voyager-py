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
"""Submits a indexing job for a data location."""
import sys
from voyager_worker import base_job
from voyager_worker import sql_worker


if __name__ == '__main__':
    job = base_job.Job(sys.argv[1])
    #job = base_job.Job(r"C:\Voyager\sql_server_field_mapping.json")
    #job = base_job.Job(r"C:\Temp\GIFD_DATA_TEST.json")
    if job.path:
        from voyager_worker import esri_worker
        esri_worker.assign_work(job.job_file)
    elif job.sql_connection_info:
        sql_worker.assign_job(job.job_file)
    else:
        sys.stdout.write("No job information.")
        sys.exit(1)
    sys.exit(0)
