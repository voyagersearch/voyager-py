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
from voyager_worker import job
from voyager_worker import odbc_worker


if __name__ == '__main__':
    jb = job.Job(sys.argv[1])
    #jb = job.Job(r"C:\Voyager\sql_server_job.json")
    if jb.path:
        from voyager_worker import esri_worker
        esri_job = esri_worker.EsriJob(jb.job_file)
        esri_job.assign_job()
    elif jb.sql_connection_info:
        odbc_job = odbc_worker.ODBCJob(jb.job_file)
        odbc_job.assign_job()
    else:
        sys.stdout.write("No job information.")
        sys.exit(1)
    sys.exit(0)
