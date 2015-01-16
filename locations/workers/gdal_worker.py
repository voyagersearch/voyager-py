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
import base_job
import ogr
import gdalconst


def global_job(args):
    """Create a global job object for multiprocessing."""
    global job
    job = args


def worker():
    """Worker function to index each geometry in each feature in the datasource."""
    job.connect_to_zmq()
    ds = ogr.Open(job.url, gdalconst.GA_ReadOnly)
    # Layers
    for l in ds:
        # Fields
        fnames = []
        ldef = l.GetLayerDefn()

        # Layer name
        ln = ldef.GetName()

        # process or skip this layer
        cont = '*' in job.tables_to_keep or ln.lower() in [t.lower() for t in job.tables_to_keep]

        if cont:
            for i in range(ldef.GetFieldCount()):
                fn = ldef.GetFieldDefn(i).GetName()
                if '*' in job.fields_to_keep or fn.lower() in [f.lower() for f in job.fields_to_keep]:
                    fnames.append(fn)

            # Features
            for f in l:
                # Geometry
                g = f.GetGeometryRef()
                # Geomtry type
                t = g.GetGeometryType()
                geo = {}
                if t == 1: # point
                    geo['lon'] = g.GetX()
                    geo['lat'] = g.GetY()
                else:
                    e = g.GetEnvelope()
                    geo['xmin'] = e[0]
                    geo['xmax'] = e[1]
                    geo['ymin'] = e[2]
                    geo['ymax'] = e[3]

                fvalues = []
                for i in fnames:
                    fvalues.append(f.GetField(i))

                entry = {}
                entry['id'] = '{0}_{1}_{2}'.format(job.location_id, l.GetName(), f.GetFID())
                entry['location'] = job.location_id
                entry['action'] = job.action_type
                entry['entry'] = {'geo': geo, 'fields': dict(zip(job.map_fields(ln, fnames), fvalues))}
                job.send_entry(entry)


def assign_job(job_info):
    """Connects to ZMQ, opens the datasource, and assigns the job."""
    job = base_job.Job(job_info)
    global_job(job)
    worker()
