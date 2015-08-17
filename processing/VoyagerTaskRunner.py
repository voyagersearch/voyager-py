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
"""Executes a Voyager processing task."""
import os
import collections
import json
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'tasks'))
import tasks
from tasks import utils


def run_task(json_file):
    """Main function for running processing tasks."""
    with open(json_file) as data_file:
        try:
            request = json.load(data_file)
            __import__(request['task'])
            getattr(sys.modules[request['task']], "execute")(request)
        except (ImportError, ValueError) as ex:
            sys.stderr.write(repr(ex))
            sys.exit(1)


if __name__ == '__main__':
    if sys.argv[1] == '--info':
        # Metadata GP tools do not work in Python with ArcGIS 10.0
        task_info = collections.defaultdict(list)
        info_dir = os.path.join(os.path.dirname(__file__), 'info')
        for task in tasks.__all__:
                if task not in ('ogr', 'utils', 'sample_task', 'template_task', 'dev_pretend_py'):
                    # Validate the .info.json file.
                    task_properties = collections.OrderedDict()
                    task_properties['name'] = task
                    task_properties['available'] = True
                    fp = None
                    fp = open(os.path.join(info_dir, '{0}.info.json'.format(task)))
                    try:
                        d = json.load(fp)
                    except ValueError as ve:
                        task_properties['available'] = False
                        task_properties['JSON syntax error'] = str(ve)
                    finally:
                        if fp:
                            fp.close()

                    # Validate the Python code.
                    try:
                        __import__(task)
                    except ImportError as ie:
                        if 'arcpy' in ie:
                            task_properties['available'] = False
                            task_properties['Import error'] = '{0}. Requires ArcGIS'.format(str(ie))
                        else:
                            task_properties['available'] = False
                            task_properties['Import error'] = str(ie)
                    except RuntimeError as re:
                        if 'NotInitialized' in re:
                            task_properties['available'] = False
                            task_properties['License error'] = '{0}. ArcGIS is not licensed.'.format(str(re))
                        else:
                            task_properties['available'] = False
                            task_properties['Error'] = str(re)
                    except SyntaxError as se:
                        task_properties['available'] = False
                        task_properties['Python syntax error'] = str(se)

                    task_info['tasks'].append(task_properties)
        sys.stdout.write(json.dumps(task_info, indent=2))
        sys.stdout.flush()
    elif sys.argv[1] == '--license':
        import arcpy
        with open(os.path.join(os.path.dirname(__file__), 'supportfiles', 'licenses.json'), 'r') as fp:
            licenses = json.load(fp)
            for product in licenses['product']:
                product['status'] = arcpy.CheckProduct(product['code'])
            for extension in licenses['extension']:
                extension['status'] = arcpy.CheckExtension(extension['code'])
        [licenses['extension'].remove(e) for e in licenses['extension'] if e['status'].startswith('Unrecognized')]
        sys.stdout.write(json.dumps(licenses, indent=2))
        sys.stdout.flush()
    else:
        run_task(sys.argv[1])
    sys.exit(0)
