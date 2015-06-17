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
"""Executes a Voyager pipeline step."""
import collections
import json
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipelines'))
import pipelines


def run_pipeline_step(pipeline_name, entry_file):
    """Main function for running pipeline steps."""
    with open(entry_file) as data_file:
        try:
            __import__(pipeline_name)
            getattr(sys.modules[pipeline_name], "run")(entry_file)
        except (ImportError, ValueError) as ex:
            sys.stderr.write(repr(ex))
            sys.exit(1)


if __name__ == '__main__':
    if sys.argv[1] == '--info':
        pipeline_info = collections.defaultdict(list)
        for pipeline in pipelines.__all__:
            try:
                __import__(pipeline)
                pipeline_info['pipeline'].append({'name': pipeline, 'available': True})
            except (ImportError, RuntimeError) as ie:
                pipeline_info['pipeline'].append({'name': pipeline, 'available': False, 'warning': str(ie)})

        sys.stdout.write(json.dumps(pipeline_info, indent=2))
        sys.stdout.flush()
    else:
        run_pipeline_step(sys.argv[1], sys.argv[2])
    sys.exit(0)
