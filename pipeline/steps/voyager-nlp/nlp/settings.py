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
import glob
import platform


# Import required Python libraries required for NLP.
def append_or_set_path(path):
  try:
    p = os.environ['PATH']
    if len(p) > 0 and not p.endswith(os.pathsep):
        p += os.pathsep
    p += path
    os.environ['PATH'] = p
  except KeyError:
    os.environ['PATH'] = path

# Add Python dependent libraries to the system paths.
arch_dir = 'win32_x86'
if platform.system() == 'Darwin':
    arch_dir = 'darwin_x86_64'
elif platform.system() == 'Linux':
    arch_dir = 'linux_amd64'

dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), '..', 'arch', arch_dir))
append_or_set_path(dll_path)

egg_path = os.path.join(dll_path, 'py')
sys.path.append(egg_path)
sys.path.append(os.path.dirname(dll_path))
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)


"""Settings for the nlp service."""
SERVICE_ADDRESS = 'localhost'
SERVICE_PORT = 9999

base_path = os.path.dirname(__file__)
""" Change this path to the location you would like the logs to be written to. """
LOG_FILE_PATH = os.path.join(base_path, 'logs')
if not os.path.exists(LOG_FILE_PATH):
    os.makedirs(LOG_FILE_PATH)
