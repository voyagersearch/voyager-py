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


"""Import required Python libraries required for NLP."""
path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'nlp-libs'))
egg_files = glob.glob(os.path.join(path, '*.egg'))
egg_files += glob.glob(os.path.join(path, '*', '*.egg'))
for egg_file in egg_files:
    sys.path.append(egg_file)


"""Settings for the nlp service."""
SERVICE_ADDRESS = 'localhost'
SERVICE_PORT = 9999

base_path = os.path.dirname(__file__)
""" Change this path to the location you would like the logs to be written to. """
LOG_FILE_PATH = os.path.join(base_path, 'logs')
if not os.path.exists(LOG_FILE_PATH):
    os.makedirs(LOG_FILE_PATH)
