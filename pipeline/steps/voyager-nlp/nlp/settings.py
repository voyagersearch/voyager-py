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


"""Settings for the nlp service."""
SERVICE_ADDRESS = 'localhost'
SERVICE_PORT = 8081

base_path = os.path.dirname(__file__)
""" Change this path to the location you would like the logs to be written to. """
LOG_FILE_PATH = os.path.join(base_path, 'Volumes/Untitled/tmp')
if not os.path.exists(LOG_FILE_PATH):
    os.makedirs(LOG_FILE_PATH)
