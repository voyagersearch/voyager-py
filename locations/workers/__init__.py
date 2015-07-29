# -*- coding: utf-8 -*-
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
import platform
import glob

__all__ = ['base_job', 'esri_worker', 'gdal_worker', 'mongodb_worker', 'sql_worker', 'oracle_worker', 'mysql_worker']

# Add dependent libraries to the system paths.
if platform.system() == 'Darwin':
    dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'arch', 'darwin_x86_64'))
    gdal_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'gdal'))
    os.environ['PATH'] += os.pathsep + dll_path
else:
    dll_path = r"C:\Voyager\server_1.9.7.3386\app\arch\win32_x86" #os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'arch', 'win32_x86'))
    gdal_path = r"C:\Voyager\server_1.9.7.3386\app\gdal"
    # dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'arch', 'win32_x86'))
    # gdal_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'gdal'))
    if os.environ['PATH'].endswith(';'):
        os.environ['PATH'] += dll_path
    else:
        os.environ['PATH'] += os.pathsep + dll_path
os.environ['GDAL_DATA'] = gdal_path
egg_path = os.path.join(dll_path, 'py')
sys.path.append(egg_path)
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)
__all__.append('ogr')
