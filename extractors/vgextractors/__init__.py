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
import glob
import platform


# Add Python dependent libraries to the system paths.
if platform.system() == 'Darwin':
    dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'arch', 'darwin_x86_64'))
    os.environ['PATH'] += os.pathsep + dll_path
else:
    # dll_path = r"C:\Voyager\server_1.9.6.3147\app\arch\win32_x86" #os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'arch', 'win32_x86'))
    dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'arch', 'win32_x86'))
    try:
        if os.environ['PATH'].endswith(';'):
            os.environ['PATH'] += dll_path
        else:
            os.environ['PATH'] += os.pathsep + dll_path
    except KeyError:
        os.environ['PATH'] += os.pathsep + dll_path
egg_path = os.path.join(dll_path, 'py')
sys.path.append(egg_path)
sys.path.append(os.path.dirname(dll_path))
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)

# Add Voyager Extractor Modules
extractors_path = os.path.dirname(__file__)
extractors = []
for ext in glob.glob(os.path.join(extractors_path, '*Extractor.py')):
    extractors.append(os.path.basename(ext)[:-3])

del os,sys,glob,extractors_path
__all__ = extractors
