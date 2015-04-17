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
import glob
import locale
import gettext
import sys


__all__ = []
for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or module[-3:] != '.py':
        continue
    __all__.append(module[:-3])

dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'arch', 'win32_x86'))
ogr2ogr = os.path.join(dll_path, 'ogr2ogr.exe')
if os.environ['PATH'].endswith(';'):
    os.environ['PATH'] += dll_path
else:
    os.environ['PATH'] += os.pathsep + dll_path
egg_path = os.path.join(dll_path, 'py')
sys.path.append(egg_path)
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)
__all__.append('ogr')

# Code for translating task messages.
locale.setlocale(locale.LC_ALL, '')
loc = locale.getlocale()[0].lower()[0:2]
try:
    sys.path.append(os.path.join(os.path.dirname(sys.executable), "tools", "i18n"))
    import msgfmt
    mo_filename = os.path.join(os.path.dirname(__file__), "locale", "LC_MESSAGES", "messages_%s.mo" % loc)
    po_filename = os.path.join(os.path.dirname(__file__), "locale", "LC_MESSAGES", "messages_%s.po" % loc)
    if not os.path.exists(mo_filename) and os.path.exists(po_filename):
        msgfmt.make(po_filename, mo_filename)
    trans = gettext.GNUTranslations(open(mo_filename, "rb"))
except (IOError, ImportError):
    trans = gettext.NullTranslations()

# This has changed in Python 3.x
if sys.version_info[0] < 3:
    trans.install(unicode=True)
    _ = trans.ugettext
else:
    trans.install()
    _ = trans.gettext
