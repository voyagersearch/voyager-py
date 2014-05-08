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


def make_mo_files():
    """Utility function to generate MO files."""
    po_files = glob.glob(os.path.join(os.path.dirname(__file__), 'LC_MESSAGES', '*.po'))
    try:
        sys.path.append(os.path.join(os.path.dirname(sys.executable), "tools", "i18n"))
        import msgfmt
        for po_file in po_files:
            msgfmt.make(po_file, po_file.replace('.po', '.mo'))
    except (IOError, ImportError):
        pass

if __name__ == '__main__':
    make_mo_files()
