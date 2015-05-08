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
import hashlib
import _vgdexfield


class Md5Extractor(object):

    @staticmethod
    def extractor():
        return "md5"

    def get_info(self):
        return { 'name' : Md5Extractor.extractor(),
                 'description' : 'compute md5 hash of input file',
                 'formats': [
		  { 'name': 'text',
		    'mime': 'text/plain',
		    'priority': 10 },
		  { 'name': 'shapefile',
		    'mime': 'application/vnd.esri.shapefile',
		    'priority': 4 }
                 ]
               }


    def extract(self, infile, job):
        md = hashlib.md5()
        with open(infile, 'r') as f:
            while 1:
                data = f.read(128)
                if len(data) == 0:
                    break
                md.update(data)
                if len(data) < 128:
                    break
        job.set_field(_vgdexfield.VgDexField.CONTENT_HASH, md.hexdigest())
