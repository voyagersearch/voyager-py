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
import platform
import sys


try:
    import vgextractors
    from vgextractors._extraction_worker import ExtractionWorker
    from vgextractors._job import ExtractionJob
    from vgextractors._error import VgErr
    from vgextractors._vgdexfield import VgDexField
except ImportError as ie:
    sys.stdout.write(str(ie))
    sys.exit(1)
except OSError as ose:
    sys.stdout.write("{0}. {1}.".format(str(ose), "This version of pyzmq requires 32bit Python"))
    sys.exit(1)

def get_class( kls ):
    parts = kls.split('.')
    module = ".".join(parts[:-1])
    m = __import__( module )
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m


class VoyagerExtractorPy(ExtractionWorker):

    def __init__(self):
        ExtractionWorker.__init__(self, ExtractionJob)

        # Maps extractor_name -> python_class_object
        self.extmap = {}
        for extmod in vgextractors.extractors:
            kls = get_class("vgextractors." + extmod + "." + extmod)
            self.extmap[kls.extractor()] = kls


    def run_info(self):
        extractors = []

        for kls in self.extmap.values():
            extinst = kls()
            extractors.append(extinst.get_info())

        return {
            'name':'VoyagerWorkerPy',
            'description':'Python worker',
            'version':'1.9',
            'properties':{
                'version.python':platform.python_version()
            },
            'extractors': extractors }



    def extract(self, infile, job, name):
        extkls = self.get_extractor(name)
        if extkls is None:
            # No such extractor
            pass
        else:
            extinst = extkls()
            extinst.extract(infile, job)


    def get_extractor(self, name):
        if self.extmap is None:
            self.extmap = {}
        if self.extmap.has_key(name):
            return self.extmap[name]
        return None



if __name__ == "__main__":
    VoyagerExtractorPy().run()
