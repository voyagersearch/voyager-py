# Python worker. Use extractor name "md5" to get the md5 hash of the
# input file.
#
# Invoke something like this:
#   PYTHONPATH=path/to/voyager-1.9.1dev-py2.7.egg python VoyagerWorkerPy.py --help
#
#

import sys
import platform
import glob
import os

pwd = os.path.dirname(os.path.split(os.path.dirname(__file__))[0]) #os.path.dirname(os.path.realpath(__file__))
arch = platform.architecture()[0]
system = platform.system()
path = None

if system == 'Windows':
    if arch == '32bit': path = 'win32_x86'
    elif arch == '64bit': path = 'win_amd64'
elif system == 'Darwin': path = 'darwin_x86_64'
elif system == 'Linux':
    if arch == '32bit': path = 'linux_x86'
    elif arch == '64bit': path = 'linux_amd64'

sys.path.append(os.path.join(pwd, "arch", path, "py"))
pwd = os.path.join(pwd, 'py', 'extractors')
for f in glob.glob(os.path.join(pwd, "*.zip")): sys.path.append(f)
sys.path.append(pwd)

from voyager import ExtractionWorker
from voyager import ExtractionJob
from voyager import VgErr
from voyager import VgDexField


# Users can drop "extractors" in here
import vgextractors


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
