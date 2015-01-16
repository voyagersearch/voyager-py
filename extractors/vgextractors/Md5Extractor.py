import hashlib

from voyager import VgDexField


class Md5Extractor:

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
        job.set_field(VgDexField.CONTENT_HASH, md.hexdigest())
