import os
import json
from _vgdexfield import VgDexField


class JSONExtractor(object):

    @staticmethod
    def extractor():
        return "json"

    def get_info(self):
        return { 'name' : JSONExtractor.extractor(),
                 'description' : 'Extract a JSON file using Python',
                 'formats': [{ 'name': 'text', 'mime': 'application/json', 'priority': 10 }]
               }

    def extract(self, infile, job):
        """
        Main function to set properties of a JSON file.
        The JSON is loaded as a dictionary and each key is set as a field.
        :param infile: The JSON file
        :param job: A job object
        """

        # Set some of the most basic properties.
        json_name = os.path.splitext(os.path.basename(infile))[0]
        job.set_field(VgDexField.NAME, json_name)
        job.set_field(VgDexField.PATH, infile)
        job.set_field(VgDexField.FILE_EXTENSION, 'json')
        job.set_field(VgDexField.FORMAT, 'application/json')
        job.set_field(VgDexField.FORMAT_CATEGORY, 'JSON')

        # Load file into dictionary.
        with open(infile, 'rb') as json_file:
            json_dict = json.load(json_file)

        for key, value in json_dict.iteritems():
            if isinstance(value, str) or isinstance(value, unicode):
                job.set_field('fs_{0}'.format(key), value)
            elif isinstance(value, int):
                job.set_field('fl_{0}'.format(key), value)
            elif isinstance(value, float):
                job.set_field('fu_{0}'.format(key), value)
            else:
                job.set_field('meta_{0}'.format(key), value)

        # Finally, read the file and set the text field.
        job.set_field(VgDexField.TEXT, json.dumps(json_dict))
