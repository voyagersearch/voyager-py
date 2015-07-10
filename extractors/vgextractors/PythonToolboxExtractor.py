import os
import arcpy
from _vgdexfield import VgDexField


class PythonToolboxExtractor(object):

    @staticmethod
    def extractor():
        return "pyt"

    def get_info(self):
        return { 'name' : PythonToolboxExtractor.extractor(),
                 'description' : 'extract an Esri Python toolbox (.pyt) file',
                 'formats': [{ 'name': 'text', 'mime': 'application/vnd.esri.gp.pyt', 'priority': 10 }]
               }

    def extract(self, infile, job):
        """
        Main function to set properties of a Python toolbox (.pyt).
        :param infile: The Python toolbox file
        :param job:
        """
        toolbox_name = os.path.splitext(os.path.basename(infile))[0]
        job.set_field(VgDexField.NAME, toolbox_name)
        job.set_field(VgDexField.PATH, infile)
        job.set_field(VgDexField.FILE_EXTENSION, 'PYT')
        job.set_field(VgDexField.FORMAT, 'application/vnd.esri.gp.toolbox')
        job.set_field(VgDexField.FORMAT_CATEGORY, 'GIS')
        arcpy.ImportToolbox(infile)
        toolbox = arcpy.ListToolboxes(toolbox_name)[0]
        alias_name = toolbox[toolbox.find("(")+1:toolbox.find(")")]
        job.set_field(VgDexField.NAME_ALIAS, alias_name)
        tools = arcpy.ListTools('*_{0}'.format(alias_name))
        job.set_field('meta_tools', tools)
        job.set_field(VgDexField.TEXT, open(infile, 'rb').read())
