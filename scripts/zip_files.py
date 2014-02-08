from __future__ import unicode_literals
import os
import zipfile
import status

__author__ = 'VoyagerSearch'
__copyright__ = 'VoyagerSearch, 2014'
__date__ = '02/07/2014'


def zip_files(input_items, zip_file_location):
    """Zips up all input files into a compressed zip file
    named output.zip
    """
    file_count = len(input_items)
    i = 1.
    status_writer = status.Writer()
    status_writer.send_status('Starting to zip files...')
    zip_file = os.path.join(zip_file_location, 'output.zip')
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipper:
        for in_file in input_items:
            if os.path.isfile(in_file):
                zipper.write(in_file, os.path.basename(in_file))
                status_writer.send_percent(i/file_count, 'Zipped {0}.'.format(in_file), 'zip_files')
            else:
                status_writer.send_percent(i/file_count, '{0} is not a file.'.format(in_file), 'zip_files')
            i += 1.

    status_writer.send_status('Completed.')
# End zip_files function

