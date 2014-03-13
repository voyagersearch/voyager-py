from __future__ import unicode_literals
import os
import zipfile
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


class ZipFileManager(zipfile.ZipFile):
    def __init__(self, zip_file, mode='r', compression=zipfile.ZIP_DEFLATED):
        zipfile.ZipFile.__init__(self, zip_file, mode, compression)

    def __enter__(self):
        """Return object created in __init__ part"""
        return self

    def __exit__(self, exc_type, exc_value, trace_back):
        """Close zipfile.ZipFile"""
        self.close()


def execute(request):
    """Zips all input files to output.zip
    :param request: json as a dict.
    """
    in_data = task_utils.find(lambda p: p['name'] == 'input_items', request['params'])
    docs = in_data.get('response').get('docs')
    input_items = [v['path'] for v in docs]
    zip_file_location = request['folder']
    if not os.path.exists(zip_file_location):
        os.makedirs(request['folder'])
    file_count = len(input_items)
    i = 1.
    status_writer = status.Writer()
    status_writer.send_status('Starting to zip files...')
    zip_file = os.path.join(zip_file_location, 'output.zip')
    with ZipFileManager(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipper:
        for in_file in input_items:
            if os.path.isfile(in_file):
                zipper.write(in_file, os.path.basename(in_file))
                status_writer.send_percent(i/file_count, 'Zipped {0}.'.format(in_file), 'zip_files')
            else:
                status_writer.send_percent(i/file_count, '{0} is not a file or does not exist.'.format(in_file), 'zip_files')
            i += 1.0

    status_writer.send_status('Completed.')
