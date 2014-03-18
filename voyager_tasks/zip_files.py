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
    zipped = 0
    skipped = 0
    in_data = task_utils.find(lambda p: p['name'] == 'input_items', request['params'])
    docs = in_data.get('response').get('docs')
    input_items = [v['path'] for v in docs]
    try:
        flatten_results = task_utils.find(lambda p: p['name'] == 'flatten_results', request['params'])['value']
    except KeyError:
        flatten_results = 'false'
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
                if flatten_results == 'true':
                    zipper.write(in_file, os.path.basename(in_file))
                else:
                    zipper.write(
                        in_file,
                        os.path.join(os.path.abspath(os.path.join(in_file, os.pardir)), os.path.basename(in_file))
                    )
                status_writer.send_percent(i/file_count, 'Zipped {0}.'.format(in_file), 'zip_files')
                zipped += 1
            else:
                status_writer.send_percent(i/file_count, '{0} is not a file or does not exist.'.format(in_file), 'zip_files')
                skipped += 1
            i += 1.0

    if zipped == 0:
        status_writer.send_status('No files were zipped.')
    else:
        status_writer.send_status('Zipped {0} files.'.format(zipped))
    task_utils.report(os.path.join(request['folder'], '_report.md'), request['task'], zipped, skipped)
