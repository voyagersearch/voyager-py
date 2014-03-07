from __future__ import unicode_literals
import os
import zipfile
import voyager_tasks
from voyager_tasks.utils import status


def get_info():
    """Returns the parameter information for this geoprocessing task."""
    params = list()
    params.append({'name': 'input_items', 'type': 'VoyagerResults', 'required': 'True'})
    param_info = {'task': 'zip_files', 'params': params}
    return param_info


def execute(request):
    """Zips all input files to output.zip
    :param request: json as a dict.
    """
    from voyager_tasks.utils import task_utils
    # Retrieve input items to be clipped.
    in_data = task_utils.find(lambda p: p['name'] == 'input_items', request['params'])
    docs = in_data.get('response').get('docs')
    input_items = [v['path'] for v in docs]
    zip_file_location = request['folder']
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
