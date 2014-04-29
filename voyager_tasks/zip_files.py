import os
import sys
import shutil
import zipfile
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def execute(request):
    """Zips all input files to output.zip.
    :param request: json as a dict.
    """
    zipped = 0
    skipped = 0
    shp_files = ('shp', 'shx', 'sbn', 'dbf', 'prj', 'cpg', 'shp.xml', 'dbf.xml')
    sdc_files = ('sdc', 'sdi', 'sdc.xml', 'sdc.prj')
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')
    if not flatten_results:
        flatten_results = False
    zip_file_location = request['folder']
    if not os.path.exists(zip_file_location):
        os.makedirs(request['folder'])
    file_count = len(input_items)
    i = 1.
    status_writer = status.Writer()
    zip_file = os.path.join(zip_file_location, 'output.zip')
    with task_utils.ZipFileManager(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipper:
        for in_file in input_items:
            if os.path.isfile(in_file):
                if in_file.endswith('.shp'):
                    files = task_utils.list_files(in_file, shp_files)
                elif in_file.endswith('.sdc'):
                    files = task_utils.list_files(in_file, sdc_files)
                else:
                    files = [in_file]
                if flatten_results:
                    for f in files:
                        zipper.write(f, os.path.basename(f))
                else:
                    for f in files:
                        zipper.write(f, os.path.join(os.path.abspath(os.path.join(f, os.pardir)), os.path.basename(f)))
                status_writer.send_percent(i/file_count, 'Zipped {0}.'.format(in_file), 'zip_files')
                zipped += 1
            elif in_file.endswith('.gdb'):
                for root, dirs, files in os.walk(in_file):
                    for f in files:
                        if not f.endswith('zip'):
                            absf = os.path.join(root, f)
                            zf = absf[len(in_file) + len(os.sep):]
                            try:
                                if flatten_results:
                                    zipper.write(absf, os.path.join(os.path.basename(in_file), zf))
                                else:
                                    zipper.write(absf, os.path.join(in_file, zf))
                            except IOError:
                                # For File GDB lock files.
                                pass
            else:
                status_writer.send_percent(i/file_count,
                                           '{0} is not a file or does not exist.'.format(in_file),
                                           'zip_files')
                skipped += 1
            i += 1.0

    try:
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        status_writer.send_status('Could not copy thumbnail.')
        pass

    # Update state if necessary.
    if zipped == 0:
        status_writer.send_state(status.STAT_FAILED, 'All results failed to zip.')
        task_utils.report(os.path.join(request['folder'], '_report.md'), zipped, skipped, len(input_items), 0)
        sys.exit(1)
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, '{0} results could not be zipped.'.format(skipped))
        task_utils.report(os.path.join(request['folder'], '_report.md'), zipped, skipped, 0, skipped)