import os
import sys
import glob
import shutil
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def get_files(source_file, file_extensions):
    """Returns a list of files for each file type.
    :param source_file: source file path
    :param file_extensions: list of file extensions - i.e. ['*.shp', '*.prj']
    :rtype : list
    """
    folder_location = os.path.dirname(source_file)
    file_name = os.path.basename(source_file)[:-4]
    all_files = []
    for ext in file_extensions:
        all_files.extend(glob.glob(os.path.join(folder_location, '{0}.{1}'.format(file_name, ext))))
    return all_files


def execute(request):
    """Copies files data to a target folder.
    :param request: json as a dict.
    """
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    target_folder = task_utils.get_parameter_value(parameters, 'target_folder', 'value')
    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])
    try:
        flatten_results = task_utils.get_parameter_value(parameters, 'flatten_results', 'value')
    except KeyError:
        target_dirs = os.path.splitdrive(target_folder)[1]
        flatten_results = 'false'

    i = 1.
    copied = 0
    skipped = 0
    file_count = len(input_items)
    shp_files = ('shp', 'shx', 'sbn', 'dbf', 'prj', 'cpg', 'shp.xml', 'dbf.xml')
    sdc_files = ('sdc', 'sdi', 'sdc.xml', 'sdc.prj')
    status_writer = status.Writer()

    for src_file in input_items:
        try:
            if os.path.isfile(src_file) or src_file.endswith('.gdb'):
                if flatten_results == 'false':
                    # Maintain source file's folder structure.
                    copy_dirs = os.path.splitdrive(os.path.dirname(src_file))[1]
                    if not copy_dirs == target_dirs:
                        dst = target_folder + copy_dirs
                        if not os.path.exists(dst):
                            os.makedirs(dst)
                else:
                    if not os.path.exists(target_folder):
                        dst = target_folder
                        os.makedirs(dst)
                    else:
                        dst = target_folder
                if os.path.isfile(src_file):
                    if src_file.endswith('.shp'):
                        all_files = get_files(src_file, shp_files)
                    elif src_file.endswith('.sdc'):
                        all_files = get_files(src_file, sdc_files)
                    else:
                        all_files = [src_file]
                    for f in all_files:
                        shutil.copy2(f, dst)
                else:
                    shutil.copytree(src_file, os.path.join(dst, os.path.basename(src_file)))
                status_writer.send_percent(i/file_count, 'Copied {0}.'.format(src_file), 'copy_files')
                copied += 1
            else:
                status_writer.send_percent(
                    i/file_count,
                    '{0} is not a file or does not exist.'.format(src_file),
                    'copy_files'
                )
                skipped += 1
        except IOError as io_err:
            status_writer.send_percent(
                i/file_count,
                'Failed to copy: {0}. {1}.'.format(src_file, repr(io_err)),
                'copy_files'
            )
            skipped += 1
            pass

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.getcwd()), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        status_writer.send_status('Could not copy thumbnail.')
        pass
    try:
        task_utils.report(os.path.join(request['folder'], '_report.md'), request['task'], copied, skipped)
    except IOError:
        status_writer.send_status('Could not create report file.')
        pass
    # Update state if necessary.
    if copied == 0:
        status_writer.send_state(status.STAT_FAILED, 'All results failed to copy.')
        sys.exit(1)
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, '{0} results could not be copied.'.format(skipped))