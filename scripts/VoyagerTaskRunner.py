"""Executes a Voyager processing task."""
import os
import sys
import json
import urllib
import argparse


__author__ = 'VoyagerSearch'
__copyright__ = 'VoyagerSearch, 2014'
__date__ = '02/07/2014'

try:
  import arcpy
except ImportError:
  pass


def find(f, seq):
  """Return first item in sequence where f(item) == True."""
  for item in seq:
    if f(item):
      return item

def get_feature_data(item):
    """Return a valid layer file or dataset path.

    Describe will fail if the layer file does not exist or
    if the layer's datasource does not exist.
    """
    try:
        dsc = arcpy.Describe(item['[lyrFile]'])
        return item['[lyrFile]']
    except Exception:
        pass

    try:
        layer_file = urllib.urlretrieve(item['[lyrURL]'])[0]
        return layer_file
    except Exception:
        return item['path']
# End get_item function


def add_to_geodatabase_task(request, parameters):
    """Runs the add to geodatabase task."""
    import add_to_geodatabase

    # Retrieve input items to be clipped.
    input_items = find(lambda p: p['name'] == 'input_items', parameters)
    docs = input_items.get('response').get('docs')
    in_data = dict((get_feature_data(v), v['name']) for v in docs)

    # Retrieve the coordinate system code.
    sr_code = find(lambda p: p['name'] == 'output_projection', parameters)['code']

    output_location = request['folder']

    # Execute task.
    try:
        add_to_geodatabase.add_to_gdb(str(in_data), output_location, int(sr_code))
    except Exception as ex:
        sys.stderr.write(ex.message)
        sys.stderr.flush()
        sys.exit(1)
# End add_to_geodatabase_task function


def clip_data_task(request, parameters):
    """Runs the clip data task."""
    import clip_data

    # Retrieve input items to be clipped.
    input_items = find(lambda p: p['name'] == 'input_items', parameters)
    docs = input_items.get('response').get('docs')
    in_data = dict((get_feature_data(v), v['name']) for v in docs)

    # Retrieve clip geometry.
    try:
        clip_geom_wkt = find(lambda p: p['name'] == 'clip_geometry', parameters)['wkt']
    except KeyError:
        clip_geom_wkt = find(lambda p: p['name'] == 'clip_geometry', parameters)['feature']

    # Retrieve the coordinate system code.
    sr_code = find(lambda p: p['name'] == 'output_projection', parameters)['code']

    # Retrieve the output format type.
    output_format = find(lambda p: p['name'] == 'output_format', parameters)['value']
    output_location = request['folder']

    # Execute task.
    try:
        clip_data.clip_data(str(in_data), output_location, clip_geom_wkt, int(sr_code), output_format)
    except Exception as ex:
        sys.stderr.write(ex.message)
        sys.stderr.flush()
        sys.exit(1)
# End clip_data_task function


def convert_to_kml_task(request, parameters):
    """Runs the convert to kml task."""
    import convert_to_kml

    # Retrieve input items to be clipped.
    input_items = find(lambda p: p['name'] == 'input_items', parameters)
    docs = input_items.get('response').get('docs')
    in_data = dict((get_feature_data(v), v['name']) for v in docs)
    extent = find(lambda p: p['name'] == 'extent', parameters)['wkt']
    output_location = request['folder']

    # Execute task.
    try:
        convert_to_kml.convert_to_kml(str(in_data), output_location, extent)
    except Exception as ex:
        sys.stderr.write(ex.message)
        sys.stderr.flush()
        sys.exit(1)
# End convert_to_kml_task function


def zip_files_task(request, parameters):
    """Runs the zip files task."""
    import zip_files

    # Retrieve input items to be clipped.
    input_items = find(lambda p: p['name'] == 'input_items', parameters)
    docs = input_items.get('response').get('docs')
    in_data = [v['path'] for v in docs]
    output_location = request['folder']

    try:
        zip_files.zip_files(in_data, output_location)
    except Exception as ex:
        sys.stderr.write(ex.message)
        sys.stderr.flush()
        sys.exit(1)
# End zip_files function


def run_task(json_file):
    """Main function for running processing tasks."""
    with open(json_file) as data_file:
        request = json.load(data_file)

        # Create the folder if it does not exist.
        if not os.path.exists(request['folder']):
            os.makedirs(request['folder'])

        # Retrieve the list of parameters.
        parameters = request['params']

        if os.path.basename(request['task']) == 'add_to_geodatabase':
            add_to_geodatabase_task(request, parameters)
        elif os.path.basename(request['task']) == 'clip_data':
            clip_data_task(request, parameters)
        elif os.path.basename(request['task']) == 'convert_to_kml':
            convert_to_kml_task(request, parameters)
        elif os.path.basename(request['task']) == 'zip_files':
            zip_files_task(request, parameters)

        # Success
        sys.exit(0)
# End run_task function

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--info', action='store_true', help='Get all task parameter information.')
    parser.add_argument('json', help='Provide a json file with parameter information.')
    args = vars(parser.parse_args())
    if args['info']:
        import get_parameter_info
        task_info = get_parameter_info.TaskInfo()
        task_info()
    else:
        run_task(args['json'])
    #run_job(r"C:\NewfoundGEO\Clients\Voyager\voyager-processing\scripts\zipfilestask.json")


