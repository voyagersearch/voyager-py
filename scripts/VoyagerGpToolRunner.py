"""---------------------------------------------------------------------------
Name:        VoyagerGpToolRunner.py
Purpose:
Author:      VoyagerSearch
Created:     18/12/2013
Copyright:   (c) VoyagerSearch 2013
---------------------------------------------------------------------------"""
import os
import sys
import json
import arcpy

def find(f, seq):
  """Return first item in sequence where f(item) == True."""
  for item in seq:
    if f(item):
      return item

def run_job(json_file):
    """Run a Geoprocessing tool.
    Parse the json for tool name and parameters.
    """
    with open(json_file) as data_file:
        data = json.load(data_file)
        request = data;  # data['request'][0]

        # Load the GP Toolbox
        arcpy.ImportToolbox(os.path.dirname(request['task']))
        
        # Create the folder if it does not exist.
        if not os.path.exists(data['folder']):
            os.makedirs(data['folder'])

        # Retrieve the list of parameters.
        parameters = request['params']

        # TODO: this script makes assumptions about the *order* of the path elements
        # ideally this should lookup names by key and validate as required
        if os.path.basename(request['task']) == 'ClipData':

            # Retrieve input items to be clipped.
            input_items = find(lambda p: p['name'] == 'input_items', parameters)
            docs = input_items.get('response').get('docs')
            in_data = ';'.join([v['[lyrFile]'] if not v['[lyrFile]'] == '' else v['path'] for v in docs])

            # Retrieve clip geometry.
            clip_geom_wkt = find(lambda p: p['name'] == 'clip_geometry', parameters)['wkt']

            # Retieve the coordinate system code.
            sr_code = find(lambda p: p['name'] == 'output_projection', parameters)['code']

            # Retrive the output format type.
            output_format = find(lambda p: p['name'] == 'output_format', parameters)['value']
            if output_format == 'FileGDB':
                output_location = os.path.join(data['folder'], 'output.gdb')
            else:
                output_location = data['folder']

            # Execute task.
            try:
                arcpy.voyager.ClipData(in_data, output_location, clip_geom_wkt, sr_code, output_format)
            except arcpy.ExecuteError:
                sys.stderr.write(arcpy.GetMessages(2))
                sys.exit(1)

        # Success
        sys.exit(0)

if __name__ == '__main__':
    #run_job(r"C:\jason\scripts\cliptask.json")
    run_job(sys.argv[1])

