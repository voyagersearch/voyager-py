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

def run_job(json_file):
    """Run a Geoprocessing tool.
    Parse the json for tool name and parameters.
    """
    with open(json_file) as data_file:
        data = json.load(data_file)
        request = data['request'][0]

        # Check for request type, if not TASK, exit.
        if request['type'] == 'TASK':
            arcpy.ImportToolbox(os.path.dirname(request['task']))
        else:
            sys.exit(1)

        # Create the folder if it does not exist.
        if not os.path.exists(data['folder']):
            os.makedirs(data['folder'])

        # Retrieve the list of parameters.
        parameters = request['params']

        # TODO: this script makes assumptions about the *order* of the path elements
        # ideally this should lookup names by key and validate as required

        # Prepare parameters for task.
        in_data = ';'.join([v['lyr'] if not v['lyr'] == '' else v['path'] for v in parameters[1]['value']])
        clip_geom_wkt = parameters[0]['wkt']
        sr_code = parameters[2]['value']
        if parameters[3]['value'] == 'FileGDB':
            output_location = os.path.join(data['folder'], 'output.gdb')
        else:
            output_location = data['folder']

        # Execute task.
        try:
            if os.path.basename(request['task']) == 'ClipData':
                arcpy.voyager.ClipData(in_data, output_location, clip_geom_wkt, sr_code)
        except arcpy.ExecuteError:
            sys.stderr.write(arcpy.GetMessages(2))
            sys.exit(1)

        # Success
        sys.exit(0)

if __name__ == '__main__':
    run_job(sys.argv[1])

