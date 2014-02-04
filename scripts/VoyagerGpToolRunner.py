"""---------------------------------------------------------------------------
Name:        VoyagerTaskRunner.py
Purpose:     Execute a Voyager processing task.
Author:      VoyagerSearch
Created:     12/18/2013
Updated:     01/31/2014
Copyright:   (c) VoyagerSearch 2013
---------------------------------------------------------------------------"""
import os
import sys
import json
import urllib
import shutil

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


def get_file(item):
  """TODO: Return a valid file.
  Implement when JSON format finalized. 
  Will require no dependency on arcpy!
  """
  pass

def run_job(json_file):
    """Run a Geoprocessing tool.
    Parse the json for tool name and parameters.
    """
    with open(json_file) as data_file:
        request = json.load(data_file)

        # Create the folder if it does not exist.
        if not os.path.exists(request['folder']):
            os.makedirs(request['folder'])      
                 
        # Retrieve the list of parameters.
        parameters = request['params']

        # Lookup names by key and validate as required.
        if os.path.basename(request['task']) == 'ClipData':
            import clip_data

            # Retrieve input items to be clipped.
            input_items = find(lambda p: p['name'] == 'input_items', parameters)
            docs = input_items.get('response').get('docs')
            in_data = dict((get_feature_data(v), v['name']) for v in docs)

            #in_data = [get_item(v) for v in docs]
            #output_names = [n['name'] for n in docs]
            #in_data = str(dict(zip(in_datasets, output_names)))

            # Retrieve clip geometry.
            try:
                clip_geom_wkt = find(lambda p: p['name'] == 'clip_geometry', parameters)['wkt']
            except KeyError:
                clip_geom_wkt = find(lambda p: p['name'] == 'clip_geometry', parameters)['feature']

            # Retieve the coordinate system code.
            sr_code = find(lambda p: p['name'] == 'output_projection', parameters)['code']

            # Retrive the output format type.
            output_format = find(lambda p: p['name'] == 'output_format', parameters)['value']
            output_location = request['folder']

            # Execute task.
            try:
                clip_data.clip_data(str(in_data), output_location, clip_geom_wkt, int(sr_code), output_format)
            except Exception as ex:
                sys.stderr.write(ex.message)
                sys.stderr.flush()
                sys.exit(1)

        # Success
        sys.exit(0)

if __name__ == '__main__':
    run_job(r"C:\GIS\Python\ClippingData\cliptask.json")
    #run_job(sys.argv[1])

