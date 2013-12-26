import os
import sys
import json
import argparse
import arcpy


def parameter_info(task):
    """Get a task's parameter information."""
    arcpy.ImportToolbox(os.path.dirname(task))
    param_info = arcpy.GetParameterInfo(os.path.basename(task))
    params = []

    for pi in param_info:
        if pi.name.find('Geometry') > 0:
            params.append({'name':pi.name, 'type':'Geometry', 'wkt':pi.value})

        elif pi.name == 'Input_Items' and pi.multiValue == True:
            params.append({'name':pi.name, 'type':'VoyagerResults', 'value':[{'name':'', 'path':'', 'lyr':''}]})

        elif pi.name == 'Output_Location' or pi.name == 'Output_Geodatabase':
            continue
        else:
            try:
                if not pi.filter.list == []:
                    params.append({'name':pi.name, 'type':'StringChoice', 'value':pi.value, 'choices':pi.filter.list})
                else:
                    params.append({'name':pi.name, 'type':'String', 'value':pi.value})
            except AttributeError:
                params.append({'name':pi.name, 'type':'String', 'value':pi.value})

    sys.stdout.write(json.dumps([p for p in params]))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('task', help='The path to the task.')
    args = parser.parse_args()
    parameter_info(args.task)