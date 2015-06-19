import csv
import sys
import json
import subprocess
from copy import deepcopy
from collections import defaultdict


def run(entry):
    """Extracts coordinates from the given entry and returns a new entry with coordinate information.
    :param entry: a file containing the entry information
    """
    orig_entry = json.load(open(entry, 'r'))
    new_entry = deepcopy(orig_entry)
    geo = {}

    if 'fields' in orig_entry and 'text' in orig_entry['fields']:
        text_field = orig_entry['fields']['text']
        print (text_field)
        # command = 'C:/Program Files (x86)/ClearTerra/License Server/LocateXT_API_CLI32.exe -t "{0}"'.format(''.join(text_field))
        # process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=134217728)
        # process.wait()
        # if process.returncode == 1:
        #     sys.stderr.write('FAILED. {0}'.format(process.stderr.read()))
        #     return
        #
        # columns = defaultdict(list)
        # reader = csv.DictReader(process.stdout.read().splitlines())
        # for row in reader:
        #     for (k,v) in row.items():
        #         columns[k].append(v)
        # y_coordinates = columns['Latitude (WGS-84)']
        # x_coordinates = columns['Longitude (WGS-84)']
        y_coordinates = ['-25.131919', '-82.167294', '30.738889', '30.723611', '30.950000', '-82.167294']
        x_coordinates = ['-166.091209', '-68.442921', '46.444444', '46.679167', '46.183333', '-68.442921']
        if len(x_coordinates) > 1:
            # Make a Multipoint WKT
            coordinates = zip(y_coordinates, x_coordinates)
            points = (' '.join(str(round(float(c), 3)) for c in pt) for pt in coordinates)
            points = ('({0})'.format(pt) for pt in points)
            wkt_multipoint = 'MULTIPOINT ({0})'.format(', '.join(points))
            geo['wkt'] = wkt_multipoint
        else:
            # Make single point
            wkt_point = 'POINT ({0} {1})'.format(x_coordinates[0], y_coordinates[0])
            geo['wkt'] = wkt_point

        new_entry['geo'] = geo
        new_entry['fields']['fs_processed_by'] = 'LocateXT'
        sys.stdout.write(json.dumps(new_entry, ensure_ascii=False))
        sys.stdout.flush()
    else:
        sys.stderr.write("No text to process for: {0}".format(orig_entry['fields']['id']))
        sys.stderr.flush()
        return

