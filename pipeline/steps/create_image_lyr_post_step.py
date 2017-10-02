import urllib2
import sys
import json
from layer_creation import settings


def run(entry):
    """
    Creates a layer file (.lyr) in the Voyager meta location for a GIS item that currently has no associated layer file.
    This pipeline step supports image service layers.
    :param entry: a JSON file containing a voyager entry.
    """
    req = urllib2.Request("http://{0}:{1}/createimagelayers".format(settings.SERVICE_ADDRESS, settings.SERVICE_PORT),
                          json.dumps(json.load(open(entry, "rb"))))
    response = urllib2.urlopen(req)
    result = response.read()
    sys.stdout.write(result)
    sys.stdout.flush()
