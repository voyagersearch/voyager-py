import os 
import sys
import json


def run(entry):
    new_entry = json.load(open(entry, "rb"))
    if 'fields' in new_entry['entry']:
        new_entry['entry']['fields']['fss_env_vars'] = ["%s=%s" % (k, v) for k, v in os.environ.items()]
    sys.stdout.write(json.dumps(new_entry))
    sys.stdout.flush()