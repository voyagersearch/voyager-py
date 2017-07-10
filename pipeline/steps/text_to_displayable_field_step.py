import json
import sys
import logging

from steps.utils import settings

logging.basicConfig(filename="{0}/text_to_displayable_field.log".format(settings.LOG_FILE_PATH),
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

def run(entry):
    entry_json = json.load(open(entry, "rb"))

    try:
        if 'job' in entry_json and 'path' in entry_json['job'] and len(entry_json['job']['path']) > 0:
            if 'text' in entry_json['entry']['fields'] and 'fs_story' not in entry_json['entry']['fields']:
                entry_json['entry']['fields']['fs_story'] = entry_json['entry']['fields']['text']
                logging.info("set fs_story to text for id: %s", entry_json['entry']['fields']['id'])
            else:
                logging.info('did not set fs_story to text for id: %s', entry_json['entry']['fields']['id'])
            sys.stdout.write(json.dumps(entry_json))
            sys.stdout.flush()

    except Exception:
        sys.stdout.write(json.dumps(entry_json))
        sys.stdout.flush()