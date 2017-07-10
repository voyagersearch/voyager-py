import json
import sys
import xml.etree.ElementTree as ET
import logging
import datetime


# logging.basicConfig(filename='/Volumes/Untitled/tmp/data_extraction.log',level=logging.DEBUG)

def content(tag):
    return ''.join(ET.tostring(e) for e in tag)

def string_to_timestamp(str_d):
    try:
        str_time = datetime.datetime.strptime(str_d, "%Y-%m-%d")
        d = str_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return d
    except Exception as e:
        logging.error("could not convert date %s %s" % (str_d, e))

def run(entry):
    entry_json = json.load(open(entry, "rb"))

    try:
        if 'job' in entry_json and 'path' in entry_json['job'] and len(entry_json['job']['path']) > 0:
            tree = ET.parse(entry_json['job']['path'])
            root = tree.getroot()

            if root.find('title').text is not None:
                entry_json['entry']['fields']['title'] = root.find('title').text
            else:
                entry_json['entry']['fields']['title'] = root.find('headline').text

            text = content(root.find('text'))
            text = text.replace('<p>', '').replace('</p>', '')
            entry_json['entry']['fields']['fs_story'] = text
            entry_json['entry']['fields']['text'] = text

            entry_json['entry']['fields']['fs_headline'] = root.find('headline').text
            entry_json['entry']['fields']['fs_copyright'] = root.find('copyright').text

            entry_json['entry']['fields']['fs_byline'] = root.get('byline')
            entry_json['entry']['fields']['fs_dateline'] = root.get('dateline')
            entry_json['entry']['fields']['fs_itemid'] = root.get('itemid')
            entry_json['entry']['fields']['fd_date'] = string_to_timestamp(root.get('date'))

            sys.stdout.write(json.dumps(entry_json))
            sys.stdout.flush()

    except Exception:
        sys.stdout.flush()