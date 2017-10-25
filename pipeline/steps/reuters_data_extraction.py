import sys
import json
import xml.etree.ElementTree as ET
import logging
# import time
import datetime
from steps.utils.dc_map import DC_MAP
from steps.utils import settings


logging.basicConfig(filename="{0}/reuters_data_extraction.log".format(settings.LOG_FILE_PATH),
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

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

            entry_json['entry']['fields']['ft_story'] = content(root.find('text'))
            entry_json['entry']['fields']['text'] = content(root.find('text'))

            entry_json['entry']['fields']['fs_headline'] = root.find('headline').text
            entry_json['entry']['fields']['fs_copyright'] = root.find('copyright').text

            entry_json['entry']['fields']['fs_byline'] = root.get('byline')
            entry_json['entry']['fields']['fs_dateline'] = root.get('dateline')
            entry_json['entry']['fields']['fs_itemid'] = root.get('itemid')
            entry_json['entry']['fields']['fd_date'] = string_to_timestamp(root.get('date'))

            metadata = root.find('metadata')
            dcnodes = metadata.findall('dc')
            codesnodes = metadata.findall('codes')

            keys = DC_MAP.keys()

            for dc in dcnodes:
                try:
                    element = dc.get('element')
                    value = dc.get('value')
                    field = DC_MAP[element]

                    entry_fields = entry_json['entry']['fields']
                    current_fields = entry_fields.keys()

                    if element in keys and value is not "":
                        if field.startswith('fdd_'):
                            try:
                                str_time = string_to_timestamp(value)
                                if field not in current_fields or (field in current_fields and value not in entry_fields[field]):
                                    entry_fields.setdefault(field, []).append(str_time)
                            except Exception as e:
                                logging.error("could not convert date: %s " % value)
                                logging.error(e)
                        elif field not in current_fields or (field in current_fields and value not in entry_fields[field]):
                            entry_fields.setdefault(field, []).append(value)

                    else:
                        logging.info('%s not found:' % element)
                        entry_fields.setdefault("meta_%s" % element.replace('.', '_'), []).append(value)

                except Exception as e:
                    logging.error("exception occurred adding %s " % element)
                    logging.error("could not parse dc node.\n %s: %s " % (content(dc), e))
                    logging.error("metadata: %s " % content(metadata))

            for codesnode in codesnodes:
                try:
                    code_type = codesnode.get('class')

                    if code_type == 'bip:countries:1.0':
                        codes = codesnode.findall('code')
                        for code in codes:
                            entry_fields.setdefault("fss_region_code", []).append(code.get('code'))
                    elif code_type == 'bip:topics:1.0':
                        codes = codesnode.findall('code')
                        for code in codes:
                            entry_fields.setdefault("fss_topic_code", []).append(code.get('code'))
                    elif code_type == 'bip:industries:1.0':
                        codes = codesnode.findall('code')
                        for code in codes:
                            entry_fields.setdefault("fss_industry_code", []).append(code.get('code'))
                    else:
                        logging.info('could not map code type: %s' % code_type)

                except Exception as e:
                    logging.error("exception occurred adding %s " % element)
                    logging.error("could not parse code node.\n %s: %s " % (content(dc), e))
                    logging.error("metadata: %s " % metadata)

            # logging.info(json.dumps(entry_json, indent=4))

            sys.stdout.write(json.dumps(entry_json))
            sys.stdout.flush()

    except Exception:
        sys.stdout.flush()