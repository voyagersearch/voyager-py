from __future__ import unicode_literals
import sys
import json
import logging
import urllib2
import urllib
import csv
from unidecode import unidecode
import unicodedata
from collections import OrderedDict

from unidecode import unidecode
from steps.utils import settings, nlp_settings



NLP_FIELDS = ['text', 'fulltext', 'description']
NLP_GEO_KEYS = ['GPE', 'LOC']
NLP_SOLR_MAPPING = {
    'FIELD': 'fss_nlp_bp_field',
    'WELL': 'fss_nlp_bp_well',
    'BASIN': 'fss_nlp_bp_basin'
}

logging.basicConfig(filename="{0}/nlp_worker.log".format(settings.LOG_FILE_PATH),
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")


def post_to_nlp_service(text):
    """ posts content to the nlp service """
    text = unicode(text).encode('utf-8')
    text = urllib.quote(text)
    try:
        req = urllib2.Request("http://{0}:{1}/nlp".format('localhost', 8001), text)
        response = urllib2.urlopen(req)
        result = response.read()
        logging.debug('sent %s chars to nlp, got back %s chars', len(text), len(result))
        return result
    except Exception as e:
        logging.error('sent %s chars to nlp, got back error: %s', len(text), e)

def clean_path_parts(*args):
    for arg in args:
        arg = arg.replace('.', '')
    return args

def run(entry, *args):
    """ run the worker """
    _fields = NLP_FIELDS
    if args is not None and len(args) > 0:
        _fields = list(args)

    new_entry = json.load(open(entry, "rb"))

    if 'path' not in new_entry['job'].keys():
        logging.debug('no job path in this entry, skipping...')
        return

    text = ''
    for field in _fields:
        if field in new_entry['entry']['fields'].keys():
            _v = new_entry['entry']['fields'][field]
            if _v is not None:
                if isinstance(_v, list):
                    text = u'{0}, {1}'.format(text, ', '.join(_v))
                else:
                    text = u'{0}, {1}'.format(text, _v)

    nlp_items = dict()

    # text = ('There was evidence to suggest depth influenced the community in the ' 
    #             'Taurus-Libra site area, as greater numbers of species and individuals were recorded '
    #             'in the shallower, south-eastern region of the survey area; a pattern considered typical for this region.'
    #             'Caracal Water Well Drilling Programme WSW1-64/1 Well WSW1-64/1 Programme Status '
    #             'Revision 4 FINAL Operation Water Well Drilling (Exploration) Issue Date 22 July 2010 BP North Africa SPU Libya Onshore '
    #             'Exploration WSW1-64/1 Water Well Drilling Programme Caracal NE Ghadames Basin Document No: 100714 Distribution List Company Position '
    #             'Copy NOC National Oil Company Representative 1, 2* B P Sunbury Wells P g ro ramme Manager 3 Lead Drilling Engineer 4 Geologist 5 B P '
    #             'Libya, Tripoli E xploration Manager 6 Wells Team Leader 7 Lead Drilling Engineer 8 Drilling Engineer 9 Operational Geologist 10 B P Libya, '
    #             'Wellsit W e ellsit L e eader 11 Wellsite Geologist 12 R ig Contract or WDI 802 T oolpusher 13 WDI 802 Rig Manager 14 WDI 802 Driller '
    #             '15 Service Comp anies Halliburton Cement 16 * Weatherford Managed Pressure Drilling 17 * * denotes electronic copy 1 Caracal Water Well '
    #             'Drilling Programme WSW1-64/1 Well WSW1-64/1 Programme Status Revision 4 FINAL Operation Water Well Drilling (Exploration).'
    #             'Expected Operation Next 24 Hours Install Gate valves to TG-308 C. Comments on Operations 1- Crane not supplied by Lead 2- '
    #             'No sand removal activity.'
    #             'Algeria JV: In Salah & In Amenas 12 September UpdateSafety Current Activity - In SalahYTDWellsTeamRecordables Teg-11z ; Rig T212.' 
    #             'The Geos confirmed that the Raven 1 well head box in position was to within 2m of the provided position.'
    #         )
    # test_sentence = text.decode('utf-8', 'ignore')
    # text = unidecode(test_sentence)

    if text:
        try:
            nlp_items = json.loads(post_to_nlp_service(text))
            for nlp_item_key in nlp_items['named_entities'].keys():
                if nlp_item_key in NLP_SOLR_MAPPING:
                    nlp_text_field_name = NLP_SOLR_MAPPING[nlp_item_key]
                    nlp_items['named_entities'][nlp_item_key] = filter(None, nlp_items['named_entities'][nlp_item_key])
                    new_entry['entry']['fields'][nlp_text_field_name] = nlp_items['named_entities'][nlp_item_key]

            csv_file = csv.reader(open('D:\\voyager\\voyager-py\\pipeline\\steps\\csvs\\BP_N_Africa.csv'), delimiter=str(u","))

            taxo_fields = {
                'COUNTRY': [],
                'BASIN': [],
                'FIELD': [],
                'WELL': [],
                'BH_LITHOSTRAT_UNIT': [],
                'BH_AGE': []
            }
            for row in csv_file:
                for well in nlp_items['named_entities']['WELL']:
                    well = well.upper().lower()
                    if well == row[4].upper().lower() or well == row[5].upper().lower() or well == row[6].upper().lower():
                        taxo_fields.setdefault('COUNTRY', []).append(row[2])
                        taxo_fields.setdefault('BASIN', []).append(row[3])
                        taxo_fields.setdefault('FIELD', []).append(row[8])
                        # taxo_fields.setdefault('BLOCK', []).append(row[9])
                        taxo_fields.setdefault('WELL', []).append(row[4]) # well name
                        taxo_fields.setdefault('WELL', []).append(row[5]) # alt well name
                        taxo_fields.setdefault('WELL', []).append(row[6]) # alt well name 2
                        taxo_fields.setdefault('BH_LITHOSTRAT_UNIT', []).append(row[9])
                        taxo_fields.setdefault('BH_AGE', []).append(row[10])
#                        taxo_fields.setdefault('PATH_WELL', []).append( '.'.join(list(clean_path_parts(row[2], row[3], row[9], row[8], row[4]))))

                for field in nlp_items['named_entities']['FIELD']:
                    if field.upper().lower() in row[8].upper().lower():
                        taxo_fields.setdefault('COUNTRY', []).append(row[2])
                        taxo_fields.setdefault('BASIN', []).append(row[3])
                        taxo_fields.setdefault('FIELD', []).append(row[8])
                        # taxo_fields.setdefault('BLOCK', []).append(row[9])
                        taxo_fields.setdefault('BH_LITHOSTRAT_UNIT', []).append(row[9])
                        taxo_fields.setdefault('BH_AGE', []).append(row[10])
#                       taxo_fields.setdefault('PATH_FIELD', []).append( '.'.join(list(clean_path_parts(row[2], row[3], row[9], row[8]))))

                # for block in nlp_items['named_entities']['BLOCK']:
                #     if block.upper().lower() in row[9].upper().lower():
                #         taxo_fields.setdefault('COUNTRY', []).append(row[2])
                #         taxo_fields.setdefault('BASIN', []).append(row[3])
                #         taxo_fields.setdefault('BLOCK', []).append(row[9])
#                        taxo_fields.setdefault('PATH_BLOCK', []).append( '.'.join(list(clean_path_parts(row[2], row[3], row[9]))))

                for basin in nlp_items['named_entities']['BASIN']:
                    if basin.upper().lower() in row[3].upper().lower():
                        taxo_fields.setdefault('COUNTRY', []).append(row[2])
                        taxo_fields.setdefault('BASIN', []).append(row[3])
                        taxo_fields.setdefault('BH_LITHOSTRAT_UNIT', []).append(row[9])
                        taxo_fields.setdefault('BH_AGE', []).append(row[10])
#                        taxo_fields.setdefault('PATH_BASIN', []).append( '.'.join(list(clean_path_parts(row[2], row[3]))))

            for category in taxo_fields:
                    taxo_fields[category] = filter(None, OrderedDict((x, True) for x in taxo_fields[category]).keys())

            for taxo_item_key in taxo_fields.keys():
                new_entry['entry']['fields']['fss_taxo_{0}'.format(taxo_item_key.lower())] = taxo_fields[taxo_item_key]

        except Exception as e:
            logging.error("could not get response from NLP parser. ")
            logging.error(e)

    else:
        logging.info('no text found to send to NLP in entry id %s', new_entry['entry']['fields']['id'])

    sys.stdout.write(json.dumps(new_entry, indent=4))
    sys.stdout.flush()
    