from __future__ import unicode_literals
import sys
import json
import logging
import urllib2
import urllib

from steps.utils import settings, nlp_settings

"""
* PERSON:  People, including fictional.
* NORP:    Nationalities or religious or political groups.
* FAC: Facilities, such as buildings, airports, highways, bridges, etc.
* ORG: Companies, agencies, institutions, etc.
* GPE: Countries, cities, states.
* LOC: Non-GPE locations, mountain ranges, bodies of water.
* PRODUCT: Vehicles, weapons, foods, etc. (Not services)
* EVENT:   Named hurricanes, battles, wars, sports events, etc.
* WORK_OF_ART: Titles of books, songs, etc.
* LAW: Named documents made into laws
* LANGUAGE:    Any named language
"""

NLP_FIELDS = ['text', 'fulltext', 'description', 'fs_story','meta_data_description','purpose','abstract']
NLP_GEO_KEYS = ['GPE', 'LOC']
NLP_SOLR_MAPPING = {
    'PERSON':   'nlp_people',
    'NORP':     'nlp_groups',
    'FAC':      'nlp_facilities',
    'ORG':      'nlp_orgs',
    'GPE':      'nlp_admin_places',
    'LOC':      'nlp_geo_places',
    'PRODUCT':  'nlp_products',
    'EVENT':    'nlp_events',
    'WORK_OF_ART': 'nlp_art',
    'LAW':      'nlp_legal',
    'LANGUAGE': 'nlp_languages'
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
        req = urllib2.Request("http://{0}:{1}/nlp".format(nlp_settings.SERVICE_ADDRESS, nlp_settings.SERVICE_PORT), text)
        response = urllib2.urlopen(req)
        result = response.read()
        logging.debug('sent %s chars to nlp, got back %s chars', len(text), len(result))
        return result
    except Exception as e:
        logging.error('sent %s chars to nlp, got back error: %s', len(text), e)


def run(entry, *args):
    """ run the worker """
    _fields = NLP_FIELDS
    if args is not None and len(args) > 0:
        _fields = list(args)

    new_entry = json.load(open(entry, "rb"))

    # if 'path' not in new_entry['job']:
    #     logging.debug('no job path in this entry, skipping...')
    #     return

    text = ''
    for field in _fields:
        if field in new_entry['entry']['fields']:
            _v = new_entry['entry']['fields'][field]
            if _v is not None:
                if isinstance(_v, list):
                    text = u'{0}, {1}'.format(text, ', '.join(_v))
                else:
                    text = u'{0}, {1}'.format(text, _v)

    nlp_items = dict()

    if text:
        text = text.replace('\r', '').replace('\n', '').replace('\t', '').replace('\"', '').replace('-', '')
        try:
            nlp_items = json.loads(post_to_nlp_service(text))
            for nlp_item_key in nlp_items.keys():
                nlp_text_field_name = NLP_SOLR_MAPPING[nlp_item_key]
                new_entry['entry']['fields'][nlp_text_field_name] = nlp_items[nlp_item_key]

            geo_text = " "
            for field in NLP_GEO_KEYS:
                if field in nlp_items.keys():
                    if nlp_items[field]:
                        geo_text = "{0} {1}".format(geo_text, ', '.join(nlp_items[field]))
            if not geo_text.isspace():
                new_entry['entry']['fields']['nlp_place'] = geo_text

            # logging.info('got back for item id %s', json.dumps(nlp_items, indent=4))

        except Exception as e:
            logging.error("could not get response from NLP parser. ")
            logging.error(e)


    else:
        logging.info('no text found to send to NLP in entry id %s', new_entry['entry']['fields']['id'])

    sys.stdout.write(json.dumps(new_entry))
    sys.stdout.flush()
    