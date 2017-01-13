import sys
import json
import logging
import urllib2
import urllib

from utils import settings

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

NLP_FIELDS = ['description', 'text']
NLP_GEO_KEYS = ['GPE', 'LOC', 'FAC']

logging.basicConfig(filename="{0}/nlp_worker.log".format(settings.LOG_FILE_PATH), level=logging.DEBUG)


def post_to_nlp_service(text):
    text = unicode(text).encode('utf-8')
    text = urllib.quote(text)
    try:
        req = urllib2.Request("http://{0}:{1}/nlp".format(settings.SERVICE_ADDRESS, settings.SERVICE_PORT), text.encode('utf-8'))
        response = urllib2.urlopen(req)
        result = response.read()
        # logging.debug("\n\n--> sent to NLP %s \n\nGOT BACK: %s" % (text, result))
        return result
    except Exception as e:
        logging.error("NLP error. Sent {0}, error: \n {1}".format(text, e))


def run(entry, *args):
    if args is not None:
        NLP_FIELDS = list(args)

    new_entry = json.load(open(entry, "rb"))

    text = ''
    for field in NLP_FIELDS:
        if field in new_entry['entry']['fields'].keys():
            v = new_entry['entry']['fields'][field]
            if v is not None:
                if isinstance(v, list):
                    text = u'{0}, {1}'.format(text, ', '.join(v))
                else:
                    text = u'{0}, {1}'.format(text, v)

    if len(text) > 0:
        nlp_items = json.loads(post_to_nlp_service(text))
    else:
        nlp_items = dict()

    try:
        for nlp_item_key in nlp_items.keys():
            nlp_text_field_name = "fss_NLP_{0}".format(nlp_item_key)
            new_entry['entry']['fields'][nlp_text_field_name] = nlp_items[nlp_item_key]

        geo_text = ""
        for field in NLP_GEO_KEYS:
            if field in nlp_items.keys():
                v = nlp_items[field]
                geo_text = "{0} {1}".format(geo_text, ' '.join(v))

        new_entry['entry']['fields']['ft_NLP_Geo'] = geo_text

    except Exception as e:
        logging.error("could not get response from NLP parser. ")
        logging.error(e)

    sys.stdout.write(json.dumps(new_entry))
    sys.stdout.flush()
