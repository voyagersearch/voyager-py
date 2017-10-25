from __future__ import unicode_literals
import sys
import json
import logging
import urllib2
import urllib

from steps.utils import settings, nlp_settings

logging.basicConfig(filename="{0}/train_doc2vec.log".format(settings.LOG_FILE_PATH),
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")


def post_to_gensim_service(_id, location, text, name):
    data = json.dumps({'id': _id, 'text': text, 'location': location, 'name': name})
    try:
        req = urllib2.Request("http://localhost:9998/doc2vec/add", data)
        response = urllib2.urlopen(req)
        result = response.read()
        logging.debug('sent %s chars to nlp, got back %s chars', len(text), len(result))
        return result
    except Exception as e:
        logging.error('sent %s chars to nlp, got back error: %s', len(text), e)


def run(entry, *args):
    entry_json = json.load(open(entry, "rb"))

    try:
        fields = entry_json['entry']['fields']
        text = ''
        if 'text' in fields:
            text = fields['text']
        else:
            return

        if 'name' in fields:
            text += ' ' + fields['name']
        # if 'abstract' in fields:
        #     text += ' ' + fields['abstract']
        # if 'purpose' in fields:
        #     text += ' ' + fields['purpose']


        text = text.replace('<p>', '').replace('</p>', '')
        response = post_to_gensim_service(fields['id'], fields['location'], text, fields['name'])

        logging.info(response)
        sys.stdout.write(json.dumps(entry_json))
        sys.stdout.flush()

    except Exception:
        sys.stdout.flush()
    