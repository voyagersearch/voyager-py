import json
import sys
import xml.etree.ElementTree as ET
import logging
import datetime


import gensim
import os
import collections
import random


logging.basicConfig(filename='data_extraction.log',
                    level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

def run(entry):
    entry_json = json.load(open(entry, "rb"))

    try:
        fields = entry_json['entry']['fields']
        text = ''
        if 'text' in fields:
            text = fields['text']
        # if 'meta_data_description' in fields:
        #     text += ' ' + fields['meta_data_description']
        if 'description' in fields:
            text += ' ' + fields['description']
        if 'abstract' in fields:
            text += ' ' + fields['abstract']
        if 'purpose' in fields:
            text += ' ' + fields['purpose']


        text = text.replace('<p>', '').replace('</p>', '')
        test_corpus = gensim.models.doc2vec.LabeledSentence(gensim.utils.simple_preprocess(text), [fields['id']])
        # logging.info(test_corpus)
        count = len(test_corpus.words)
        if count > 0:
            logging.info("corpus contains %s words for doc id %s", len(test_corpus.words), fields['id'])
        fields['fss_words'] = test_corpus.words
        fields['fss_tags'] = test_corpus.tags
        sys.stdout.write(json.dumps(entry_json))
        sys.stdout.flush()

    except Exception:
        sys.stdout.flush()