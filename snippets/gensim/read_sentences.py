#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function


import logging
import six
import sys
import os

from gensim.corpora.wikicorpus import WikiCorpus
from gensim.models.word2vec import LineSentence
from gensim.models.word2vec import Word2Vec


if __name__ ==  '__main__':

    program = os.path.basename(sys.argv[0])
    logger = logging.getLogger(program)

    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s', filename="train_wikipedia.log")
    logging.root.setLevel(level=logging.INFO)
    logger.info("running %s" % ' '.join(sys.argv))

    logging.info('corpus-ing archive...')
    wiki = WikiCorpus('enwiki-latest-pages-articles.xml.bz2', 
                    lemmatize=False, dictionary={})
    logging.info('sentencing archive...')
    space = " "
    i = 0
    output = open('sentences.txt', 'w')
    for text in wiki.get_texts():
        if six.PY3:
            output.write(u' '.join(text).decode('utf-8') + '\n')
        #   ###another method###
        #    output.write(
        #            space.join(map(lambda x:x.decode("utf-8"), text)) + '\n')
        else:
            output.write(space.join(text).encode('utf-8') + "\n")
        i = i + 1
        if (i % 10000 == 0):
            logging.info("Saved " + str(i) + " articles")

    output.close()
    logging.info("Finished Saved " + str(i) + " articles")


    # sentences = list(wiki.get_texts())
    # params = {'size': 200, 'window': 5, 'min_count': 5, 
    #         'workers': 4, 'sample': 1E-3,}
    # logging.info('word2vec-ing sentences...')
    # word2vec = Word2Vec(sentences, **params)
    # logging.info('saving...')
    # word2vec.save('enwiki-latest-pages-articles.mm')
    # logging.info('done!')