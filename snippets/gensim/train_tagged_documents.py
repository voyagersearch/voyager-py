#!/usr/bin/env python
# -*- coding: utf-8 -*-


import gensim
import os
import collections
import random
import logging
import sys
import json
import requests
from gensim.models.doc2vec import LabeledSentence



class LabeledLineSentence(object):
    def __init__(self, filename):
        self.filename = filename
    def __iter__(self):
        for uid, line in enumerate(open(self.filename)):
            l = json.loads(line)
            yield gensim.models.doc2vec.TaggedDocument(words=l['words'], tags=l['tags'])

if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])
    logger = logging.getLogger(program)

    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)
    logger.info("running %s" % ' '.join(sys.argv))

    rows = 5000
    start = 0
    total_ids = 1 # default to 1 so the first loop executed
    corpus = []
    iterations = 0

    model = gensim.models.doc2vec.Doc2Vec(size=50, min_count=2, iter=55, workers=4)
    max_ids = 10000

    space = " "
    i = 0

    output = open('corpus_docs.txt', 'w')
    while iterations < total_ids:
        response = requests.get('http://ec2-35-167-216-239.us-west-2.compute.amazonaws.com:8888/solr/v0/select?fl=id,fss_words,fss_tags,name&indent=on&q=fss_words:*&wt=json&rows=%s&start=%s' % (rows, start), verify=False)
        docs = json.loads(response.content)
        total_ids = docs['response']['numFound']
        for text in docs['response']['docs']:
            line = '{"words": ["'+'","'.join(text['fss_words']).encode('utf-8') + '"], "tags": ["' + '","'.join(text['fss_tags']).encode('utf-8') + '"]}\n'
            output.write(line)
            i = i + 1
            if (i % 10000 == 0):
                logging.info("Saved " + str(i) + " corpus docs")

        logger.info('getting %s ids from voyager, already retrieved %s', rows, len(corpus))

        start = start + rows
        iterations += rows
    output.close()

    model.build_vocab(LabeledLineSentence('corpus_docs.txt'))

    logger.info("building model from corpus: %s", model.corpus_count)

    model.train(LabeledLineSentence('corpus_docs.txt'), total_examples=model.corpus_count, epochs=model.iter)
    logger.info("training model: ")

    model.save('test_model.doc2vec')


    # model = gensim.models.doc2vec.Doc2Vec(size=50, min_count=2, iter=55)
    # model = gensim.models.doc2vec.Doc2Vec.load('test_model.doc2vec')
    
    logging.info("model.corpus_count: %s" % model.corpus_count)

    # model.save('test_model.doc2vec')

    doc_id = random.randint(0, model.corpus_count)
    inferred_vector = model.docvecs[doc_id]
    inferred_vector = model.infer_vector("British Prime Minister Tony Blair gave his soccer-mad offspring a lesson in clinical finishing when he scored twice in a five-a-side match in sunny Tuscany on Wednesday. Sons Euan, 13, and Nicky, 11, could only manage a goal each while their dad, playing for an Italian police force side, scored once with a header and then again with a right-foot shot. The match, recreation for the Blairs during their poolside holiday in the rolling hills of central Italy, finished as a 5-5 draw. A penalty shoot-out followed but the sides were inseparable, ending the day tied at 7-7. Blair senior played in a number 10 shirt -- the number of the British prime minister's house in Downing Street, London. Ewan, playing for the official opposition, wore the number 10 shirt of Italy striker Roberto Baggio which he was given as a gift when the Blairs arrived in Italy earlier this month. The match was staged amid tight security on an indoor pitch near the picturesque town of San Gimignano. Blair's wife Cherie and nine-year-old daughter Kathryn were among only a handful of people allowed to watch.".split(" "))
    similar = model.docvecs.most_similar([inferred_vector], topn=len(model.docvecs))

    logger.info("similar: %s", similar[0])

    # ranks = []
    # second_ranks = []
    # for doc_id in range(len(corpus)):
    #     logger.info(doc_id)
    #     inferred_vector = model.infer_vector(corpus[doc_id].words)
    #     logger.info("inferred_vector: %s", inferred_vector)
    #     sims = model.docvecs.most_similar([inferred_vector], topn=len(model.docvecs))
    #     logger.info("sims: %s", sims)
    #     rank = [docid for docid, sim in sims].index(doc_id)
    #     ranks.append(rank)
        
    #     second_ranks.append(sims[1])

    # logger.info("ranks: %s", collections.Counter(ranks))

    # logger.info("testing model: ")
    # doc_id = random.randint(0, len(corpus))
    # logger.info('Train Document ({}): «{}»\n'.format(doc_id, ' '.join(corpus[doc_id].words)))

    # logger.info('Document ({}): «{}»\n'.format(doc_id, ' '.join(corpus[doc_id].words)))
    # logger.info(u'SIMILAR/DISSIMILAR DOCS PER MODEL %s:\n' % model)
    # for label, index in [('MOST', 0), ('MEDIAN', len(sims)//2), ('LEAST', len(sims) - 1)]:
    #     logger.info(u'%s %s: «%s»\n' % (label, sims[index], ' '.join(corpus[sims[index][0]].words)))

 
 