#!/usr/bin/env python
# -*- coding: utf-8 -*-
# # (C) Copyright 2016 Voyager Search
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import json
import gensim
import settings
import argparse

from bottle import route, run, request, response

logging.basicConfig(filename="{0}/gensim_service.log".format(settings.LOG_FILE_PATH), level=logging.DEBUG)


class GensimService(object):
    def __init__(self):
        logging.info('loading model... ')
        self.word2Vec_model = gensim.models.KeyedVectors.load_word2vec_format('D:/gensim_investigation/sentences.vectors', binary=False)
        self.doc2Vec_model = gensim.models.doc2vec.Doc2Vec.load('D:/gensim_investigation/test_model.doc2vec')
        logging.info('model loaded.')

    def get_similar_words(self, text=None, positive=None, negative=None):
        result = {}
        if text:
            logging.info('parsing - %s', text)
            result['similar'] = self.word2Vec_model.most_similar(text)
        elif positive:
            logging.info('parsing - %s', positive)
            result['similar'] = self.word2Vec_model.most_similar(positive=positive, negative=negative)
        return json.dumps(result)

    def get_similar_docs(self, id=None, positive=None, negative=None):
        result = {}
        if id:
            logging.info('parsing - %s', id)
            result['similar'] = self.doc2Vec_model.docvecs.most_similar(id)
            ids = []
            for d in result['similar']:
                ids.append(d[0])
            result['ids'] = ids
            result['solr'] = "http://vector:8888/navigo/search?disp=default&q=id:(%s)" % " ".join(ids)
        elif positive:
            logging.info('parsing - %s', positive)
            result['similar'] = self.word2Vec_model.most_similar(positive=positive, negative=negative)
        return json.dumps(result)


_g = GensimService()

def jsonp(request, dictionary):
    if (request.query.callback):
        return "%s(%s)" % (request.query.callback, dictionary)
    return dictionary

@route('/word2vec/similar', method='GET')
def word2vec_similar():
    logging.info(request.query)
    if request.query.callback:
        response.content_type = "application/javascript"
    if request.query.text:
        result = _g.get_similar_words(text=request.query.text)
    if request.query.positive: 
        result = _g.get_similar_words(
            positive=request.query.positive.split(','),
            negative=request.query.negative.split(',') if request.query.negative else []
        )
    return jsonp(request, result)

@route('/doc2vec/similar', method='GET')
def doc2vec_similar():
    logging.info(request.query)
    if request.query.callback:
        response.content_type = "application/javascript"
    if request.query.id:
        result = _g.get_similar_docs(id=request.query.id)
    if request.query.positive: 
        result = _g.get_similar_docs(
            positive=request.query.positive.split(','),
            negative=request.query.negative.split(',') if request.query.negative else []
        )
    return jsonp(request, result)



@route('/word2vec/get_similar', method='POST')
def gensimservice():
    logging.info(request.query)
    postdata = request.body.read()
    return _g.parse(postdata)

argument_parser = argparse.ArgumentParser(description='gensim service')
argument_parser.add_argument('-p', '--port', help='port to run on', default=settings.SERVICE_PORT)
argument_parser.add_argument('-a', '--address', help='service address', default=settings.SERVICE_ADDRESS)
arrgs = argument_parser.parse_args()

run(host=arrgs.address, port=arrgs.port)
