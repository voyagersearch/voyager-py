# (C) Copyright 2016 Voyager Search
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
from __future__ import unicode_literals

from bottle import route, run, request
from spacy.en import English
import linguistic_features as lf
import logging
import json
import urllib
import settings
import argparse


logging.basicConfig(filename="{0}/nlp_service.log".format(settings.LOG_FILE_PATH), level=logging.DEBUG)


class NLPException(Exception):
    pass

class NLPParser(object):
    def __init__(self):
        self.english = English()

    def parse(self, text):
        result = {}
        doc = None
        try:
            text = urllib.unquote(text)
            text = text.decode('utf-8')
            doc = lf.parseText(text, self.english)

        except Exception as ee:
            logging.info('error in utf-8 decode - (will attempt latin-1 decode)')
            logging.info(ee)
            doc = lf.parseText(text.decode('latin-1'), self.english)

        if doc is not None:
            try:
                functions = [lf.tagSentences, lf.tagPOSTags, lf.tagNamedEntities, lf.tagNounChunks]
                lf.run_functions(doc, result, *functions)

                if 'named_entities' in result:
                    return json.dumps(result['named_entities'])
                else:
                    raise NLPException('No named entities found.')
            except Exception as e:
                logging.error("Error in NLP: %s" % e)
                return json.dumps({})
        else:
            logging.info('nothing returned from linguistic features parseText.')
            return json.dumps({})


_nlp = NLPParser()


@route('/nlptest/<text>', method='GET')
def nlptest(text):
    return _nlp.parse(text)


@route('/nlp', method='POST')
def nlpservice():
    postdata = request.body.read()
    return _nlp.parse(postdata)


argument_parser = argparse.ArgumentParser(description='NLP Service')
argument_parser.add_argument('-p', '--port', help='port to run on', default=settings.SERVICE_PORT)
argument_parser.add_argument('-a', '--address', help='service address', default=settings.SERVICE_ADDRESS)
arrgs = argument_parser.parse_args()

run(host=arrgs.address, port=arrgs.port)
