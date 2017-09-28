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

import logging
import json
import urllib
import argparse

import settings
from bottle import route, run, request, response
import linguistic_features as lf

#import en_bp_test_model_3
import en_bp_trained_model_09_22_2017_10_15

logging.basicConfig(filename="{0}/bp_nlp_service.log".format(settings.LOG_FILE_PATH),
                    level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")


                    # country -> Basin -> block -> field -> well & alt well
                    # also Bh Lithostrat Unit & Bh Age


class NLPException(Exception):
    pass

class NLPParser(object):
    def __init__(self):
        self.bp = en_bp_trained_model_09_22_2017_10_15.load()

    def parse(self, text):
        result_bp = {}
        doc = None
        try:
            text = urllib.unquote(text)
            text = text.decode('utf-8')
            doc = lf.parseText(text, self.bp)

        except Exception as ee:
            logging.info('error in utf-8 decode - (will attempt latin-1 decode)')
            logging.info(ee)
            doc = lf.parseText(text.decode('latin-1'), self.bp)

        if doc is not None:
            try:
                functions = [lf.tagNamedEntities]
                lf.run_functions(doc, result_bp, *functions)

                if 'named_entities' in result_bp:

                    return result_bp
                else:
                    raise NLPException('No named entities found.')
            except Exception as e:
                print("Error in NLP: %s" % e)
                logging.error("Error in NLP: %s" % e)
                return json.dumps({})
        else:
            logging.info('nothing returned from linguistic features parseText.')
            return json.dumps({})


_nlp = NLPParser()


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
# text = text.decode('utf-8', 'ignore')
# text = unidecode(text)

# _nlp.parse(text)

def jsonp(request, dictionary):
    if (request.query.callback):
        return "%s(%s)" % (request.query.callback, dictionary)
    return dictionary

@route('/nlptest', method='GET')
def nlptest():
    if request.query.callback:
        response.content_type = "application/javascript"
    result = _nlp.parse(request.query.text)
    return jsonp(request, result)

@route('/nlp', method='POST')
def nlpservice():
    postdata = request.body.read()
    rsp = _nlp.parse(postdata)
    return jsonp(request, json.dumps(rsp))

argument_parser = argparse.ArgumentParser(description='NLP Service')
argument_parser.add_argument('-p', '--port', help='port to run on', default=settings.SERVICE_PORT)
argument_parser.add_argument('-a', '--address', help='service address', default=settings.SERVICE_ADDRESS)
arrgs = argument_parser.parse_args()

run(debug=True, host=arrgs.address, port=arrgs.port)


