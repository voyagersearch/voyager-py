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
import warnings
import re
from unidecode import unidecode

spchars = re.compile('\`|\~|\!|\@|\#|\$|\%|\^|\&|\*|\(|\)|\_|\+|\=|\\|\||\{|\[|\]|\}|\:|\;|\'|\"|\<|\,|\>|\?|\/|\-')
numbers = set(["DATE", "TIME", "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL"])
warnings.filterwarnings('ignore', '.*narrow Python.*')


def tagSentences(doc, data):
    data["sentences"] = []
    for d1 in doc.sents:
        data["sentences"].append(d1.text.strip())


def tagPOSTags(doc, data):
    data["pos"] = []
    for d1 in doc.sents:
        for p1 in d1:
            data["pos"].append((p1.text, p1.pos_))


def tagNamedEntities(doc, data):
    data["named_entities"] = {"PERSON": [], "NORP": [], "FAC": [], "ORG": [],
                                "GPE": [], "LOC": [], "PRODUCT": [],
                                "EVENT": [], "WORK_OF_ART": [], "LAW": [],
                                "LANGUAGE": []}
    for e1 in doc.ents:
        if e1.label_ not in numbers:
            entity = ""
            for word in e1:
                if word.pos_ != "DET":
                    entity = entity + " " + word.text
            if not entity.strip() in data["named_entities"][e1.label_]: 
                data["named_entities"][e1.label_].append(entity.strip())
            


def tagNounChunks(doc, data):
    data["noun_chunks"] = []
    for np in doc.noun_chunks:
        data["noun_chunks"].append(np.text.strip())


def tagWordCase(doc, data):
    return None


def formatUnicode(text):
    text1 = text.encode('utf-8', 'ignore')
    text2 = unidecode(text1)
    return text2


def parseText(text1, nlp):
    """Run the Spacy parser on the input text that is converted to unicode."""
    doc = nlp(text1)
    return doc


def run_functions(doc, data, *fns):
    for func in fns:
        func(doc, data)
