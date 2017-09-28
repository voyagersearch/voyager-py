import sys
import json
from nltk.tokenize import sent_tokenize
import nltk.data
import re
import logging
import io

logging.basicConfig(filename="D://voyager//bp_docs//sentence_tagger.log",
                    level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

class SentenceTagger(object):
    def __init__(self):
        with open('D://voyager//bp_docs//wellnames.txt') as f:
            content = f.readlines()
        self.well_names = [x.strip() for x in content]
        self.wellname_sentence_file = io.open('D://voyager//bp_docs//tagged_sentences_wellnames.txt', 'a', encoding='utf-8')
        
        with open('D://voyager//bp_docs//alternate_wellnames.txt') as f:
            content = f.readlines()
        self.alternate_well_names = [x.strip() for x in content]
        self.alt_wellname_sentence_file = io.open('D://voyager//bp_docs//tagged_sentences_alt_wellnames.txt', 'a', encoding='utf-8')
        
        with open('D://voyager//bp_docs//basins.txt') as f:
            content = f.readlines()
        self.basins = [x.strip() for x in content]
        self.basins_sentence_file = io.open('D://voyager//bp_docs//tagged_sentences_basins.txt', 'a', encoding='utf-8')

        with open('D://voyager//bp_docs//fields.txt') as f:
            content = f.readlines()
        self.fields = [x.strip() for x in content]
        self.fields_sentence_file = io.open('D://voyager//bp_docs//tagged_sentences_fields.txt', 'a', encoding='utf-8')

        # with open('D://voyager//bp_docs//blocks.txt') as f:
        #     content = f.readlines()
        # self.blocks = [x.strip() for x in content]
        # self.blocks_sentence_file = io.open('D://voyager//bp_docs//tagged_sentences_blocks.txt', 'a', encoding='utf-8')

        self.tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')


    def tag_sentences(self, line, words, tag, sentence_file):
        for word in words:
            pattern = re.compile(r'\b' + word + r'\b')
            for match in pattern.finditer(line):
                newline = line[0:match.start()] + '<{1}>{0}</{1}>'.format(word, tag) + line[match.end():]
                logging.info("found sentence " + newline)
                sentence_file.write(newline + '\n')

    def run(self, text):
        lines = sent_tokenize(text)
        #regex = re.compile(r'(\b('+ r'|'.join(self.well_names)+r')\b(\s*('+r'|'.join(self.well_names)+r')\b)*)', re.I)
        logging.info('checking lines... ')
        for line in lines:
            line = line.replace('\r', '').replace('\n', '')
            line = ' '.join(line.split())
            consequitivedots = re.compile(r'\.{2,}')
            line = consequitivedots.sub('', line)

            logging.info('tagging wellnames... ')
            self.tag_sentences(line, self.well_names, 'wellname', self.wellname_sentence_file)

            logging.info('tagging alternate wellnames... ')
            self.tag_sentences(line, self.alternate_well_names, 'alt_wellname', self.alt_wellname_sentence_file)

            logging.info('tagging fields... ')
            self.tag_sentences(line, self.fields, 'field', self.fields_sentence_file)

            logging.info('tagging basins... ')
            self.tag_sentences(line, self.basins, 'basin', self.basins_sentence_file)

            # logging.info('tagging blocks... ')
            # self.tag_sentences(line, self.blocks, 'block', self.blocks_sentence_file)

            # for wellname in self.well_names:
            #     pattern = re.compile(r'\b' + wellname + r'\b')
            #     for match in pattern.finditer(line):
            #         newline = line[0:match.start()] + '<wellname>{0}</wellname>'.format(wellname) + line[match.end():]
            #         logging.info("found sentence " + newline)
            #         self.wellname_sentence_file.write(newline + '\n')
            # for wellname in self.alternate_well_names:
            #     pattern = re.compile(r'\b' + wellname + r'\b')
            #     for match in pattern.finditer(line):
            #         newline = line[0:match.start()] + '<wellname>{0}</wellname>'.format(wellname) + line[match.end():]
            #         logging.info("found sentence " + newline)
            #         self.wellname_sentence_file.write(newline + '\n')
        self.alt_wellname_sentence_file.close()
        self.basins_sentence_file.close()
        # self.blocks_sentence_file.close()
        self.fields_sentence_file.close()
        self.wellname_sentence_file.close()


def run(entry):
    new_entry = json.load(open(entry, "rb"))
    if 'fields' in new_entry['entry']:
        if 'text' in new_entry['entry']['fields'] and 'sentences_tagged' not in new_entry['entry']['fields']:
            text_field = new_entry['entry']['fields']['text']
            SentenceTagger().run(text_field)
            new_entry['entry']['fields']['sentences_tagged'] = True

    sys.stdout.write(json.dumps(new_entry))
    sys.stdout.flush()
