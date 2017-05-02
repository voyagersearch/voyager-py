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
import codecs
import logging
import os
import sys
import argparse

import settings
import ujson as json
from spacy.en import English
import linguistic_features as lf


sys.setrecursionlimit(10000)

logging.basicConfig(filename="{0}/nlp_service.log".format(settings.LOG_FILE_PATH), level=logging.DEBUG)


class NLPException(Exception):
    pass


def handle_result(result, name, output_dir):
    """Write the JSON output to a file in specified directory."""
    with open(os.path.join(output_dir, os.path.basename(name) + ".json"), 'w') as f:
        f.write(json.dumps(result))


def read_files(dir_path):
    """Read file in the input directory and yield the text."""
    for file_pth in map(lambda x: os.path.join(dir_path, x), os.listdir(dir_path)):
        if os.path.isfile(file_pth):
            with open(file_pth, 'r', encoding='utf-8') as f:
                yield (file_pth, f.read())


def run_nlp(input_text='', input_file='', input_directory='', output_directory='', output_json=True):
    """Extracts named entities (place names, etc...) from text through the use of natural language processing (NLP)l
    :param input_text: input text to be processed -- optional
    :param input_file: input file to be processed -- optional
    :param input_directory: a directory of files to be processed
    :param output_directory: an output directory where the output results are written to a JSON file -- optional
    :param output_json: specify to output json result or not
    """

    logging.info("recieved input text: %s " % input_text)

    # Load the English model.
    nlp = English()

    source = None
    if input_text:
        source = [('stdin', input_text)]

    if not source:
        file_path = input_file
        with codecs.open(file_path, 'r', 'utf-8') as f:
            source = [(file_path, f.read())]

    if not source:
        if input_directory:
            source = read_files(input_directory)

    if not source:
        source = ['stdin', sys.stdin.read()]

    if source:
        for name, text in source:
            result = {}
            try:
                doc = lf.parseText(text, nlp)
            except (UnicodeError, TypeError):
                try:
                    doc = lf.parseText(text.decode('utf-8'), nlp)
                except UnicodeDecodeError:
                    doc = lf.parseText(text.decode('latin-1'), nlp)

            # The list of functions to run.
            functions = [lf.tagSentences, lf.tagPOSTags, lf.tagNamedEntities, lf.tagNounChunks]
            lf.run_functions(doc, result, *functions)

            # Write results to a file.
            if 'named_entities' in result:
                if output_directory:
                    handle_result(result['named_entities'], name, output_directory)
                else:
                    if output_json is True:
                        return json.dumps(result['named_entities'])
                    else:
                        return result['named_entities']
            else:
                raise NLPException('No named entities found.')
    else:
        raise NLPException('No text to process.')


if __name__ == '__main__':
    # Set up arguments - can be run standalone or via the command line.
    argument_parser = argparse.ArgumentParser(description='NLP Runner')
    argument_parser.add_argument('-f', '--file', help='single file to process', default='')
    argument_parser.add_argument('-d', '--directory', help='directory of files to process', default='')
    argument_parser.add_argument('-o', '--output', help='directory to store results', default='')
    argument_parser.add_argument('-t', '--text', help='input text', default='')
    arguments = argument_parser.parse_args()
    logging.info(arguments)
    run_nlp(arguments.text, arguments.file, arguments.directory, arguments.output)
