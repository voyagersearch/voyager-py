import json
import gensim
import datetime
import time, os, stat
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shutil import copyfile
from bottle import Bottle, response, request
from threading import Timer

# import gensim
import gensim.models.doc2vec as doc2vec
from gensim.models.doc2vec import LabeledSentence

# import doc2vec_online as doc2vec
# from doc2vec_online import LabeledSentence

import logging

logging.basicConfig(filename="{0}/doc2vec_service.log".format(r'D:'),
                    level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

class LabeledLineSentence(object):
    def __init__(self, filename):
        self.filename = filename
    def __iter__(self):
        for uid, line in enumerate(open(self.filename)):
            l = json.loads(line)
            yield doc2vec.TaggedDocument(words=l['words'], tags=l['tags'])

class Doc2VecService(object):
    def __init__(self):
        
        self.max_lines = 500
        self.max_age_seconds = 60
        self.min_sentences_to_train_vocab =  5000

        self.data_dir = r'D:\voyager_data\doc2vec_training'
        self.timer = RepeatedTimer(20, self.train_model)
        self.trained_model = None
        self.path_to_trained_model = None
        for _f in os.listdir(self.data_dir):
            if _f.endswith(".doc2vec"):
                self.path_to_trained_model = os.path.join(os.sep, self.data_dir, _f)
                break
        if not self.path_to_trained_model:
            self.training_model = doc2vec.Doc2Vec(size=50, min_count=2, iter=55, workers=1, hs=0)
            # self.training_model = doc2vec.Doc2Vec(min_count=1, workers=1, hs=0)
        else:
            self.trained_model = doc2vec.Doc2Vec.load(self.path_to_trained_model)
            self.training_model = doc2vec.Doc2Vec.load(self.path_to_trained_model)
        
        # print self.trained_model

    def train_model(self):
        logging.info('train model called')
        # if the trained model doesn't exist, we need to create it. 
        files_to_parse = []
        for subdir, dirs, files in os.walk(self.data_dir):
            for _f in files:
                print _f
                if 'tagged_sentences.txt' in _f:
                    self.check_and_create_file(os.path.join(os.sep, subdir, _f))
                elif _f.startswith('tagged_sentences_'):
                    files_to_parse.append(os.path.join(os.sep, subdir, _f))
        
        logging.info('files to parse: %s', files_to_parse)
        if files_to_parse:
            self.timer.stop()
            # if the model DOES NOT exist, we want to wait until theres a HUGE amount of data to train the vocab with. 
            # we can train the model with more sentences, but we CANNOT update the vocab later. 
            # need to ensure there's a large enough vocab before creating the initial model.
            if not self.trained_model:
                logging.info('model does not exist.  should i create it? ')
                # get the line count of all the parsed files
                total_lines = 0
                for _file in files_to_parse:
                    total_lines += self.file_len(_file)

                logging.info('total lines: %s '  % total_lines)
                
                # is it enough?
                if total_lines >= self.min_sentences_to_train_vocab:
                    logging.info("yep!")
                    self.timer.stop()
                    # combine all the files into one... 
                    combined_file = self.combine_files(files_to_parse, 'combined_sentences_%s.txt' % time.time())
                    logging.info("combined files... %s" % combined_file)
                    # pass it to the model to build the vocab
                    logging.info("building vocab...")
                    self.training_model.build_vocab(LabeledLineSentence(combined_file))
                    # train the model 
                    logging.info("training model...")
                    self.training_model.train(LabeledLineSentence(combined_file), total_examples=self.file_len(combined_file), epochs=self.training_model.iter)
                    model_name = "model_%s.doc2vec" % time.time()
                    self.save_and_reload_model(model_name)
                    self.cleanup_after_training(files_to_parse, model_name)

            else: # the model exists, just train it some more.
                logging.info("model exists, just updating with some new training material...")
                combined_file = self.combine_files(files_to_parse, 'combined_sentences_%s.txt' % time.time())
                self.training_model.train(LabeledLineSentence(combined_file), total_examples=self.file_len(combined_file), epochs=self.training_model.iter)
                model_name = "model_%s.doc2vec" % time.time()
                self.save_and_reload_model(model_name)
                self.cleanup_after_training(files_to_parse, model_name)
            self.timer.start()

                
    def save_and_reload_model(self, model_name):
        new_model_file = os.path.join(os.sep, self.data_dir, model_name)
        logging.info("saving new model as %s " % new_model_file )
        self.training_model.save(new_model_file)
        logging.info("reloading training model ")
        self.training_model_model = doc2vec.Doc2Vec.load(new_model_file)
        logging.info("reloading trained model ")
        self.trained_model = doc2vec.Doc2Vec.load(new_model_file)
        self.path_to_trained_model = new_model_file

    def cleanup_after_training(self, files_to_parse, model_name):
        # clean up the combined sentences file
        old_combined_files = [ f for f in os.listdir(self.data_dir) if "combined_sentences" in f]
        logging.info("removing old combined files %s " % old_combined_files)
        for _f in old_combined_files:
            os.remove(os.path.join(os.sep, self.data_dir, _f))

        # clean up all the old models
        old_models = [ f for f in os.listdir(self.data_dir) if f.endswith(".doc2vec") and model_name not in f ]
        logging.info("removing old models %s " % old_models)
        for _f in old_models:
            os.remove(os.path.join(os.sep, self.data_dir, _f))

        # clean up the sentences files 
        logging.info("removed tagged sentence files %s " % files_to_parse)
        for _f in files_to_parse:
            os.remove(_f)


    def combine_files(self, files, filename):
        combined_file = os.path.join(os.sep, self.data_dir, filename)
        with open(combined_file, 'w+') as outfile:
            for _file in files:
                with open(_file) as infile:
                    outfile.write(infile.read())
        return combined_file

    def file_len(self, _file):
        if not os.path.isfile(_file):
            return 0 
        with open(_file) as f:
            for i, l in enumerate(f):
                pass
            return i + 1

    def file_age_in_seconds(self, _file):
        if not os.path.isfile(_file):
            return 0 
        return time.time() - os.stat(_file)[stat.ST_MTIME]

    def check_and_create_file(self, _file):

        current_lines = self.file_len(_file)
        file_age = self.file_age_in_seconds(_file)
        
        if current_lines > self.max_lines or file_age > self.max_age_seconds:
            print "moving file"
            final_files_path = os.path.join(os.sep, os.path.dirname(os.path.abspath(_file)), 'finalised')
            print "target directory: %s" % final_files_path

            try:
                os.makedirs(final_files_path)
            except OSError:
                if not os.path.isdir(final_files_path):
                    raise

            target_path = os.path.join(os.sep, final_files_path, 'tagged_sentences_%s.txt' % time.time())
            print "target path: %s" % target_path
            copyfile(_file, target_path)
            print "copied new file."
            os.remove(_file)
            print "removed old file."

    def add_document(self, doc_id, text, location_id):
        location_data_dir = os.path.join(os.sep, self.data_dir, location_id)
        try:
            os.makedirs(location_data_dir)
        except OSError:
            if not os.path.isdir(location_data_dir):
                raise

        _file  = os.path.join(os.sep, location_data_dir, 'tagged_sentences.txt')

        self.check_and_create_file(_file)

        test_corpus = doc2vec.TaggedDocument(gensim.utils.simple_preprocess(text), [doc_id])
        with open(_file, 'a+') as corpus_docs:
            line = '{"words": ["' + '","'.join(test_corpus.words).encode('utf-8') + '"], "tags": ["' + '","'.join(test_corpus.tags).encode('utf-8') + '"]}\n'
            corpus_docs.write(line)

    def get_similar_docs(self, doc_id=None, positive=None, negative=None):
        result = {}
        if self.trained_model is None:
            result['error'] = "no trained model has been loaded. "
        elif doc_id:
            print 'parsing - %s' % doc_id
            result['similar'] = self.trained_model.docvecs.most_similar(doc_id)
            ids = []
            for d in result['similar']:
                ids.append(d[0])
            result['ids'] = ids
            result['solr'] = "http://localhost:9000/search?disp=default&q=id:(%s)" % " ".join(ids)
        elif positive:
            print 'parsing - %s' % positive
            result['similar'] = self.trained_model.most_similar(positive=positive, negative=negative)
        return json.dumps(result)


_d2v = Doc2VecService()

# _d2v.train_model()

route_prefix = 'doc2vec'
service = Bottle()

def jsonp(request, dictionary):
    if (request.query.callback):
        return "%s(%s)" % (request.query.callback, dictionary)
    return dictionary

@service.route('/' + route_prefix + '/add', method='POST')
def create():
    response.content_type = 'application/json'
    postdata = json.load(request.body)
    _id = postdata['id']
    _text = postdata['text']
    _location_id = postdata['location']
    _d2v.add_document(_id, _text, _location_id)

@service.route('/'+ route_prefix +'/similar', method='GET')
def doc2vec_similar():
    if request.query.callback:
        response.content_type = "application/javascript"
    if request.query.id:
        result = _d2v.get_similar_docs(doc_id=request.query.id)
    if request.query.positive: 
        result = _d2v.get_similar_docs(
            positive=request.query.positive.split(','),
            negative=request.query.negative.split(',') if request.query.negative else []
        )
    return jsonp(request, result)