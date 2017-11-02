"""
service which handles adding documents to the doc2vec index and keeping the model up to date with new documents.

"""
import json
import gensim
import datetime
import time
import os
import stat
import sys
import urllib2
import urllib
import urlparse
import uuid
import logging
import base64
import multiprocessing

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shutil import copyfile
from bottle import Bottle, response, request
from threading import Timer

# import gensim
import gensim.models.doc2vec as doc2vec
from gensim.models.doc2vec import LabeledSentence

if 'VOYAGER_LOGS_DIR' in os.environ:
    logging_path = os.path.normpath(os.environ['VOYAGER_LOGS_DIR'])
else:
    logging_path = r'D:'


logging.basicConfig(filename=os.path.join(os.sep, logging_path, 'doc2vec-service.log'),
                    level=logging.INFO,
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
        self.status     = None
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.status = 'running'
            self.is_running = True

    def stop(self, current_status):
        self.status = current_status
        self._timer.cancel()
        self.is_running = False


class Doc2VecService(object):
    def __init__(self):
        logging.info(json.dumps(["%s=%s" % (k, v) for k, v in os.environ.items()], indent=4))
        # minimum items to train the vocab
        # vocab training happens only once when the model is initially created,
        # then the tagged documents are iteratively added to the model
        self.min_sentences_to_train_vocab = 25000
        self.max_ids_to_train_in_one_shot = 100
        self.run_new_docs_check_seconds = 90
        self.path_to_trained_model  = None

        if 'VOYAGER_BASE_URL' in os.environ:
            self.solr_url = urlparse.urljoin(os.environ['VOYAGER_BASE_URL'], 'solr/doc2vec')
        else:
            logging.warn('VOYAGER_BASE_URL not in os.environ, defaulting to localhost')
            self.solr_url = 'http://localhost:8888/solr/doc2vec'

        if 'VOYAGER_DATA_DIR' in os.environ:
            self.data_dir = os.path.join(os.path.sep, os.environ["VOYAGER_DATA_DIR"], 'doc2vec_training')
        else:
            logging.warn('VOYAGER_DATA_DIR not in os.environ, defaulting to current directory')
            self.data_dir = 'doc2vec_training'

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        self.timer = RepeatedTimer(self.run_new_docs_check_seconds, self.train_model_from_solr)

        # load the model - there should be only one or zero. safe to load the first one encountered.
        for _f in os.listdir(self.data_dir):
            if _f.endswith(".doc2vec"):
                self.path_to_trained_model = os.path.join(os.sep, self.data_dir, _f)
                break
        if not self.path_to_trained_model:
            # no model available, have to create one for training. 
            self.training_model = doc2vec.Doc2Vec(
                size=300,
                min_count=2,
                iter=55,
                workers=multiprocessing.cpu_count())
        else:
            self.training_model = doc2vec.Doc2Vec.load(self.path_to_trained_model)

    def get_docs_to_train(self, _fq, _q='*:*', return_tagged_docs=False):
        rows = 10000 # rows per page
        start = 0
        ids = []
        total_ids = 1
        tagged_docs = []

        while len(ids) < total_ids and len(ids) < self.max_ids_to_train_in_one_shot:
            logging.info('total ids: %s collected ids: %s', total_ids, len(ids))
            query = 'q=%s&fq=%s&wt=json&rows=%s&start=%s' % (_q, _fq, rows, start)
            url = '%s/select?%s' % (self.solr_url, query)
            logging.debug(url)
            result = self.send_request(url)
            docs = json.loads(result)
            total_ids = docs['response']['numFound']
            for item in docs['response']['docs']:
                if 'fss_words' in item:
                    if return_tagged_docs:
                        tagged_docs.append(doc2vec.TaggedDocument(words=item['fss_words'], tags=[item['id']]))
                    ids.append(item['id'])
            start = start + rows
        if return_tagged_docs:
            return tagged_docs, ids
        else:
            return ids

    def send_request(self, url, data=None):
        logging.debug('sending to %s', url)
        if data:
            req = urllib2.Request(url, data=json.dumps(data))
        else:
            req = urllib2.Request(url)

        base64string = base64.encodestring('%s:%s' % ('admin', 'admin')).replace('\n', '')
        req.add_header("Authorization", "Basic %s" % base64string)
        req.add_header('Content-type', 'application/json')
        rsp = urllib2.urlopen(req)
        result = rsp.read()
        return result

    def get_status(self):
        return {
            'training_timer_running': self.timer.is_running,
            'training_timer_status': self.timer.status,
            'check_index_seconds': self.run_new_docs_check_seconds
        }

    def stop_timer(self):
        self.timer.stop('stopped by user')
        return self.get_status()

    def start_timer(self):
        self.timer.start()
        return self.get_status()

    def check_index(self):
        query = 'q=*:*&rows=0&wt=json&json.facet={added_to_model:{type:query,query:"fb_added_to_model:true"},not_added_to_model:{type:query,query:"fb_added_to_model:false"}}'
        url = '%s/select?%s' % (self.solr_url, query)
        result = self.send_request(url)
        return result

    def clear_index(self, query):
        query = 'commit=true&stream.body=<delete><query>%s</query></delete>&wt=json' % query
        url = '%s/update?%s' % (self.solr_url, query)
        logging.info(url)
        result = self.send_request(url)
        return result

    def check_id_exists(self, doc_id):
        query = 'q=id:%s&rows=0&wt=json&_=%s"}}' % (doc_id, time.time())
        url = '%s/select?%s' % (self.solr_url, query)
        result = json.loads(self.send_request(url))
        return result['response']['numFound'] is not 0

    def update_trained_docs(self, doc_ids, added_to_model=True):
        # update the documents as 'trained' in batches of 100
        chunks = [doc_ids[x: x + 100] for x in xrange(0, len(doc_ids), 100)]
        for chunk in chunks: 
            logging.info('updating %s ids', len(chunk))
            docs = []
            for _id in chunk:
                docs.append({
                    'id': _id,
                    'fb_added_to_model': { 'set': added_to_model }
                })
            url = "{0}/update?_={1}&commitWithin=1000&wt=json".format(self.solr_url, time.time())
            result = self.send_request(url, data=docs)
            logging.debug(json.dumps(result))

    def reset_trained_docs(self, query):

        self.timer.stop('resetting trained ids - %s' % query)
        returned_ids = ['foo']
        while returned_ids:
            returned_ids = self.get_docs_to_train('fb_added_to_model:true', query, return_tagged_docs=False)
            self.update_trained_docs(returned_ids, added_to_model=False)
        self.timer.start()

    def post_to_solr(self, words, doc_id, location_id, title=None):
        try:
            doc = {
                'id': doc_id,
                'name': title,
                'fs_location_id': location_id,
                'fss_words': words,
                'fb_added_to_model': False
            }
            url = "{0}/update/json/docs?_={1}&commitWithin=1000&overwrite=true&wt=json".format(self.solr_url, time.time())
            result = self.send_request(url, data=doc)
            logging.info(json.dumps(result))
        except Exception as e:
            logging.exception(e)

    def train_model_from_solr(self):
        self.timer.stop('training model')
        try:
            _resp = json.loads(self.check_index())
            
            if 'not_added_to_model' in _resp['facets']:
                not_added_to_model = int(_resp['facets']['not_added_to_model']['count'])
            else:
                not_added_to_model = 0

            if 'added_to_model' in _resp['facets']:
                added_to_model = int(_resp['facets']['added_to_model']['count'])
            else:
                added_to_model = 0

            logging.info('added: %s not added: %s', added_to_model, not_added_to_model)

            if not_added_to_model > 0:
                if not self.path_to_trained_model:
                    if not_added_to_model >= self.min_sentences_to_train_vocab:
                        model_name = "model_%s.doc2vec" % time.time()
                        vocab_items, returned_ids = self.get_docs_to_train('fb_added_to_model:false', return_tagged_docs=True)
                        self.training_model.build_vocab(vocab_items)
                        logging.info('trained vocab on %s items', len(vocab_items))
                        self.save_and_reload_model(model_name)
                        self.cleanup_after_training(model_name)
                    else:
                        logging.info('not enough data to train vocab, waiting... ')
                else:
                    logging.info("model exists, just updating with some new training material...")
                    model_name = "model_%s.doc2vec" % time.time()
                    vocab_items, returned_ids = self.get_docs_to_train('fb_added_to_model:false', return_tagged_docs=True)
                    self.training_model.train(vocab_items, total_examples=len(vocab_items), epochs=self.training_model.iter)
                    self.update_trained_docs(returned_ids)
                    self.save_and_reload_model(model_name)
                    self.cleanup_after_training(model_name)
            else:
                logging.info('index up to date, nothing to process. ')
        except Exception as e:
            logging.exception(e)

        self.timer.start()

    def save_and_reload_model(self, model_name):
        new_model_file = os.path.join(os.sep, self.data_dir, model_name)
        logging.info("saving new model as %s " % new_model_file )
        self.training_model.save(new_model_file)
        logging.info("reloading training model ")
        self.training_model = doc2vec.Doc2Vec.load(new_model_file)
        self.path_to_trained_model = new_model_file

    def cleanup_after_training(self, model_name):
        # clean up all the old models
        old_models = [ f for f in os.listdir(self.data_dir) if '.doc2vec' in f and model_name not in f ]
        logging.info("removing old models %s " % old_models)
        for _f in old_models:
            os.remove(os.path.join(os.sep, self.data_dir, _f))

    def add_document(self, doc_id, text, location_id, name):
        if not self.check_id_exists(doc_id):
            test_corpus = doc2vec.TaggedDocument(gensim.utils.simple_preprocess(text), [doc_id])
            self.post_to_solr(test_corpus.words, doc_id, location_id, name)
        else:
            logging.info('id exists, skipping. ')

    def get_similar_docs(self, doc_id=None, positive=None, negative=None):
        result = {}
        if self.training_model is None:
            result['error'] = "no trained model has been loaded. "
        elif doc_id:
            logging.info('parsing - %s', doc_id)
            try:
                result['similar'] = self.training_model.docvecs.most_similar(doc_id)
                ids = []
                for d in result['similar']:
                    ids.append(d[0])
                result['ids'] = ids
                result['solr'] = "%s/navigo/search?disp=default&q=id:(%s)" % (os.environ["VOYAGER_BASE_URL"], " ".join(ids))
            except Exception as e:
                logging.exception(e)
        elif positive:
            logging.info('parsing - %s', positive)
            result['similar'] = self.training_model.most_similar(positive=positive, negative=negative)
        return json.dumps(result)


_d2v = Doc2VecService()

# _d2v.train_model()

route_prefix = 'doc2vec'
service = Bottle()

def jsonp(request, dictionary):
    if request.query.callback:
        return "%s(%s)" % (request.query.callback, dictionary)
    return dictionary

@service.route('/' + route_prefix + '/status', method='GET')
def check_status():
    response.content_type = 'application/json'
    return jsonp(request, _d2v.get_status())

@service.route('/' + route_prefix + '/stop_timer', method='GET')
def stop_timer():
    response.content_type = 'application/json'
    return jsonp(request, _d2v.stop_timer())

@service.route('/' + route_prefix + '/start_timer', method='GET')
def start_timer():
    response.content_type = 'application/json'
    return jsonp(request, _d2v.stop_timer())

@service.route('/' + route_prefix + '/check_index', method='GET')
def check_index():
    response.content_type = 'application/json'
    return jsonp(request, _d2v.check_index())

@service.route('/' + route_prefix + '/check_id_exists', method='GET')
def check_id_exists():
    response.content_type = 'application/json'
    if request.query.id:
        return jsonp(request, {'exists': _d2v.check_id_exists(request.query.id)})
    else:
        return jsonp(request, {'error': 'you must supply an id parameter'})

@service.route('/' + route_prefix + '/reset_trained_docs', method='GET')
def reset_trained_docs():
    response.content_type = 'application/json'
    if request.query.query:
        return jsonp(request, _d2v.reset_trained_docs(request.query.query))
    else:
        return jsonp(request, {'error': 'you must supply a query - *:* resets everything. '})

@service.route('/' + route_prefix + '/clear_index', method='POST')
def clear_index():
    response.content_type = 'application/json'
    if request.query.query:
        return jsonp(request, _d2v.clear_index(request.query.query))
    else:
        return jsonp(request, {'error': 'you must supply a query - *:* clears everything. '})

@service.route('/' + route_prefix + '/add', method='POST')
def create():
    response.content_type = 'application/json'
    postdata = json.load(request.body)
    _id = postdata['id']
    _text = postdata['text']
    _location_id = postdata['location']
    _name = postdata['name']
    _d2v.add_document(_id, _text, _location_id, _name)

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
