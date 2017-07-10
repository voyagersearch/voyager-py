import json
from wikipedia import search, page
import gensim


class Server(object):
    def __init__(self):

        # title = 'machine learning'
        # results = search(title)

        # print json.dumps(results, indent=4)

        # sentences = []

        # for res in results:
        #     p = page(res)
        #     sentences.extend(p.content.replace('\n', '').split('.'))

        # print json.dumps(sentences, indent=4)
        # self.model = gensim.models.word2vec.Word2Vec(sentences)
        # self.model.save('foo')
        # self.model = gensim.models.word2vec.Word2Vec.load('foo')

        self.model = gensim.models.KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin.gz', binary=True)
        result = self.model.most_similar('dog')
        print json.dumps(result, indent=4)
Server()
