import logging, gensim, bz2
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

from pymongo import MongoClient
from collections import defaultdict
from gensim import corpora, models, similarities


client = MongoClient(host="Master.Hadoop",port=21871)
db = client.common_crawl

dictionary =  corpora.Dictionary().load("wet_dictionary_sample.dict")

dict_keys = dictionary.token2id.keys()

class MyCorpus(object):
	def __iter__(self):
		for document in db.wet_data.find():
			origin_doc = filter(lambda ch: ch.isalpha() or ch.isspace(), document['content'].lower())
			filter_doc = origin_doc.split() #[word for word in origin_doc if word in dict_keys]
			yield dictionary.doc2bow(filter_doc)

corpus_memory_friendly = MyCorpus() # doesn't load the corpus into memory!
lda = gensim.models.ldamodel.LdaModel(corpus_memory_friendly, id2word=dictionary, num_topics=10,  passes=20)
lda.save("lda_30_topics.model")
print lda.print_topics(10)

