from gensim import corpora, models
from gensim.models.ldamodel import LdaModel
import pymongo

# put top k word here
top_k_words = [['human', 'graph', 'computer']]

dictionary =  corpora.Dictionary(top_k_words)

print dictionary.token2id

class MyCorpus(object):
	def __iter__(self):
		# change to get document from mongodb
		for line in open('mycorpus.txt'):
			yield dictionary.doc2bow(line.lower().split())

corpus = MyCorpus()

lda = LdaModel(corpus, num_topics = 2, id2word = dictionary)

print lda.print_topics(2)



