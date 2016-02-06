# _*_ coding:utf-8 _*_
import codecs;
import string
import gensim
from pymongo import MongoClient
from gensim import corpora, models, similarities


"""###  MongoDB ###"""
"""################"""
client = MongoClient(host="Master.Hadoop",port=21871)
db = client.common_crawl

class MyCorpus(object):
	def __iter__(self):
		for document in db.wet_data_sample.find():
			yield document['content'].lower().split()

## read stop words
stop_words = set()
with open('stop-words_english_6_en.txt','r') as stop_word_file:
  for line in stop_word_file.readlines():
    stop_words.add(line.strip())

corpos = MyCorpus()

dictionary = corpora.Dictionary(corpos)


once_ids = [tokenid for tokenid, docfreq in dictionary.dfs.iteritems() if docfreq <= 2]
dictionary.filter_tokens(once_ids)# remove words that appear only once

stop_word_ids = [tid for token,tid in dictionary.token2id.iteritems() if token in stop_words]
dictionary.filter_tokens(stop_word_ids)


dictionary.compactify()

print dictionary
dictionary.save("wet_dictionary_sample.dict")

