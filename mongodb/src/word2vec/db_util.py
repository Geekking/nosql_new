import pymongo
from pymongo import MongoClient

word2vec_path='word2vec.model'
HOST='Master.Hadoop'
PORT = 21871


client = MongoClient(HOST,PORT)

db = client['common_crawl']



