import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from gensim.models import Word2Vec
from db_util import db, word2vec_path
import math
## load all doc_vector into memory
class MyDocVector(object):
	def __iter__(self):
		for title_vec in db.title_vec.find():
			yield title_vec
doc_vecs = MyDocVector()
title_vecs = [title_vec for title_vec in doc_vecs]
key_words = ['sports' ,'art']


def cal_sim(record1, record2):
	vec_size = len(record1['vector'])
	doc_sum = 0.0
	a_norm = 0.0
	b_norm = 0.0	
	for i in range(0,vec_size):
		doc_sum += record1['vector'][i] * record2['vector'][i]
		a_norm += record1['vector'][i] * record1['vector'][i]
		b_norm += record2['vector'][i] * record2['vector'][i]
	if a_norm > 0 and b_norm > 0:
		return doc_sum /(math.sqrt(a_norm) * math.sqrt(b_norm))
	else:
		return  -111111

def top_k_sim(candidate_set, search_record, top_k = 10):
	result_ind = [(0,0)] * len(candidate_set)
	for i in range(len(candidate_set)):
		result_ind[i] = (i,cal_sim(candidate_set[i],search_record))
	result_ind = sorted(result_ind, key = lambda a:-a[1])
	result = []
	for j in range(top_k):
		result.append([candidate_set[result_ind[j][0]],result_ind[j][1]])

	return result
result_file = open('result_test.txt','w')
for keyword in key_words:

	print '.*' + keyword + '.*'
	search_result = db.title_vec.find({'title': {'$regex': '.*' + keyword + '.*'}})
	search_result = [result for result in search_result]
	if len(search_result) > 0:	
		result_file.write('aim\n')
		for doc in search_result:
			sstr = str(doc['title']).decode('utf-8','ignore').encode('utf-8') + '\n' + ','.join([str(float_val) for float_val in doc['vector']])
			result_file.write(sstr + '\n')	
		
		for search_doc in search_result:
			if len(search_doc['vector']) < 10 :
				continue	
			similar_docs = top_k_sim(title_vecs, search_doc)
			result_file.write('sim\n')	
			for doc in similar_docs:
				sstr = str(doc[0]['title']).decode('utf-8','ignore').encode('utf-8') + ' , ' + str(doc[1])

				result_file.write(sstr + '\n')	
		print '\n\n'
## changing title to a vector
