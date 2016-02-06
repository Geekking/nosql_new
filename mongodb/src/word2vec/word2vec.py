from gensim.models import Word2Vec
from db_util import db, word2vec_path

class MySentence(object):
	def __iter__(self):
		for docs in db.title.find():
			title = docs['title']
			yield title.lower().split(' ')
sentences = MySentence()
model = Word2Vec(sentences,min_count=1,size=100)
model.save(word2vec_path)

## changing title to a vector

titles = MySentence()
for title in titles:
	vector = None
	for word in title:
		if vector == None:
			vector = model[word]
		else:
			vector += model[word]
	db.title_vec.insert_one({'title': ' '.join(title), 'vector': vector.tolist()})