import sys  
reload(sys)  
sys.setdefaultencoding('utf-8')  

from gensim.models import Word2Vec
from db_util import db, word2vec_path
import re

class MyContent(object):
	def __iter__(self):
		for doc in db.warc_data_sample.find():
			yield doc

docs = MyContent()

title_pattern = re.compile('<html.*<head.*<title>(.+)</title>.*/head>.*',re.DOTALL)		
for doc in docs:
	if int(doc['content-length']) < 500:
		continue
	else:

		content =doc['content'].decode('utf-8','ignore').encode("utf-8")
		titles = title_pattern.findall(content)
		if len(titles) > 0 :
			title = titles[0]
			if len(title.strip()) > 0:
				db.title.insert_one({'title':title})