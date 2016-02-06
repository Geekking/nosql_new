# -*- coding: utf-8 -*-
# 加入上面的一句是使的代码可以中文注释
# '#' 这个符号开头的是单行注释
# ‘’‘ 包围的是多行注释
# 引用头文件
from pyspark import SparkContext,SparkConf
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vectors
# 初始化spark 配置
conf = SparkConf()
conf.setAppName("Simple App")
conf.set("spark.executor.memory","1g")
sc = SparkContext(conf = conf)
logFile = "./log.log"  # Should be some file on your system
logData = sc.textFile(logFile).cache()

#########################################################
"""                                                  TOP 100                                           """
kFileNum = 3
total_words = None
for file_id in range(1,kFileNum):
    YOUR_FILE = "warc_data/2015/CC-MAIN-20150124161055-%05d-ip-10-180-212-252.ec2.internal.warc.gz"%file_id
    YOUR_DELIMITER = "WARC/1.0"
    text_file= sc.newAPIHadoopFile(YOUR_FILE,"org.apache.hadoop.mapreduce.lib.input.TextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text", conf = {"textinputformat.record.delimiter":YOUR_DELIMITER}).map(lambda l:l[1])
    # 打开文件, 语句的结果是一个RDD 
    def split_and_remove_no_meaning_word(line):
    	candidate_words = line.split()
    	# 去掉符号
    	candidate_words = map(lambda word: filter(str.isalpha, str(word.encode('utf8')) ), candidate_words)
    	no_meanning_words = ['was','over','her','them','news','they','what','like','now','use','how','see','add','help','when','who','there','here','back','also','most','over','make','years','had','into','have','may','any','other','more','has','one','which','out','their','some','than','its','off','only','his','just','get','been','were','would','our','ago','not','the','and','with','for','your','you','the','from','are','that','all','will','this','can','but','about','warcdate','warctype','contentlength','warcrecordid','contenttype','warcblockdigest','warctargeturi','warcrefersto']
    	words = filter(lambda word: len(word) > 2 and word not in no_meanning_words, candidate_words)
    	return words

    text_file1 = text_file.map(lambda line:line.lower()).flatMap(split_and_remove_no_meaning_word)
    words = text_file1.map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
    if total_words == None:
        total_words = words
    else:
        total_words = total_words.union(words)
num_words = 200
total_words = total_words.reduceByKey(lambda a,b: a+b)
top_word = total_words.takeOrdered(num_words, key = lambda v: -v[1])
###########################################################

###########################################################
"""                                                          get list                                     """
str_word = " ".join(str(k) for k in top_word)
length = len(str_word)
list_ref= []
temp = 0
count = 0
while(count < num_words):
    start_index = str_word.find("'",temp,length-1)
    if start_index < 0:
        break
    end_index = str_word.find("'",start_index+1,length-1)
    var1 = str_word[start_index+1:end_index]
    temp = end_index+1
    list_ref.append(var1)
    count = count+1
len_list = len(list_ref)
print list_ref
broadcast_list_ref = sc.broadcast(list_ref)
########################################################

########################################################
"""                                         get file_word                               """

def gen_vectors(line):
    list_ref = broadcast_list_ref.value
    vec = [0] * len(list_ref)
    line = filter(lambda ch: str.isalpha(ch) or str.isspace(ch), str(line.encode('utf8'))) 
    list_str = line.split(' ')
    len_word = len(list_str)
    for j in range(len_word):
        if list_str[j] in list_ref:
       	    ind = list_ref.index(list_str[j])
            vec[ind] = float(vec[ind]+1)
    return Vectors.dense(vec)

total_corpus = None
for file_id in range(1,kFileNum):
    YOUR_FILE = "data/wet/CC-MAIN-20150728002301-%05d-ip-10-236-191-2.ec2.internal.warc.wet"%file_id
    YOUR_DELIMITER = "WARC/1.0"
    text_file= sc.newAPIHadoopFile(YOUR_FILE,"org.apache.hadoop.mapreduce.lib.input.TextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text", conf = {"textinputformat.record.delimiter":YOUR_DELIMITER}).map(lambda l:l[1])
    
    file_words = text_file.map(lambda file:file.replace('\n',' '))
    current_corpus = file_words.map(gen_vectors)	
    if total_corpus == None:
	total_corpus = current_corpus
    else:
	total_corpus = total_corpus.union(current_corpus)
total_corpus = total_corpus.zipWithIndex().map(lambda x: [x[1], x[0]]).cache()

kNumTopics = 10
# Cluster the documents into three topics using LDA
ldaModel = LDA.train(total_corpus, k=kNumTopics)

# Output topics. Each is a distribution over words (matching word count vectors)
print("Learned topics (as distributions over vocab of " + str(ldaModel.vocabSize()) + " words):")
topics = ldaModel.topicsMatrix()

topic_words = []

for topic in range(kNumTopics):
    word_weight = []
    print("Topic " + str(topic) + ":")
    for word in range(0, ldaModel.vocabSize()):
        #print(" " + str(topics[word][topic]))
        word_weight.append((word,topics[word][topic]))
    sorted_word_weight = sorted(word_weight,key = lambda x:-x[1])
    print sorted_word_weight
    #print sorted_word_weight[0:19]
    list_temp = []
    for i in range(0,30):
        word_index = sorted_word_weight[i][0]
        list_temp.append(list_ref[word_index])
    str_temp = " ".join(str(k) for k in list_temp)
    print str_temp
    topic_words.append(str_temp)

sc.parallelize(topic_words).saveAsTextFile('mude/top_words_10_final')
