import warc
from pymongo import MongoClient
import os
client = MongoClient(host="Master.Hadoop",port=21871)

db = client.common_crawl
db.wet_data_median.drop()

warc_folder = '/data/common_crawl/Date2/'
SAMPLE = False
median_size = 30
wet_count = 0
count = 0
for c_,p_,fns in os.walk(warc_folder):
	for fn in fns:
		abs_path = warc_folder + fn
		warc_file = warc.open(abs_path)
		wet_count += 1
		print wet_count
		if wet_count > median_size:
			break
	
		for record in warc_file:
			mongo_record = dict()
			if 'warc-target-uri' not in record.header.keys():
				continue
			for header_key in  record.header.keys():
				mongo_record[header_key] = record.header[header_key]
			content = record.payload.read(3*1024*1024)
			mongo_record['content'] = content
			
			if SAMPLE:
				db.wet_data_sample.insert_one(mongo_record)
			else:
				db.wet_data_median.insert_one(mongo_record)
				
			count += 1
			if count % 1000 == 0:
				print 'processed ', count 
		if SAMPLE:
			break
	if wet_count > median_size:
		break
	
	if SAMPLE:
		break






