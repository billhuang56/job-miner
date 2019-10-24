import os
import logging
from datetime import date
from pyspark.sql import SparkSession
import util.config as conf
'''
Parses raw XML files from S3 bucket into a Parquet file
'''

# ===== Spark Configs =====
spark = SparkSession.builder.appName('XML Parquet Convertor').getOrCreate()
spark.sparkContext.addPyFile('home/ubuntu/job-miner/src/util/config.py')
spark._jsc.hadoopConfiguration().set('fs.s3a.access.key', conf.AWS_ACCESS_KEY_ID)
spark._jsc.hadoopConfiguration().set('fs.s3a.secret.key', conf.AWS_SECRET_ACCESS_KEY)

# ===== Logger Configs =====
TS = date.today().strftime('%y%m%d')
logger = logging.getLogger('jd_logger')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(conf.LOG_DIR  + TS + '_batch_process.log')
fh.setLevel(logging.INFO)
logger.addHandler(fh)

def xml_to_parquet(filename, destination):
	"""
	- Load scrapped xml file to Spark
	- Dump files in S3 as Parquet from Spark
	Args:
	 	filename: File to be read
		destination: S3 destination address

	"""
	# load files to spark
	df = spark.read.format('xml')\
				   .options(rowTag='record')\
		           .load(filename)
	# write file to Parquet
	df.write.mode('overwrite')\
			.parquet(destination)

	logger.info('[Finished]: Converting XML to Parquet')

if __name__ == '__main__':
	xml_to_parquet(conf.XML_PATH, conf.PARQUET_PATH)
