import os
from pyspark.sql import SparkSession

'''
Parses raw XML files from S3 bucket into a Parquet file
'''

# ===== Spark Configs =====
sc = SparkSession.builder.appName('XML Parquet Convertor').getOrCreate()
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])

def upload_to_s3(filename, destination):
	"""
	- Load scrapped xml file to Spark
	- Dump files in S3 as Parquet from Spark
	Args:
	 	filename: File to be read
		destination: S3 destination address

	"""
	try:
		# load files to spark
		df = sc.read.format('xml')\
					.options(rowTag='record')\
					.load(filename)
		# write file to Parquet
		df.write.mode('overwrite').parquet(destination)

	except Exception as ex:
		print(ex)

if __name__ == "__main__":
	fn = 's3a://jd-scrapped/dice_xml/*.xml'
	dn = 's3a://jd-parquet/jd_raw.parquet'
	upload_to_s3(fn, dn)
