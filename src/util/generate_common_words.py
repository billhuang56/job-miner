import boto3
import pickle
import logging
from datetime import date
from termcolor import colored
from pyspark.sql.functions import monotonically_increasing_id, explode, countDistinct
import config as conf
'''
Generate a list of words that appear in 40% of the documents
'''
# ===== Logger Configs =====
TS = date.today().strftime('%y%m%d')
logger = logging.getLogger('jd_logger')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(conf.LOG_DIR + TS + '_batch_process.log')
fh.setLevel(logging.INFO)
logger.addHandler(fh)

def frequency_cutoff(file, frequency):
    """
    Calculate the number of documents needed for a document frequency threshold
    Args:
        file: documents or job descriptions
        frequency: frequency threshold
    Returns:
        the number of documents
    """
    return int(round(file.count()*frequency))

def dump_pickle(fn, bucket, key):
    """
    Save the pickle file
    Args:
        fn: file to be saved
        bucket: S3 bucket name
        key: S3 path
    """
    pickle_byte_obj = pickle.dumps(fn)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=pickle_byte_obj)

def generate_frequent_words(jd, cutoff=0.4):
    """
    Generate a list of words that appear in 40% of the documents or at other levels
    Args:
        jd: dataframe with job description stemmed
        cutoff: user specified document frequency threshold
    """
    logger.info('[Starting]: Generate Common Words')
    logger.info('[PROCESSING]: Constructing ID')
    jd_wid = jd.withColumn('id', monotonically_increasing_id())

    logger.info('[PROCESSING]: Explode')
    jd_exploded = jd_wid.withColumn('token', explode('stemmed'))
    doc_freq = jd_exploded.groupBy('token').agg(countDistinct('id').alias('df'))

    logger.info('[PROCESSING]: Assemble Word List')
    freq_words = doc_freq.filter(doc_freq['df'] > frequency_cutoff(jd, cutoff))
    word_list = freq_words.select('token').rdd.map(lambda row : row[0]).collect()

    logger.info('[Finished]: Generate Common Words')
    dump_pickle(word_list, conf.PARQUET_BUCKET, 'assets/common_jd_words.pkl')
