import boto3
import pickle
import logging
from datetime import date
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from generate_common_words import dump_pickle
import config as conf
'''
Assign Top 500 Stackoverflow tags to job postings
'''
# ===== Logger Configs =====
TS = date.today().strftime('%y%m%d')
logger = logging.getLogger('jd_logger')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(conf.LOG_DIR + TS + '_batch_process.log')
fh.setLevel(logging.INFO)
logger.addHandler(fh)

def read_pickle(bucket, key):
    s3 = boto3.resource('s3')
    pickled = pickle.loads(s3.Bucket(bucket)\
                             .Object(key)\
                             .get()['Body']\
                             .read())
    return pickled

def filter_common_words(tokens, word_list):
    """
    Filter out words that appear in many of the documents
    Args:
        tokens: a list of word lemmas
        word_list: a list of common words
    Returns:
        a set of word lemmas with the common words removed
    """
    return list(set([word for word in tokens if word not in word_list]))

def select_tag_words(tokens, tag_list):
    """
    Match Stackoverflow tags to word lemmas
    Args:
        tokens: a list of word lemmas
        tag_list: a list of tags
    Returns:
        a list of tags for each job posting
    """
    return [tag for tag in tag_list if tag in tokens]

def assign_tags(jd):
    """
    Assign Stackoverflow tags and construct a set of keywords
    Args:
        jd: cleaned job posting dataframe
    Returns:
        a dataframe with columns containing keywords and tags
    """
    logger.info('[STARTING]: Assigning Tags')
    common_words = read_pickle(conf.PARQUET_BUCKET, conf.COMMON_WORDS_PATH)
    stack_tags = read_pickle(conf.PARQUET_BUCKET, conf.TAG_PATH)

    logger.info('[PROCESSING]: Removing Common Words')
    cw_remover = udf(lambda body: filter_common_words(body, common_words), ArrayType(StringType()))
    jd_keywords = jd.withColumn('keywords', cw_remover('stemmed'))

    logger.info('[PROCESSING]: Getting Tags')
    tagger = udf(lambda body: select_tag_words(body, stack_tags), ArrayType(StringType()))
    jd_tags = jd_keywords.withColumn('tags', tagger('keywords'))

    logger.info('[Finished]: Assigning Tags')
    return jd_tags
