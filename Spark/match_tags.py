import boto3
import pickle
from termcolor import colored
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from generate_common_words import dump_pickle

'''
Assign Top 500 Stackoverflow tags to job postings
'''
def read_pickle(bucket, key):
    s3 = boto3.resource('s3')
    pickled = pickle.loads(s3.Bucket(bucket).Object(key).get()['Body'].read())
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
    print(colored("[STARTING]: Assigning Tags", "green"))
    common_words = read_pickle('jd-parquet', 'assets/common_jd_words.pkl')
    stack_tags = read_pickle('jd-parquet', 'assets/stack_tags.pkl')

    print(colored("[PROCESSING]: Removing Common Words", "red"))
    cw_remover = udf(lambda body: filter_common_words(body, common_words), ArrayType(StringType()))
    jd_keywords = jd.withColumn("keywords", cw_remover("stemmed"))

    print(colored("[PROCESSING]: Getting Tags", "red"))
    tagger = udf(lambda body: select_tag_words(body, stack_tags), ArrayType(StringType()))
    jd_tags = jd_keywords.withColumn("tags", tagger('keywords'))

    print(colored("[Finished]: Assigning Tags", "green"))
    return jd_tags
