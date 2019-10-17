import boto3
import pickle
from termcolor import colored
from pyspark.sql.functions import monotonically_increasing_id, explode, countDistinct

'''
Generate a list of words that appear in 40% of the documents
'''
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
    print(colored("[Starting]: Generate Common Words", "red"))
    print(colored("[PROCESSING]: Constructing ID", "red"))
    jd_wid = jd.withColumn("id", monotonically_increasing_id())

    print(colored("[PROCESSING]: Explode", "red"))
    jd_exploded = jd_wid.withColumn("token", explode("stemmed"))
    doc_freq = jd_exploded.groupBy("token").agg(countDistinct("id").alias("df"))

    print(colored("[PROCESSING]: Assemble Word List", "red"))
    freq_words = doc_freq.filter(doc_freq['df'] > frequency_cutoff(jd, cutoff))
    word_list = freq_words.select('token').rdd.map(lambda row : row[0]).collect()

    print(colored("[Finished]: Generate Common Words", "green"))
    dump_pickle('s3a://jd-parquet/jd_stemmed.parquet',
        'jd-parquet', 'assets/common_jd_words.pkl')
