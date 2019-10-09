from termcolor import colored
from pyspark.sql import SQLContext, SparkSession
from clean_description import clean_description
from generate_common_words import generate_frequent_words
from match_tags import assign_tags
from save_data import load_es, update_state_list, tag_count
'''
Converts scrapped job posting data into documents that can be queried from
Elasticsearch using tags.
'''

spark = SparkSession.builder.appName('Job Description Processer').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.addPyFile("clean_description.py")
spark.sparkContext.addPyFile("generate_common_words.py")
spark.sparkContext.addPyFile("match_tags.py")
spark.sparkContext.addPyFile("save_data.py")
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)


def main(fn, es_host):
    print(colored("[STARTING]: Processing", "green"))

    print(colored("[PROCESSING]: Loading Raw Data", "red"))
    jd = sqlContext.read.parquet(fn)
    jd.printSchema()

    # Step 1: Extract word lemmas from the job description field
    jd = clean_description(jd)

    # Optional Step: Find common words that appear in many of the job postings
    #   Users may choose not to run this step everytime given the word list from
    #   previous sessions can be used.

    #jd = generate_frequent_words(jd.select('stemmed'))

    # Step 2: Extract keywords and assign tags
    jd = assign_tags(jd)

    # Step 3: Load the data to Elasticsearch
    load_es(jd, es_host)

    # Step 4: update tag counts and state list
    update_state_list(jd.select("state"))
    tag_count(jd.select("tags"))

if __name__ == "__main__":
    fn = 's3a://jd-parquet/jd_raw.parquet'
    es_host = '10.0.0.13'
    main(fn, es_host)
