from pyspark.sql import SQLContext, SparkSession
from clean_description import clean_description
from generate_common_words import generate_frequent_words
from match_tags import assign_tags
from save_data import load_es, update_state_list, tag_count
import config as conf

'''
Converts scrapped job posting data into documents that can be queried from
Elasticsearch using tags.
'''
# ===== Spark Configs =====
spark = SparkSession.builder.appName('Job Description Processer').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.addPyFile("home/ubuntu/job-miner/src/util/config.py")
spark.sparkContext.addPyFile("home/ubuntu/job-miner/src/util/clean_description.py")
spark.sparkContext.addPyFile("home/ubuntu/job-miner/src/util/generate_common_words.py")
spark.sparkContext.addPyFile("home/ubuntu/job-miner/src/util/match_tags.py")
spark.sparkContext.addPyFile("home/ubuntu/job-miner/src/util/save_data.py")

# ===== SQL Configs =====
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

# ===== Logger Configs =====
TS = date.today().strftime('%y%m%d')
logger = logging.getLogger('jd_logger')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(conf.LOG_DIR + TS + '_batch_process.log')
fh.setLevel(logging.INFO)
logger.addHandler(fh)

def main(fn, es_host):
    logger.info('[STARTING]: Processing')
    logger.info('[PROCESSING]: Loading Raw Data')
    jd = sqlContext.read.parquet(fn)
    jd.printSchema()

    # Step 1: Remove all the duplicates
    jd = jd.dropDuplicates(['job_title', 'state', 'job_description'])

    # Step 2: Extract word lemmas from the job description field
    jd = clean_description(jd)

    # Optional Step: Find common words that appear in many of the job postings
    #   Users may choose not to run this step everytime given the word list from
    #   previous sessions can be used.

    #jd = generate_frequent_words(jd.select('stemmed'))

    # Step 3: Extract keywords and assign tags
    jd = assign_tags(jd)

    # Step 4: Load the data to Elasticsearch
    load_es(jd)

    # Step 5: update tag counts and state list
    update_state_list(jd.select("state"))
    tag_count(jd.select("tags"))

if __name__ == "__main__":
    main(conf.PARQUET_PATH, conf.ES_HOST)
