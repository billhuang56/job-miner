import json
import logging
from datetime import date
from pyspark.sql.functions import explode
from elasticsearch import Elasticsearch
from generate_common_words import dump_pickle
import config as conf

'''
Save data to appropriate locations
'''
# ===== Logger Configs =====
TS = date.today().strftime('%y%m%d')
logger = logging.getLogger('jd_logger')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(conf.LOG_DIR + TS + '_batch_process.log')
fh.setLevel(logging.INFO)
logger.addHandler(fh)

def load_es(jd):
    """
    Save dataframe to Elasticsearch
    Args:
        jd: dataframe
    """
    logger.info('[STARTING]: Saving to Elasticsearch')
    es = Elasticsearch([{'host': conf.ES_HOST, 'port': conf.ES_PORT}])

    if not es.ping():
        raise ValueError('Connection failed')

    logger.info('[PROCESSING]: Connection Good')
    es_write_conf = {
        'es.nodes' : conf.ES_HOST,
        'es.port' : conf.ES_PORT,
        'es.resource' : 'uniq/jobs',
        'es.input.json': 'yes',
        'es.mapping.id': 'uniq_id',
        'es.batch.size.entries': '100',
    }

    logger.info('[PROCESSING]: Converting to RDD')
    jd_rdd = jd.rdd.map(lambda item: {
        'uniq_id': item['uniq_id'],
        'job_title': item['job_title'],
        'company_name': item['company_name'],
        'state': item['state'],
        'post_date': item['post_date'],
        'job_description': item['job_description'],
        'keywords': tuple(item['keywords']) if item['keywords'] else None,
        'tags': tuple(item['tags']) if item['tags'] else None
    }).map(json.dumps)\
      .map(lambda x: ('key', x))

    logger.info('[PROCESSING]: Writing to ES')
    jd_rdd.saveAsNewAPIHadoopFile(
        path = '-',
        outputFormatClass = 'org.elasticsearch.hadoop.mr.EsOutputFormat',
        keyClass = 'org.apache.hadoop.io.NullWritable',
        valueClass = 'org.elasticsearch.hadoop.mr.LinkedMapWritable',
        conf = es_write_conf)
    logger.info('[Finished]: Saving to Elasticsearch')

def update_state_list(jd_state):
    """
    Save the list of state for the dropdown menu
    Args:
        jd_state: dataframe column
    """
    logger.info('Generate State list')
    jd_state = jd_state.distinct()
    state_list = [row['state'] for row in jd_state.collect()]
    state_list = [s for s in state_list if s]
    state_list.sort()
    dump_pickle(state_list, conf.PARQUET_BUCKET, conf.STATE_PATH)

def tag_count(jd_tags):
    """
    Save the list of tags and their frequency of appearance among job postings
    Args:
        jd_tags: dataframe
    """
    logger.info('Calculating tag counts')
    tag_count = jd_tags.withColumn("tags", explode("tags"))\
                       .groupBy('tags')\
                       .count()\
                       .sort('count', ascending=False)
    tag_count_list = [(row['tags'], row['count']) for row in tag_count.collect()]
    dump_pickle(tag_count_list, conf.PARQUET_BUCKET, conf.TAG_PATH)

def write_to_psql(df):
    """
    Save dataframe to PostgreSQL
    Args:
        jd: dataframe
    """
    connectionProperties = {
            "user": conf.PSQL_USER,
            "password": conf.PWD,
            "driver":"org.postgresql.Driver"
    }
    jdbc_host= conf.PSQL_HOST
    jdbc_db = conf.PSQL_DB 
    jdbc_port = conf.PSQL_PORT
    jdbc_url = "jdbc:postgresql://{0}:{1}/{2}".format(jdbc_host, jdbc_port, jdbc_db)

    df.write.jdbc(url = jdbc_url,
                  table = "postings",
                  properties = connectionPropenProperties,
                  mode = "overwrite")
    return
