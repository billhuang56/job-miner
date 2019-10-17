import json
from termcolor import colored
from pyspark.sql.functions import explode
from elasticsearch import Elasticsearch
from generate_common_words import dump_pickle

'''
Save data to appropriate locations
'''

def load_es(jd, es_host):
    """
    Save dataframe to Elasticsearch
    Args:
        jd: dataframe
        es_host: Elasticsearch cluster's IP
    """
    print(colored("[STARTING]: Saving to Elasticsearch", "green"))
    es = Elasticsearch([{'host': es_host, 'port': 9200}])

    if not es.ping():
        raise ValueError("Connection failed")

    print(colored("[PROCESSING]: Connection Good", "red"))
    es_write_conf = {
        "es.nodes" : es_host,
        "es.port" : "9200",
        "es.resource" : 'uniq/jobs',
        "es.input.json": "yes",
        "es.mapping.id": "uniq_id",
        "es.batch.size.entries": "100",
    }

    print(colored("[PROCESSING]: Converting to RDD", "red"))
    jd_rdd = jd.rdd.map(lambda item: {
        'uniq_id': item['uniq_id'],
        'job_title': item['job_title'],
        'company_name': item['company_name'],
        'state': item['state'],
        'post_date': item['post_date'],
        'job_description': item['job_description'],
        'keywords': tuple(item['keywords']) if item['keywords'] else None,
        'tags': tuple(item['tags']) if item['tags'] else None
        }).map(json.dumps).map(lambda x: ('key', x))

    print(colored("[PROCESSING]: Writing to ES", "red"))
    jd_rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)
    print(colored("[Finished]: Saving to Elasticsearch", "green"))

def update_state_list(jd_state):
    """
    Save the list of state for the dropdown menu
    Args:
        jd_state: dataframe column
    """
    print(colored("Generate State list", "green"))
    jd_state = jd_state.distinct()
    state_list = [row['state'] for row in jd_state.collect()]
    state_list = [s for s in state_list if s]
    state_list.sort()
    dump_pickle(state_list,'jd-parquet', 'assets/state.pkl')

def tag_count(jd_tags):
    """
    Save the list of tags and their frequency of appearance among job postings
    Args:
        jd_tags: dataframe
    """
    print(colored("Calculating tag counts", "green"))
    tag_count = jd_tags.withColumn("tags", explode("tags"))\
        .groupBy('tags').count().sort('count', ascending=False)
    tag_count_list = [(row['tags'], row['count']) for row in tag_count.collect()]
    dump_pickle(tag_count_list,'jd-parquet', 'assets/tag_count.pkl')
