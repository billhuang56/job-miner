from elasticsearch import Elasticsearch
import psycopg2
import pandas as pd
from statistics import mean
import time
import random
import pickle
from scipy import stats
import config as conf

'''
Run query speed tests from local machines between PostgreSQL and Elasticsearch
'''
# Open the list of tags
with open('tag_count.pkl', 'rb') as file:
    tag_list = pickle.load(file)
tag_list = [t[0] for t in tag_list]

# Elasticsearch configuration
es_host = conf.ES_HOST
es = Elasticsearch([{'host': es_host, 'port': conf.ES_PORT}])

# PostgreSQL configuration
ps_host = conf.PSQL_HOST
ps_user = conf.PSQL_USER
ps_pwd = conf.PSQL_PWD
ps_dbname = 'test'
con = psycopg2.connect(database = ps_dbname,
                       user = ps_user,
                       password = ps_pwd,
                       host = ps_host)
def es_new_query(tags=[]):
    """
    Query from Elasticsearch
    Args:
        tags: list of tags
    Returns:
        job postings containing the tags, ranking by relevance
    """
    tags = [t.lower() for t in tags]
    body = {"size" : 25,
        'query':{
            'match':{
                'tags': ' '.join(tags)
            }
        }
    }
    return body

def ps_new_query(tags=[], options = "all"):
    """
    Query from PostgreSQL
    Args:
        tags: list of tags
        options: 'all' means the posting must contains all the tags while 'any' means
        that the posting needs to contain at least one of the tags
    Returns:
        job postings containing the tags, either containing all of the tags or
        at least one of the tags
    """
    if options == 'all':
        sql_query = "SELECT * FROM jobs WHERE tags @> array{} LIMIT 25".format(tags)
    else:
        sql_query = "SELECT * FROM jobs WHERE tags && array{} LIMIT 25".format(tags)
    return sql_query

def speed_test(iterations = 2000, tag_num = 5):
    """
    Query 5 random tags from PostgreSQL and Elasticsearch for 2000 times
    Args:
        iterations: number of iterations
        tag_num: number of tags
    """
    es_time = [None]*iterations
    ps_time = [None]*iterations
    
    for i in range(0, iterations):
        # Randomly select 5 tags
        sampling = random.choices(tag_list, k = tag_num)

        # Run Elasticsearch query and track the time
        es_start_time = time.time()
        es_res = es.search(index='uniq',body = es_new_query(sampling))
        es_end_time = time.time()
        es_time[i] = es_end_time - es_start_time

        # Run PostgreSQL query and track the time
        ps_start_time = time.time()
        ps_res = pd.read_sql_query(ps_new_query(sampling), con)
        ps_end_time = time.time()
        ps_time[i] = ps_end_time - ps_start_time

    print("Elasticsearch: the average time is {}.".format(mean(es_time)))
    print("PostgreSQL: the average time is {}.".format(mean(ps_time)))
    print("The different in time is {}".format(mean(es_time) - mean(ps_time)))
    t = stats.ttest_ind(es_time, ps_time)
    print("The t-statistic is {} and the p-value is {}.".format(t[0], t[1]))

if __name__ == "__main__":
    speed_test()
