import os

LOG_DIR = "home/ubuntu/job-miner/logs/"
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

XML_PATH = 's3a://jd-scrapped/dice_xml/*.xml'
PARQUET_PATH = 's3a://jd-parquet/jd_raw.parquet'

ES_HOST = '10.0.0.13'
ES_PORT = '9200'
PARQUET_BUCKET = 'jd-parquet'
COMMON_WORDS_PATH = 'assets/common_jd_words.pkl'
TAG_PATH = 'assets/stack_tags.pkl'
STATE_PATH = 'assets/state.pkl'

PSQL_HOST = '10.0.0.9'
PSQL_PORT = '5432'
PSQL_DB = 'test'
PSQL_USER = 'postgres'
PSQL_PWD = 'database101'
