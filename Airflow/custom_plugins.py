from datetime import datetime, date
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
import boto3
import botocore
import airflow_config as conf
'''
Define S3 sensor to check if a new log file have been added
'''
def check_s3(date):
    	"""
    	Takes in today's date to check if a log file was added with the raw data
            successfully added to S3.
    	Args:
    	 	date: today's date
    	Returns:
            True or False
    	"""
    s3 = boto3.resource('s3', aws_access_key_id = conf.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = conf.AWS_SECRET_ACCESS_KEY)
    try:
        key = 'logs/' + date + conf.SCRAPPED_LOG
        obj = s3.Object(conf.SCRAPPED_BUCKET, key)
        log_info = obj.get()['Body'].read().decode('utf-8')
        if "Scrapped File Upload: Successful" in log_info:
            return True
        return False

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise

class update_sensor(BaseSensorOperator):
    """
	Sensor operator
    """
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(update_sensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        today = date.today().strftime("%y%m%d")
        return check_s3(today)

class custom_plugins(AirflowPlugin):
    """
	Put the sensor in the custom_plugins so Airflow can detect it
    """
    name = "custom_plugins"
    operators = [update_sensor]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
