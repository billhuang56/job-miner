import os
from datetime import datetime
from airflow import DAG
from airflow.operators import update_sensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from spark_bash_commands import convert_parquet, update_db

'''
Run the batch job using Airflow Scheduler
    1. Check if new xml files are updated
    2. If so, convert them into Parquet format
    3. Run the NLP processes and save the results to ES through Spark
    4. Email the engineer about the job completion
'''
email_address = os.environ["AIRFLOW_EMAIL"]

dag = DAG('Job_Posting_Scheduler',
          description = 'Update_the_job_postings',
          schedule_interval = '@once', # '@daily'
          start_date = datetime(2019, 10, 10), catchup = False)

check_new_data = update_sensor(task_id = 'check_for_new_file',
                               poke_interval = 60*60*23,
                               email_on_failure = True,
                               email = email_address,
                               dag = dag)

convert_to_parquet = BashOperator(task_id = 'convert_parquet',
                                  bash_command = convert_parquet,
                                  email_on_failure = True,
                                  email = email_address,
                                  dag = dag)

update_database = BashOperator(task_id = 'update_the_database',
                                  bash_command = update_db,
                                  email_on_failure = True,
                                  email = email_address,
                                  dag = dag)

email = EmailOperator(task_id = 'send_email',
                      to = email_address,
                      subject = 'Airflow Alert',
                      html_content=""" <h3> Database Updated </h3> """,
                      dag = dag)

check_new_data >> convert_to_parquet >> update_database >> email
