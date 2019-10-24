convert_parquet = ('spark-submit --master spark://ec2-3-209-174-168.compute-1.amazonaws.com:7077 '
                                '--packages com.databricks:spark-xml_2.11:0.6.0 '
                                '~/job-miner/src/save_parquet.py')

update_db = ('spark-submit --master spark://ec2-3-209-174-168.compute-1.amazonaws.com:7077 '
                                '--jars ~/job-miner/src/elasticsearch-hadoop-7.4.0.jar '
                                '--num-executors 9 '
                                '--executor-cores 2 '
                                '--executor-memory 1500m '
                                '~/job-miner/src/process.py')
