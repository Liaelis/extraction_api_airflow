import sys
sys.path.append("airflow_pipeline")

from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
from airflow.utils.dates import days_ago
from operators.twitter_operator import TwitterOperator

with DAG(dag_id = "TwitterDAG", start_date=days_ago(2),schedule_interval="@daily") as dag:
    query = "data science"

    twitter_operator=TwitterOperator(file_path=join("datalake/twitter_datascience",
                                        "extract_data={{ ds }}",
                                        "datascience_{{ ds_nodash }}.json"),
                         query=query, start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                         end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", task_id="twitter_datascience")
    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
                                            application="/home/lia/Documents/airflow/src/spark/transformation.py",
                                            name="twittter transformation",
                                            application_args=["--src", "/home/lia/Documents/airflow/datalake/twitter_datascience",
                                                              "--dest","/home/lia/Documents/airflow/data_transformate",
                                                              "--process_date", "{{ds}}"]
                                          )
    twitter_operator >> twitter_transform
