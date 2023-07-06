import sys
sys.path.append("airflow_pipeline")

from airflow import DAG
from datetime import datetime,timedelta

from os.path import join

from operators.twitter_operator import TwitterOperator

with DAG(dag_id = "TwitterTest", start_date=datetime.now()) as dag:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(days=-7)).date().strftime(TIMESTAMP_FORMAT)
    query = "data science"

    tarefa1=TwitterOperator(file_path=join("datalake/twitter_datascience",
                                        "extract_data={{ ds }}",
                                        "datascience_{{ ds_nodash }}.json"),
                         query=query, start_time=start_time, 
                         end_time=end_time, task_id="test_run")