import sys
sys.path.append("/home/lia/Documents/airflow/airflow_pipeline/")
import json
from airflow.models import BaseOperator
from pathlib import Path

from hook.twittter_hook import TwitterHook



class TwitterOperator(BaseOperator):

    template_fields = ["query","file_path", "star_time", "end_time"]

    def __init__(self,file_path,end_time,start_time,query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.file_path= file_path
        self.query = query
        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True,exist_ok=True)
        
        
    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:
            for pg in TwitterHook(end_time, start_time, query).run():
                json.dump(pg,output_file, ensure_ascii=False)
                output_file.write("\n")