from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():
    
    @task
    def t1() -> Dict[str, Any]:
        return {
            "my_val": 50,
            "my_str": "Hello, world!"
        }
    
    @task
    def t2(data: Dict[str, Any]):
        print(data["my_val"])
        print(data["my_str"])
    
    t2(t1())
    
xcom_dag()