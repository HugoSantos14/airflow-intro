from airflow.sdk import dag, task

@dag
def branch():
    
    @task
    def a():
        return 0
    
    @task.branch
    def b(val: int):
        if val == 0:
            return "equal_0"
        return "not_equal_0"
    
    @task
    def equal_0():
        print("Equal to 0")
    
    @task
    def not_equal_0():
        print("Not equal to 0")
    
    b(a()) >> [equal_0(), not_equal_0()]
    
branch()