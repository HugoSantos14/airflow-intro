from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue

@dag
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                firstname VARCHAR(255),
                lastname VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    )

    @task.sensor(
        poke_interval=30,   # O sensor vai verificar a condição a cada 30 segundos.
        timeout=300         # Se todas as tentativas dentro de 5 minutos retornarem falso, o sensor falha.
    )
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")

        is_available = response.status_code == 200
        fake_user = response.json() if is_available else None

        return PokeReturnValue(is_done=is_available, xcom_value=fake_user)

    is_api_available()

user_processing()