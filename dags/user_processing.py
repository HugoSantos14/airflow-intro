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

    @task
    def extract_user(fake_user: PokeReturnValue):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    @task
    def process_user(user_info):
        import csv
        from datetime import datetime
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )

    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    process_user(user_info)

user_processing()