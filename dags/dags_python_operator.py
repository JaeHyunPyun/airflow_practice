import pendulum
import random

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * * ",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    def select_fruit():
        fruit = ["APPLE", "BANANA", "ORANGE", "AVOCADO"]
        rand_int = random.randint(0, 3)  # 0, 1, 2, 3
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id="py_t1",
        # 실행할 python 함수 이름 지정
        python_callable=select_fruit,
    )

    py_t1
