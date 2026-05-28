import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2026, 5, 17, tz="Asia/Seoul"),
    catchup=True,
) as dag:

    @task(task_id="python_task")
    def show_templates(**kwargs):
        from pprint import pprint

        pprint(kwargs)

    show_templates()
