import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command='echo "data_interval_end: {{data_interval_end}}"',
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        env={
            "START_DATE": "{{data_interval_start | ds}}",  # YYYYMMDD 형식으로 출력위해서 | ds 붙임
            "END_DATE": "{{data_interval_end | ds}}",
        },
        bash_command="echo $START_DATE && echo $END_DATE",
    )

    bash_t1 >> bash_t2
