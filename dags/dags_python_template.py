import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 10, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id="python_t1",
        python_callable=python_function1,
        op_kwargs={
            "start_date": "{{data_interval_start | ds}}",
            "end_date": "{{data_interval_end | ds}}",
        },
    )

    @task(task_id="python_t2")
    def python_function2(**kwargs):
        print(kwargs)
        print("ds: " + kwargs["ds"])
        print("ts: " + kwargs["ts"])
        print("data_interval_start: " + str(kwargs["data_interval_start"]))
        print("data_interval_end: " + str(kwargs["data_interval_end"]))
        print("task_instance: " + str(kwargs["ti"]))

  # task decorator를 사용할때는 해당 함수를 실행시키기만 해도 task가 동작ㄹ
  python_t1 >> python_function2()