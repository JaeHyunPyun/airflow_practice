import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(task_id="python_xcom_push_by_return")
    def xcom_push_result(**kwargs):
        return "Success"

    @task(task_id="python_xcom_pull_1")
    def xcom_pull_1(**kwargs):
        ti = kwargs["ti"]
        ## task_ids = 'python_xcom_push_by_return' 인 task가 return 한 값을
        ## xcom 에서 가져오겠다라는 의미
        ## key 값을 생략하더라도 자동으로 key : 'return_value' 로 설정됨
        value1 = ti.xcom_pull(task_ids="python_xcom_push_by_return")
        print("xcom_pull 메서드로 직접 찾은 리턴 값: " + value1)

    @task(task_id="python_xcom_pull_2")
    def xcom_pull_2(status, **kwargs):
        print("함수 입력값으로 받은 값: " + status)

    python_xcom_push_by_return = xcom_push_result()

    ## task decorator가 붙어 있끼 때문에
    ## python_xcom_push_by_return task가 자신의 return값을 xcom에 push
    ## 또한 위에서 반환된 값을 인자로 전달하고 있으므로, 반대로 xcom에서 pull 해옴
    ## taskflow의 순서는 자동으로 python_xcom_push_by_return -> xcom_pull_2 로 정의됨
    xcom_pull_2(python_xcom_push_by_return)

    python_xcom_push_by_return >> xcom_pull_1()
