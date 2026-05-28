import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="10 0 * * 6#2",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # START_DATE = 2주전 월요일, END_DATE = 2주전 토요일
    bash_task_2 = BashOperator(
        task_id="bash_task_2",
        env={
            ## 2주전 토요일/월요일은 배치가 돌아가는 날짜 기준으로 며칠 이전으로 돌아가면 되는지 계산하면 됨
            ## 예를들어 배치 돌아가는 날짜가 2026/5/28(목) 이면
            ## 2주전 토요일은 12일전, 2주전 월요일은 17일전
            "START_DATE": '{{(date_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=17)) | ds}}',
            "END_DATE": '{{(date_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=12)) | ds}}',
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"',
    )
