import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # Start_Date : 전월 말일, END_DATE : 배치 도는 날 1일전
    bash_task_1 = BashOperator(
        task_id="bask_task_1",
        env={
            # START_DATE 은 전월말일이 들어가야하는데,
            ## 현재 배치가 매월 말일에 돌아가도록 되어 있고, 따라서 이번달 말일에 배치가 돌아갈 텐데,
            ## 그때 기준으로 data_interval_start는 바로 직전 배치가 돌아갔던 날짜이므로 전월 말일 날짜가 들어감
            ## 기본적으로 airflow 날짜는 utc 기준이므로 한국 기준으로 돌리기 위해서 in_timezone 필요
            ## ` | ds`를 사용해서 yyyymmdd 형식으로 출력
            "START_DATE": '{{data_interval_start.in_timezone("Asia/Seoul") | ds}}',
            ## macro를 사용해서 파이썬 dateutil함수 사용
            ## macro는 template 내에서 파이썬 date관련 함수를 사용할 수 있도록 함
            ## ` | ds`를 사용해서 yyyymmdd 형식으로 출력
            "END_DATE": '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}',
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"',
    )
