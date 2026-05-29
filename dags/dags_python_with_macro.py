import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",  # daily schedule
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(
        task_id="task_using_macros",
        templates_dict={
            "start_date": '{{(data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds}}',
            "end_date": '{{(data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds}}',
        },
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs["templates_dict"] or {}
        if templates_dict:
            start_date = templates_dict["start_date"] or "start_date없음"
            end_date = templates_dict["end_date"] or "end_date없음"
            print(start_date)
            print(end_date)

    @task(task_id="task_direct_calc")
    def get_datetime_calc(**kwargs):
        # 왜 import 문을 파일 최상단이 아니라 DAG 안에 두었을까?
        # - 스케줄러 부하 경감을 위해서임
        # - 스케줄러는 주기적으로 DAG 파일들을 순회하면서 파싱함(문법적인 오류 체크 등)
        # - 만약 import문을 맨 위에 달아두면 스케줄러가 실제 그 DAG를 실행하지도 않는데 주기적으로
        # - 해당 라이브러리를 계속 import함
        # - task 안에다가 import문을 두면 스케줄러가 파싱할때는 해당 부분은 무시하고 지나가고
        # - 해당 task가 실제로 실행되는 시점에만 라이브러리를 로드함
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs["data_interval_end"]
        prev_month_day_first = data_interval_end.in_timezone(
            "Asia/Seoul"
        ) + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(
            day=1
        ) + relativedelta(days=-1)
        print(prev_month_day_first.strftime("%Y-%m-%d"))
        print(prev_month_day_last.strftime("%Y-%m-%d"))

    get_datetime_macro() >> get_datetime_calc()
