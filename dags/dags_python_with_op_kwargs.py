from common.common_func import regist2
import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
  dag_id="dags_python_with_op_kwargs",
  schedule="30 6 * * *",
  start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
  catchup=False
) as dag:
  
  regist2_t1 = PythonOperator(
    task_id='regist2_t1',
    python_callable=regist2,
    op_args=['hjkim','man', 'kr', 'seoul']
    op_kwargs={
      'email' : 'hjkim_sum@naver.com',
      'phone' : '010',
      }
  )