import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # 1안 - Variable 라이브러리 이용해서 변수 꺼내옴
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
        task_id="bash_var_1", bash_command=f"echo variable:{var_value}"
    )

    # 2안 - Operator에서 직접 template 변수를 이용해서 variable 변수를 꺼냄
    bash_var_2 = BashOperator(
        task_id="bash_var_2", bash_command="echo variable:{{var.value.sample_key}}"
    )
