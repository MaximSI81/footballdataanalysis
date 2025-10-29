import sys
sys.path.append('/opt/airflow/dags/scripts')
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta




with DAG(
    dag_id="fixture_Ligue_1",
    start_date=datetime.now() - timedelta(days=1),
    description='Получение информации по рефери и наполнение match_fixture',
    schedule_interval=None,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
    ) as dag:
    
    
    bash_task = BashOperator(
        task_id='run_my_script',
        bash_command="cd /opt/airflow/dags/scripts && python running_script.py --round {{ var.value.get('football_current_round_Ligue_1') }} --tournament 34 --season 77356 --fixtures"
    )
