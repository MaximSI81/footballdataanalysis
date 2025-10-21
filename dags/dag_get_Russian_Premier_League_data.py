import sys
sys.path.append('/opt/airflow/dags/scripts')
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta


def get_next_round():
    try:
        # Пытаемся получить существующий round
        current_round = Variable.get("football_current_round_Russian_Premier_League")
        new_round = int(current_round) + 1
        Variable.set("football_current_round_Russian_Premier_League", new_round)
    except:
        # Если Variable не существует (первый запуск)
        Variable.set("football_current_round_Russian_Premier_League", 1)  # ← СОХРАНЯЕМ 1



with DAG(
    dag_id="football_Russian_Premier_League",
    start_date=datetime.now() - timedelta(days=1),
    description='Анализ футбольных данных Russian_Premier_League',
    schedule_interval=None,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
    ) as dag:
    
    get_round_task = PythonOperator(task_id="get_round",
                                    python_callable=get_next_round)
    
  

    bash_task = BashOperator(
        task_id='run_my_script',
        bash_command="cd /opt/airflow/dags/scripts && python running_script.py --round {{ var.value.get('football_current_round_Russian_Premier_League') }} --tournament 203 --season 77142"
    )
    get_round_task >> bash_task