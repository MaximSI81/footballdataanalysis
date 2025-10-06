import sys
sys.path.append('/opt/airflow/dags/scripts')
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

"""
# Расписание туров: {тур: дата_тура}
TOUR_SCHEDULE = {
    1: datetime(2025, 7, 21),  # Тур 1 завершился 21.07
    2: datetime(2025, 7, 27),  # Тур 2 завершился 27.07
    3: datetime(2025, 8, 4),   # Тур 3 завершился 04.08
    4: datetime(2025, 8, 10),  # Тур 4 завершился 10.08
    5: datetime(2025, 8, 18),  # Тур 5 завершился 18.08
    6: datetime(2025, 8, 24),  # Тур 6 завершился 24.08
    7: datetime(2025, 8, 31),  # Тур 7 завершился 31.08
    8: datetime(2025, 9, 14),  # Тур 8 завершился 14.09
    9: datetime(2025, 9, 22),  # Тур 9 завершился 22.09
    10: datetime(2025, 9, 28), # Тур 10 завершился 28.09
    11: datetime(2025, 10, 5), # Тур 11 завершится 05.10
    12: datetime(2025, 10, 19),# Тур 12 завершится 19.10
    13: datetime(2025, 10, 27),# Тур 13 завершится 27.10
    14: datetime(2025, 11, 2), # Тур 14 завершится 02.11
    15: datetime(2025, 11, 9), # Тур 15 завершится 09.11
    16: datetime(2025, 11, 22),# Тур 16 завершится 22.11
    17: datetime(2025, 11, 29),# Тур 17 завершится 29.11
    18: datetime(2025, 12, 6), # Тур 18 завершится 06.12
    19: datetime(2026, 2, 28), # Тур 19 завершится 28.02
    20: datetime(2026, 3, 7),  # Тур 20 завершится 07.03
    21: datetime(2026, 3, 14), # Тур 21 завершится 14.03
    22: datetime(2026, 3, 21), # Тур 22 завершится 21.03
    23: datetime(2026, 4, 4),  # Тур 23 завершится 04.04
    24: datetime(2026, 4, 11), # Тур 24 завершится 11.04
    25: datetime(2026, 4, 18), # Тур 25 завершится 18.04
    26: datetime(2026, 4, 22), # Тур 26 завершится 22.04
    27: datetime(2026, 4, 25), # Тур 27 завершится 25.04
    28: datetime(2026, 5, 2),  # Тур 28 завершится 02.05
    29: datetime(2026, 5, 9),  # Тур 29 завершится 09.05
    30: datetime(2026, 5, 17), # Тур 30 завершится 17.05
}
"""


def get_next_round():
    try:
        # Пытаемся получить существующий round
        current_round = Variable.get("football_current_round")
        new_round = int(current_round) + 1
        Variable.set("football_current_round", new_round)
    except:
        # Если Variable не существует (первый запуск)
        Variable.set("football_current_round", 1)  # ← СОХРАНЯЕМ 1



with DAG(
    dag_id="footboll",
    start_date=datetime.now() - timedelta(days=1),
    description='Анализ футбольных данных премьер лиги',
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
        bash_command="cd /opt/airflow/dags/scripts && python running_script.py --round {{ var.value.get('football_current_round') }} --tournament 203 --season 77142"
    )
    get_round_task >> bash_task