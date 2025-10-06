import sys
sys.path.append('/opt/airflow/dags/scripts')
import asyncio
import argparse
from datetime import datetime
from footbolldataclickhouse import *
from dotenv import load_dotenv  # Для загрузки переменных окружения
import os

# Загружаем переменные окружения из .env файла (опционально)
load_dotenv()

async def main():
    parser = argparse.ArgumentParser(description='Football Data Orchestrator')
    parser.add_argument('--round', type=int, required=True, help='Round number to process')
    parser.add_argument('--tournament', required=True, type=int, help='ID турнира')
    parser.add_argument('--season', required=True, type=int, help='ID сезона')
    parser.add_argument('--host', default=os.getenv('CLICKHOUSE_HOST', 'localhost'), help='ClickHouse host')
    parser.add_argument('--user', default=os.getenv('CLICKHOUSE_USER', 'default'), help='ClickHouse user')
    parser.add_argument('--password', default=os.getenv('CLICKHOUSE_PASSWORD', ''), help='ClickHouse password')
    parser.add_argument('--database', default=os.getenv('CLICKHOUSE_DB', 'football'), help='ClickHouse database name')
    parser.add_argument('--port', type=int, default=os.getenv('CLICKHOUSE_PORT', 9000), help='ClickHouse port')
    
    args = parser.parse_args()
    
    print(f"🚀 Запуск обработки тура {args.round} в {datetime.now()}")
    print(f"📊 Подключение к БД: {args.host}:{args.port}/{args.database}")
    
    # Создаем оркестратор с вашими настройками
    orchestrator = FootballDataOrchestrator(
        ch_host=args.host,
        ch_user=args.user,
        ch_password=args.password,
        ch_database=args.database,
        tournament_id=args.tournament,
        season_id=args.season

    )
    
    try:
        await orchestrator.process_round(args.round)
        print(f"✅ Тур {args.round} успешно обработан!")
    except Exception as e:
        print(f"❌ Ошибка обработки тура {args.round}: {e}")

if __name__ == "__main__":
    asyncio.run(main())