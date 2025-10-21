import asyncio
from datetime import datetime
from clickhouse_driver import Client
from sofascore_wrapper.api import SofascoreAPI

class TournamentMatchesCollector:
    def __init__(self, ch_host: str, ch_user: str, ch_password: str, ch_database: str = 'football_db'):
        self.ch_client = Client(
            host=ch_host,
            user=ch_user,
            password=ch_password,
            database=ch_database
        )
        self.api = None
    
    async def collect_season_data(self, tournament_id: int, season_id: int, season_name: str, rounds_count: int):
        """Собирает данные всего сезона"""
        
        try:
            # Создаем экземпляр API
            self.api = SofascoreAPI()
            
            print(f"🎯 Начинаем сбор сезона: {season_name}")
            print(f"📊 Количество туров: {rounds_count}")
            print("=" * 50)
            
            all_matches_data = []
            successful_rounds = 0
            
            for round_number in range(1, rounds_count + 1):
                print(f"📊 Обрабатываем тур {round_number}...")
                
                round_matches = await self.get_round_matches(tournament_id, season_id, round_number)
                
                if round_matches:
                    all_matches_data.extend(round_matches)
                    successful_rounds += 1
                    print(f"✅ Тур {round_number}: {len(round_matches)} матчей")
                else:
                    print(f"⚠️  Тур {round_number}: матчи не найдены")
                
                # Задержка чтобы не перегружать API
                await asyncio.sleep(1)
            
            # Вставляем все собранные данные
            if all_matches_data:
                self.insert_matches_data(all_matches_data)
                print(f"\n🎉 Сбор завершен! Успешных туров: {successful_rounds}/{rounds_count}")
                print(f"📊 Всего загружено: {len(all_matches_data)} матчей")
                return True
            else:
                print("❌ Не удалось собрать данные матчей")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка при сборе сезона {season_name}: {e}")
            return False
        finally:
            # Закрываем соединение с API
            if self.api:
                await self.api.close()
    
    async def get_round_matches(self, tournament_id: int, season_id: int, round_number: int):
        """Получает матчи указанного тура"""
        try:
            endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/events/round/{round_number}"
            print(f"🔍 API запрос: {endpoint}")
            
            data = await self.api._get(endpoint)
            
            matches_data = []
            for match in data.get('events', []):
                match_info = self.extract_match_info(match, tournament_id, season_id, round_number)
                if match_info:
                    matches_data.append(match_info)
            
            return matches_data
            
        except Exception as e:
            print(f"❌ Ошибка получения матчей тура {round_number}: {e}")
            return []
    
    def extract_match_info(self, match: dict, tournament_id: int, season_id: int, round_number: int):
        """Извлекает информацию о матче"""
        try:
            home_score = match.get('homeScore', {}).get('current')
            away_score = match.get('awayScore', {}).get('current')
            venue = match.get('venue', {}).get('name')
            
            match_info = {
                'match_id': match['id'],
                'tournament_id': tournament_id,
                'season_id': season_id,
                'round_number': round_number,
                'match_date': datetime.fromtimestamp(match['startTimestamp']).date(),
                'home_team_id': match['homeTeam']['id'],
                'home_team_name': match['homeTeam']['name'],
                'away_team_id': match['awayTeam']['id'],
                'away_team_name': match['awayTeam']['name'],
                'home_score': home_score if home_score is not None else 0,
                'away_score': away_score if away_score is not None else 0,
                'status': match['status']['description'],
                'venue': venue if venue is not None else 'Неизвестно',
                'start_timestamp': datetime.fromtimestamp(match['startTimestamp']),
                'created_at': datetime.now()
            }
            return match_info
            
        except Exception as e:
            print(f"❌ Ошибка извлечения данных матча {match.get('id')}: {e}")
            return None
    
    def insert_matches_data(self, matches_data: list):
        """Вставляет данные матчей в ClickHouse"""
        try:
            if not matches_data:
                print("❌ Нет данных для вставки")
                return
            
            query = """
            INSERT INTO football_matches (
                match_id, tournament_id, season_id, round_number, match_date,
                home_team_id, home_team_name, away_team_id, away_team_name,
                home_score, away_score, status, venue, start_timestamp, created_at
            ) VALUES
            """
            
            data = []
            for match in matches_data:
                data.append((
                    match['match_id'],
                    match['tournament_id'],
                    match['season_id'],
                    match['round_number'],
                    match['match_date'],
                    match['home_team_id'],
                    match['home_team_name'],
                    match['away_team_id'],
                    match['away_team_name'],
                    match['home_score'],
                    match['away_score'],
                    match['status'],
                    match['venue'],
                    match['start_timestamp'],
                    match['created_at']
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Успешно вставлено {len(data)} матчей в football_matches")
            
        except Exception as e:
            print(f"❌ Ошибка вставки данных: {e}")

# ОСНОВНОЙ СКРИПТ ДЛЯ ВСЕХ ТУРНИРОВ
async def main():
    """Загружает данные по всем турнирам с указанными сезонами"""
    
    collector = TournamentMatchesCollector(
        ch_host='localhost',
        ch_user='username', 
        ch_password='password',
        ch_database='football_db'
    )
    
    # Конфигурация всех турниров
    TOURNAMENTS = [
        # Лига 1 (Франция)
        {
            'id': 34,
            'name': 'Лига 1',
            'seasons': [
                (61736, "Лига 1 2024/2025", 34),
                (52571, "Лига 1 2023/2024", 34)
            ]
        },
        # Бундеслига (Германия)
        {
            'id': 35,
            'name': 'Бундеслига',
            'seasons': [
                (63516, "Бундеслига 2024/2025", 34),
                (52608, "Бундеслига 2023/2024", 34)
            ]
        },
        # Английская Премьер-Лига
        {
            'id': 17,
            'name': 'Английская Премьер-Лига',
            'seasons': [
                (61627, "АПЛ 2024/2025", 38),
                (52186, "АПЛ 2023/2024", 38)
            ]
        },
        # Ла Лига (Испания)
        {
            'id': 8,
            'name': 'Ла Лига',
            'seasons': [
                (61643, "Ла Лига 2024/2025", 38),
                (52376, "Ла Лига 2023/2024", 38)
            ]
        },
        # Серия А (Италия)
        {
            'id': 23,
            'name': 'Серия А',
            'seasons': [
                (63515, "Серия А 2024/2025", 38),
                (52760, "Серия А 2023/2024", 38)
            ]
        },
        # Российская Премьер-Лига
        {
            'id': 203,
            'name': 'Российская Премьер-Лига',
            'seasons': [
                (52470, "РПЛ 2023/2024", 30),
                (61712, "РПЛ 2024/2025", 30)
            ]
        }
    ]
    
    total_matches = 0
    total_seasons = 0
    
    for tournament in TOURNAMENTS:
        tournament_id = tournament['id']
        tournament_name = tournament['name']
        
        print(f"\n{'='*80}")
        print(f"🏆 ТУРНИР: {tournament_name} (ID: {tournament_id})")
        print(f"📊 Количество сезонов: {len(tournament['seasons'])}")
        print('='*80)
        
        for season_id, season_name, rounds_count in tournament['seasons']:
            print(f"\n🎯 ЗАГРУЗКА СЕЗОНА: {season_name}")
            print(f"📊 Турнир: {tournament_name}, Сезон: {season_id}")
            print(f"🎯 Количество туров: {rounds_count}")
            print('-' * 50)
            
            success = await collector.collect_season_data(
                tournament_id=tournament_id,
                season_id=season_id, 
                season_name=season_name,
                rounds_count=rounds_count
            )
            
            if success:
                print(f"🎉 Сезон {season_name} успешно загружен!")
                total_seasons += 1
            else:
                print(f"❌ Ошибка загрузки сезона {season_name}")
            
            # Пауза между сезонами
            print("\n⏳ Пауза 5 секунд перед следующим сезоном...")
            await asyncio.sleep(5)
    
    print(f"\n{'='*80}")
    print(f"🎉 ВСЕ ТУРНИРЫ ЗАГРУЖЕНЫ!")
    print(f"📊 Всего обработано сезонов: {total_seasons}")
    print(f"📊 Всего турниров: {len(TOURNAMENTS)}")
    print('='*80)

if __name__ == "__main__":
    # Основной запуск - все турниры и сезоны
    asyncio.run(main())