import sys
sys.path.append('/opt/airflow/dags/scripts')
from clickhouse_driver import Client
from footbolldatacollector import *
import asyncio
from typing import List, Dict, Any

class FootballDataOrchestrator:
    def __init__(self, ch_host: str, ch_user: str, ch_password: str, tournament_id: int, season_id: int, ch_database: str = 'football'):
        self.ch_client = Client(
            host=ch_host,
            user=ch_user,
            password=ch_password,
            database=ch_database
        )
        self.tournament_id = tournament_id 
        self.season_id = season_id 
    
    async def process_round(self, round_number: int):
        """Обрабатывает данные тура и загружает в ClickHouse"""
        async with FootballDataCollector() as collector:
            # 1. Получаем матчи тура
            matches = await collector.get_round_matches(
                self.tournament_id, self.season_id, round_number
            )
            print(f"🔍 Получено матчей: {len(matches)}")
            if not matches:
                print(f"❌ В туре {round_number} нет матчей")
                return
            
            # 2. Загружаем матчи в ClickHouse
            self._insert_matches(matches)
            print(f"✅ Загружено {len(matches)} матчей тура {round_number}")
            
            # 3. Для каждого матча получаем статистику игроков
            total_player_stats = 0
            for match in matches:
                match_id = match['match_id']
                print(f"🔍 Обрабатываем матч {match_id}...")
                
                # Пауза между запросами
                await asyncio.sleep(1)
                
                player_stats = await collector.get_player_statistics(match_id)
                if player_stats:
                    self._insert_player_stats(player_stats)
                    total_player_stats += len(player_stats)
                    print(f"   ✅ Статистика {len(player_stats)} игроков загружена")
                else:
                    print(f"   ❌ Нет статистики для матча {match_id}")
            
            print(f"🎯 Тур {round_number} обработан: {total_player_stats} записей статистики игроков")

    def _insert_matches(self, matches: List[Dict[str, Any]]):
        """Вставляет данные матчей в ClickHouse"""
        query = """
        INSERT INTO football_matches (
            match_id, tournament_id, season_id, round_number, match_date,
            home_team_id, home_team_name, away_team_id, away_team_name,
            home_score, away_score, status, venue, start_timestamp
        ) VALUES
        """
        
        data = []
        for match in matches:
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
                match['start_timestamp']
            ))
        
        self.ch_client.execute(query, data)
    
    def _insert_player_stats(self, player_stats: List[Dict[str, Any]]):
        """Вставляет статистику игроков в ClickHouse"""
        try:
            print(f"🔍 Подготовка данных для вставки {len(player_stats)} записей статистики...")
            
            query = """
            INSERT INTO football_player_stats (
                match_id, team_id, player_id, player_name, short_name, position,
                jersey_number, minutes_played, rating, goals, goal_assist,
                total_shot, on_target_shot, off_target_shot, blocked_scoring_attempt,
                total_pass, accurate_pass, pass_accuracy, key_pass, total_long_balls, accurate_long_balls,
                successful_dribbles, dribble_success, total_tackle, interception_won,
                total_clearance, outfielder_block, challenge_lost, duel_won, duel_lost,
                aerial_won, duel_success, touches, possession_lost_ctrl, was_fouled,
                fouls, yellow_cards, red_cards, saves, punches, good_high_claim,
                saved_shots_from_inside_box
            ) VALUES
            """
            
            data = []
            for i, stats in enumerate(player_stats, 1):
                try:
                    # Обрабатываем каждое поле, обеспечивая правильные типы данных
                    team_id = int(stats['team_id']) if stats['team_id'] is not None else 0
                    player_id = int(stats['player_id']) if stats['player_id'] is not None else 0
                    jersey_number = int(stats['jersey_number']) if stats['jersey_number'] is not None else 0
                    minutes_played = int(stats['minutes_played']) if stats['minutes_played'] is not None else 0
                    
                    # Обрабатываем числовые поля
                    goals = int(stats.get('goals', 0))
                    goal_assist = int(stats.get('goal_assist', 0))
                    rating = float(stats.get('rating', 0))
                    
                    # Обрабатываем строковые поля
                    player_name = stats.get('player_name', '') or ''
                    short_name = stats.get('short_name', '') or ''
                    position = stats.get('position', '') or ''
                    
                    if i <= 3:  # Показываем первые 3 записи для отладки
                        print(f"  Запись {i}: team_id={team_id}, player='{short_name}', rating={rating}")
                    
                    data.append((
                        int(stats['match_id']),
                        team_id,
                        player_id,
                        player_name,
                        short_name,
                        position,
                        jersey_number,
                        minutes_played,
                        rating,
                        goals,
                        goal_assist,
                        int(stats.get('total_shot', 0)),
                        int(stats.get('on_target_shot', 0)),
                        int(stats.get('off_target_shot', 0)),
                        int(stats.get('blocked_scoring_attempt', 0)),
                        int(stats.get('total_pass', 0)),
                        int(stats.get('accurate_pass', 0)),
                        float(stats.get('pass_accuracy', 0)),
                        int(stats.get('key_pass', 0)),
                        int(stats.get('total_long_balls', 0)),
                        int(stats.get('accurate_long_balls', 0)),
                        int(stats.get('successful_dribbles', 0)),
                        float(stats.get('dribble_success', 0)),
                        int(stats.get('total_tackle', 0)),
                        int(stats.get('interception_won', 0)),
                        int(stats.get('total_clearance', 0)),
                        int(stats.get('outfielder_block', 0)),
                        int(stats.get('challenge_lost', 0)),
                        int(stats.get('duel_won', 0)),
                        int(stats.get('duel_lost', 0)),
                        int(stats.get('aerial_won', 0)),
                        float(stats.get('duel_success', 0)),
                        int(stats.get('touches', 0)),
                        int(stats.get('possession_lost_ctrl', 0)),
                        int(stats.get('was_fouled', 0)),
                        int(stats.get('fouls', 0)),
                        int(stats.get('yellow_cards', 0)),
                        int(stats.get('red_cards', 0)),
                        int(stats.get('saves', 0)),
                        int(stats.get('punches', 0)),
                        int(stats.get('good_high_claim', 0)),
                        int(stats.get('saved_shots_from_inside_box', 0))
                    ))
                except Exception as e:
                    print(f"  ❌ Ошибка подготовки записи {i}: {e}")
                    print(f"     Данные: {stats}")
                    continue
            
            if not data:
                print("❌ Нет данных для вставки")
                return
                
            print(f"🔍 Выполняем запрос к ClickHouse для {len(data)} записей...")
            self.ch_client.execute(query, data)
            print(f"✅ Успешно вставлено {len(data)} записей статистики игроков")
            
        except Exception as e:
            print(f"❌ Ошибка вставки статистики игроков: {e}")
            import traceback
            traceback.print_exc()
            raise