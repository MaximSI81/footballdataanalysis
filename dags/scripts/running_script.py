import sys
sys.path.append('/opt/airflow/dags/scripts')
from clickhouse_driver import Client
from footbolldatacollector import FootballDataCollector
import asyncio
from typing import List, Dict, Any
import argparse
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class FootballDataOrchestrator:
    def __init__(self, ch_host: str, ch_user: str, ch_password: str, tournament_id: int, season_id: int, ch_database: str = 'football_db'):
        self.ch_client = Client(
            host=ch_host,
            user=ch_user,
            password=ch_password,
            database=ch_database
        )
        self.tournament_id = tournament_id 
        self.season_id = season_id 

    def _insert_matches(self, matches: List[Dict[str, Any]]):
        """Вставляет данные матчей в ClickHouse"""
        try:
            if not matches:
                return
                
            query = """
            INSERT INTO football_matches (
                match_id, tournament_id, season_id, round_number, match_date,
                home_team_id, home_team_name, away_team_id, away_team_name,
                home_score, away_score, status, start_timestamp, created_at
            ) VALUES
            """
            
            data = []
            for match in matches:
                data.append((
                    int(match['match_id']),
                    int(match['tournament_id']),
                    int(match['season_id']),
                    int(match['round_number']),
                    match['match_date'],
                    int(match['home_team_id']),
                    match['home_team_name'],
                    int(match['away_team_id']),
                    match['away_team_name'],
                    int(match['home_score']),
                    int(match['away_score']),
                    match['status'],
                    match['start_timestamp'],
                    datetime.now()
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Вставлено {len(data)} матчей")
            
        except Exception as e:
            print(f"❌ Ошибка вставки матчей: {e}")

    def _insert_match_fixtures(self, fixtures: List[Dict[str, Any]]):
        """Вставляет данные fixtures в ClickHouse"""
        try:
            if not fixtures:
                return
                
            query = """
            INSERT INTO match_fixtures (
                match_id, round_number, season_id, start_timestamp,
                referee_id, referee_name, referee_yellow_cards, referee_red_cards,
                referee_yellow_red_cards, referee_games, referee_country,
                venue_id, venue_name, venue_city, venue_capacity,
                home_team_id, home_team_name, home_team_short_name,
                home_manager_id, home_manager_name, home_manager_short_name,
                away_team_id, away_team_name, away_team_short_name,
                away_manager_id, away_manager_name, away_manager_short_name,
                tournament_id, tournament_name, season_year
            ) VALUES
            """
            
            data = []
            for i, fixture in enumerate(fixtures):
                # Обработка venue_capacity
                venue_capacity = fixture['venue_capacity']
                if isinstance(venue_capacity, (int, float)):
                    venue_capacity = min(65535, int(venue_capacity))
                else:
                    venue_capacity = 0
                
                data.append((
                    int(fixture['match_id']),
                    int(fixture['round_number']),
                    int(fixture['season_id']),
                    fixture['start_timestamp'],
                    
                    # Рефери
                    int(fixture['referee_id']),
                    fixture['referee_name'],
                    int(fixture['referee_yellow_cards']),
                    int(fixture['referee_red_cards']),
                    int(fixture['referee_yellow_red_cards']),
                    int(fixture['referee_games']),
                    fixture['referee_country'],
                    
                    # Стадион
                    int(fixture['venue_id']),
                    fixture['venue_name'],
                    fixture['venue_city'],
                    int(venue_capacity),
                    
                    # Команды - ДОМАШНЯЯ
                    int(fixture['home_team_id']),
                    fixture['home_team_name'],
                    fixture['home_team_short_name'],
                    int(fixture['home_manager_id']),
                    fixture['home_manager_name'],
                    fixture['home_manager_short_name'],
                    
                    # Команды - ГОСТЕВАЯ
                    int(fixture['away_team_id']),
                    fixture['away_team_name'],
                    fixture['away_team_short_name'],
                    int(fixture['away_manager_id']),
                    fixture['away_manager_name'],
                    fixture['away_manager_short_name'],
                    
                    # Турнир
                    int(fixture['tournament_id']),
                    fixture['tournament_name'],
                    fixture['season_year']
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Вставлено {len(data)} fixtures")
            
        except Exception as e:
            print(f"❌ Ошибка вставки fixtures: {e}")
            import traceback
            traceback.print_exc()

    def _insert_match_stats(self, match_stats: List[Dict[str, Any]]):
        """Вставляет статистику матча в ClickHouse"""
        try:
            if not match_stats:
                return
            
            query = """
            INSERT INTO football_match_stats (
                match_id, team_id, team_name, team_type,
                ball_possession, expected_goals, total_shots, shots_on_target, 
                shots_off_target, blocked_shots, corners, free_kicks, fouls, yellow_cards,
                big_chances, big_chances_scored, big_chances_missed, 
                shots_inside_box, shots_outside_box, touches_in_penalty_area,
                total_passes, accurate_passes, pass_accuracy, total_crosses, accurate_crosses,
                total_long_balls, accurate_long_balls, tackles, tackles_won_percent, interceptions,
                recoveries, clearances, errors_lead_to_shot, errors_lead_to_goal,
                duel_won_percent, dispossessed, ground_duels_percentage, aerial_duels_percentage, dribbles_percentage,
                created_at
            ) VALUES
            """
            
            data = []
            for stats in match_stats:
                data.append((
                    int(stats['match_id']),
                    int(stats['team_id']),
                    stats['team_name'],
                    stats['team_type'],
                    float(stats.get('ball_possession', 0)),
                    float(stats.get('expected_goals', 0)),
                    int(stats.get('total_shots', 0)),
                    int(stats.get('shots_on_target', 0)),
                    int(stats.get('shots_off_target', 0)),
                    int(stats.get('blocked_shots', 0)),
                    int(stats.get('corners', 0)),
                    int(stats.get('free_kicks', 0)),
                    int(stats.get('fouls', 0)),
                    int(stats.get('yellow_cards', 0)),
                    int(stats.get('big_chances', 0)),
                    int(stats.get('big_chances_scored', 0)),
                    int(stats.get('big_chances_missed', 0)),
                    int(stats.get('shots_inside_box', 0)),
                    int(stats.get('shots_outside_box', 0)),
                    int(stats.get('touches_in_penalty_area', 0)),
                    int(stats.get('total_passes', 0)),
                    int(stats.get('accurate_passes', 0)),
                    float(stats.get('pass_accuracy', 0)),
                    int(stats.get('total_crosses', 0)),
                    int(stats.get('accurate_crosses', 0)),
                    int(stats.get('total_long_balls', 0)),
                    int(stats.get('accurate_long_balls', 0)),
                    int(stats.get('tackles', 0)),
                    float(stats.get('tackles_won_percent', 0)),
                    int(stats.get('interceptions', 0)),
                    int(stats.get('recoveries', 0)),
                    int(stats.get('clearances', 0)),
                    int(stats.get('errors_lead_to_shot', 0)),
                    int(stats.get('errors_lead_to_goal', 0)),
                    int(stats.get('duel_won_percent', 0)),
                    int(stats.get('dispossessed', 0)),
                    int(stats.get('ground_duels_percentage', 0)),
                    int(stats.get('aerial_duels_percentage', 0)),
                    int(stats.get('dribbles_percentage', 0)),
                    stats['created_at']
                ))
                
            self.ch_client.execute(query, data)
            print(f"✅ Вставлено {len(data)} записей статистики матча")
            
        except Exception as e:
            print(f"❌ Ошибка вставки статистики матча: {e}")

    def _insert_cards(self, incidents: List[Dict[str, Any]]):
        """Вставляет данные о карточках в отдельную таблицу ClickHouse"""
        try:
            card_incidents = [inc for inc in incidents if inc.get('card_type') in ['yellow', 'red', 'yellowRed']]
            
            if not card_incidents:
                return
                
            print(f"🔍 Подготовка {len(card_incidents)} карточек для вставки...")
            
            query = """
            INSERT INTO football_cards (
                match_id, player_id, player_name, team_is_home, card_type, 
                reason, time, added_time, created_at
            ) VALUES
            """
            
            data = []
            for i, incident in enumerate(card_incidents):
                # ОТЛАДКА: что приходит в time и added_time
                original_time = incident.get('time', 0)
                original_added_time = incident.get('added_time', 0)
                
                # Жесткая обработка time
                time_value = 0
                if isinstance(original_time, (int, float)):
                    time_value = int(original_time)
                elif isinstance(original_time, str):
                    # Пробуем извлечь число из строки
                    import re
                    numbers = re.findall(r'\d+', original_time)
                    if numbers:
                        time_value = int(numbers[0])
                    else:
                        time_value = 0
                
                # Жесткая обработка added_time
                added_time = 0
                if isinstance(original_added_time, (int, float)):
                    added_time = int(original_added_time)
                elif isinstance(original_added_time, str):
                    import re
                    numbers = re.findall(r'\d+', original_added_time)
                    if numbers:
                        added_time = int(numbers[0])
                    else:
                        added_time = 0
                
                # Ограничиваем диапазон
                time_value = max(0, min(65535, time_value))
                added_time = max(0, min(65535, added_time))
    
                
                data.append((
                    int(incident['match_id']),
                    int(incident.get('player_id', 0)),
                    incident.get('player_name', ''),
                    int(incident.get('team_is_home', False)),
                    incident.get('card_type', ''),
                    incident.get('reason', ''),
                    int(time_value),      # ← гарантированно число
                    int(added_time),      # ← гарантированно число
                    datetime.now()
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Успешно вставлено {len(data)} карточек")
            
        except Exception as e:
            print(f"❌ Ошибка вставки карточек: {e}")
            import traceback
            traceback.print_exc()
    def _insert_player_stats(self, player_stats: List[Dict[str, Any]]):
        """Вставляет статистику игроков в ClickHouse"""
        try:
            if not player_stats:
                return
            
            query = """
            INSERT INTO football_player_stats (
                match_id, team_id, player_id, player_name, short_name, position,
                jersey_number, minutes_played, rating, goals, goal_assist,
                total_shot, on_target_shot, off_target_shot, blocked_scoring_attempt,
                total_pass, accurate_pass, pass_accuracy, key_pass, total_long_balls, accurate_long_balls,
                successful_dribbles, dribble_success, total_tackle, interception_won,
                total_clearance, outfielder_block, challenge_lost, duel_won, duel_lost,
                aerial_won, duel_success, touches, possession_lost_ctrl, was_fouled,
                fouls, saves, punches, good_high_claim,
                saved_shots_from_inside_box, created_at
            ) VALUES
            """
            
            data = []
            for stats in player_stats:
                data.append((
                    int(stats['match_id']),
                    int(stats['team_id']) if stats['team_id'] is not None else 0,
                    int(stats['player_id']) if stats['player_id'] is not None else 0,
                    stats.get('player_name', ''),
                    stats.get('short_name', ''),
                    stats.get('position', ''),
                    int(stats['jersey_number']) if stats['jersey_number'] is not None else 0,
                    int(stats['minutes_played']) if stats['minutes_played'] is not None else 0,
                    float(stats.get('rating', 0)),
                    int(stats.get('goals', 0)),
                    int(stats.get('goal_assist', 0)),
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
                    int(stats.get('saves', 0)),
                    int(stats.get('punches', 0)),
                    int(stats.get('good_high_claim', 0)),
                    int(stats.get('saved_shots_from_inside_box', 0)),
                    datetime.now()
                ))
                
            self.ch_client.execute(query, data)
            print(f"✅ Вставлено {len(data)} записей статистики игроков")
            
        except Exception as e:
            print(f"❌ Ошибка вставки статистики игроков: {e}")

    async def process_historical_round(self, round_number: int):
        """Обрабатывает исторический тур (матчи + статистика)"""
        print(f"🎯 Обработка исторических данных тура {round_number}")
        
        async with FootballDataCollector() as collector:
            round_data = await collector.collect_round_data(
                tournament_id=self.tournament_id,
                season_id=self.season_id, 
                round_number=round_number
            )
            
            if not round_data or not round_data.get('matches'):
                print(f"❌ Не удалось собрать данные для тура {round_number}")
                return
            
            # Сохраняем основные данные матчей
            match_infos = [match_data['match_info'] for match_data in round_data['matches']]
            self._insert_matches(match_infos)
            
            # Собираем статистику для завершенных матчей
            total_players = 0
            total_cards = 0
            total_match_stats = 0
            
            for match_data in round_data['matches']:
                match_info = match_data['match_info']
                player_stats = match_data['player_stats']
                incidents = match_data['incidents']
                match_stats = match_data['match_stats']
                
                if match_info['status'] == 'Ended':
                    total_players += len(player_stats)
                    total_cards += len(incidents)
                    total_match_stats += len(match_stats)
                    
                    print(f"📊 Матч {match_info['match_id']}: {len(player_stats)} игроков, {len(incidents)} карточек, {len(match_stats)} записей статистики")
                    
                    if player_stats:
                        self._insert_player_stats(player_stats)
                    
                    if incidents:
                        self._insert_cards(incidents)
                    
                    if match_stats:
                        self._insert_match_stats(match_stats)
            
            print(f"🎉 Тур {round_number} обработан: {total_players} игроков, {total_cards} карточек, {total_match_stats} записей статистики")
    def _insert_team_positions_cache(self, positions_data: List[Dict[str, Any]]):
        """Вставляет данные в team_positions_cache"""
        try:
            if not positions_data:
                return
                
            query = """
            INSERT INTO team_positions_cache (
                team_id, tournament_id, season_id, position, points, goal_difference,
                form, matches_played, wins, draws, losses, goals_for, goals_against,
                trend, last_updated_round, updated_at, created_at
            ) VALUES
            """
            
            data = []
            for pos in positions_data:
                data.append((
                    int(pos['team_id']),
                    int(pos['tournament_id']),
                    int(pos['season_id']),
                    int(pos.get('position', 0)),
                    int(pos.get('points', 0)),
                    int(pos.get('goal_difference', 0)),
                    pos.get('form', ''),
                    int(pos.get('matches_played', 0)),
                    int(pos.get('wins', 0)),
                    int(pos.get('draws', 0)),
                    int(pos.get('losses', 0)),
                    int(pos.get('goals_for', 0)),
                    int(pos.get('goals_against', 0)),
                    pos.get('trend', 'stable'),
                    int(pos.get('last_updated_round', 0)),
                    datetime.now(),
                    datetime.now()
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Вставлено {len(data)} записей в team_positions_cache")
            
        except Exception as e:
            print(f"❌ Ошибка вставки в team_positions_cache: {e}")

    def _insert_team_stats_cache(self, stats_data: List[Dict[str, Any]]):
        """Вставляет данные в team_stats_cache"""
        try:
            if not stats_data:
                return
                
            query = """
            INSERT INTO team_stats_cache (
                team_id, tournament_id, season_id, matches_played, goals_scored, goals_conceded,
                avg_possession, avg_shots, avg_shots_on_target, avg_corners, avg_fouls, avg_yellow_cards,
                big_chances, big_chances_missed, goals_inside_box, goals_outside_box, headed_goals,
                pass_accuracy, fast_breaks, updated_at, created_at
            ) VALUES
            """
            
            data = []
            for stats in stats_data:
                data.append((
                    int(stats['team_id']),
                    int(stats['tournament_id']),
                    int(stats['season_id']),
                    int(stats.get('matches_played', 0)),
                    int(stats.get('goals_scored', 0)),
                    int(stats.get('goals_conceded', 0)),
                    float(stats.get('avg_possession', 0)),
                    float(stats.get('avg_shots', 0)),
                    float(stats.get('avg_shots_on_target', 0)),
                    float(stats.get('avg_corners', 0)),
                    float(stats.get('avg_fouls', 0)),
                    float(stats.get('avg_yellow_cards', 0)),
                    int(stats.get('big_chances', 0)),
                    int(stats.get('big_chances_missed', 0)),
                    int(stats.get('goals_inside_box', 0)),
                    int(stats.get('goals_outside_box', 0)),
                    int(stats.get('headed_goals', 0)),
                    float(stats.get('pass_accuracy', 0)),
                    int(stats.get('fast_breaks', 0)),
                    datetime.now(),
                    datetime.now()
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Вставлено {len(data)} записей в team_stats_cache")
            
        except Exception as e:
            print(f"❌ Ошибка вставки в team_stats_cache: {e}")

    async def update_cache_tables(self):
        """Обновляет кэш-таблицы с актуальными данными"""
        print("🔄 Обновление кэш-таблиц...")
        
        async with FootballDataCollector() as collector:
            # Получаем текущий раунд
            current_round = await collector.get_current_round(self.tournament_id, self.season_id)
            print(f"📅 Текущий раунд: {current_round}")
            
            # Получаем турнирную таблицу
            standings_data = await collector.get_tournament_standings(self.tournament_id, self.season_id)
            
            if not standings_data:
                print("❌ Не удалось получить турнирную таблицу")
                return
            
            positions_data = []
            stats_data = []
            
            # Обрабатываем данные турнирной таблицы
            for standing in standings_data.get('standings', []):
                if standing['type'] == 'total':
                    for row in standing.get('rows', []):
                        team = row['team']
                        team_id = team['id']
                        team_name = team['name']
                        
                        # Используем правильные названия полей из API
                        goals_for = row.get('scoresFor', 0)
                        goals_against = row.get('scoresAgainst', 0)
                        goal_difference = goals_for - goals_against  # Вычисляем разницу
                        
                        print(f"📊 {team_name}: {goals_for}-{goals_against} (разница: {goal_difference})")
                        
                        # Подготавливаем данные для positions_cache
                        position_data = {
                            'team_id': team_id,
                            'tournament_id': self.tournament_id,
                            'season_id': self.season_id,
                            'position': row.get('position', 0),
                            'points': row.get('points', 0),
                            'goal_difference': goal_difference,
                            'matches_played': row.get('matches', 0),
                            'wins': row.get('wins', 0),
                            'draws': row.get('draws', 0),
                            'losses': row.get('losses', 0),
                            'goals_for': goals_for,
                            'goals_against': goals_against,
                            'form': row.get('form', ''),
                            'trend': 'stable',
                            'last_updated_round': current_round
                        }
                        positions_data.append(position_data)
                        
                        # Получаем детальную статистику команды
                        team_stats = await collector.get_team_season_stats(team_id, self.tournament_id, self.season_id)
                        if team_stats:
                            stats_data.append({
                                'team_id': team_id,
                                'tournament_id': self.tournament_id,
                                'season_id': self.season_id,
                                'matches_played': team_stats.get('matches', 0),
                                'goals_scored': team_stats.get('goalsScored', 0),
                                'goals_conceded': team_stats.get('goalsConceded', 0),
                                'avg_possession': team_stats.get('averageBallPossession', 0),
                                'avg_shots': team_stats.get('shots', 0),
                                'avg_shots_on_target': team_stats.get('shotsOnTarget', 0),
                                'avg_corners': team_stats.get('corners', 0),
                                'avg_fouls': team_stats.get('fouls', 0),
                                'avg_yellow_cards': team_stats.get('yellowCards', 0),
                                'big_chances': team_stats.get('bigChances', 0),
                                'big_chances_missed': team_stats.get('bigChancesMissed', 0),
                                'goals_inside_box': team_stats.get('goalsFromInsideTheBox', 0),
                                'goals_outside_box': team_stats.get('goalsFromOutsideTheBox', 0),
                                'headed_goals': team_stats.get('headedGoals', 0),
                                'pass_accuracy': team_stats.get('accuratePassesPercentage', 0),
                                'fast_breaks': team_stats.get('fastBreaks', 0)
                            })
                            print(f"✅ Статистика для {team_name} получена")
                        else:
                            print(f"⚠️ Статистика для {team_name} не получена")
                        
                        await asyncio.sleep(0.5)
            
            # Вставляем данные в кэш-таблицы
            if positions_data:
                self._insert_team_positions_cache(positions_data)
            
            if stats_data:
                self._insert_team_stats_cache(stats_data)
            
            print(f"🎉 Кэш-таблицы обновлены: {len(positions_data)} позиций, {len(stats_data)} статистик")
    async def process_upcoming_fixtures(self, round_number: int):
        """Обрабатывает предстоящие матчи и заполняет fixtures"""
        print(f"🔮 Обработка fixtures для тура {round_number}")
        
        async with FootballDataCollector() as collector:
            # Получаем матчи предстоящего тура
            upcoming_matches = await collector.get_round_matches(
                tournament_id=self.tournament_id,
                season_id=self.season_id, 
                round_number=round_number
            )
            
            if not upcoming_matches:
                print(f"❌ Не удалось получить матчи для тура {round_number}")
                return
            
            fixtures_data = []
            
            # Для каждого матча получаем детальную информацию для fixtures
            for match in upcoming_matches:
                match_id = match['match_id']
                
                try:
                    # Получаем детальную информацию о матче
                    match_details = await collector.get_match_details(match_id)
                    if match_details and 'event' in match_details:
                        fixture = collector._prepare_fixture_from_match(match, match_details['event'])
                        fixtures_data.append(fixture)
                        print(f"✅ Подготовлен fixture для матча {match_id}")
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f"❌ Ошибка обработки матча {match_id}: {e}")
                    continue
            
            # Вставляем данные в таблицу fixtures
            if fixtures_data:
                self._insert_match_fixtures(fixtures_data)
                print(f"🎉 Fixtures обновлены: {len(fixtures_data)} матчей тура {round_number}")

def check_database_state(host, user, password, database):
    """Проверяет состояние базы данных"""
    ch_client = Client(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    try:
        matches_count = ch_client.execute("SELECT COUNT(*) FROM football_matches")[0][0]
        stats_count = ch_client.execute("SELECT COUNT(*) FROM football_player_stats")[0][0]
        cards_count = ch_client.execute("SELECT COUNT(*) FROM football_cards")[0][0]
        fixtures_count = ch_client.execute("SELECT COUNT(*) FROM match_fixtures")[0][0]
        
        print(f"📊 Матчей: {matches_count}, Статистики: {stats_count}, Карточек: {cards_count}, Fixtures: {fixtures_count}")
        
        return matches_count > 0
        
    except Exception as e:
        print(f"❌ Ошибка проверки базы данных: {e}")
        return False

async def main():
    parser = argparse.ArgumentParser(description='Football Data Orchestrator')
    parser.add_argument('--round', type=int, required=False, help='Round number to process')
    parser.add_argument('--tournament', required=True, type=int, help='ID турнира')
    parser.add_argument('--season', required=True, type=int, help='ID сезона')
    parser.add_argument('--host', default=os.getenv('CLICKHOUSE_HOST', 'clickhouse-server'), help='ClickHouse host')
    parser.add_argument('--user', default=os.getenv('CLICKHOUSE_USER', 'username'), help='ClickHouse user')
    parser.add_argument('--password', default=os.getenv('CLICKHOUSE_PASSWORD', ''), help='ClickHouse password')
    parser.add_argument('--database', default=os.getenv('CLICKHOUSE_DB', 'football_db'), help='ClickHouse database name')
    parser.add_argument('--port', type=int, default=os.getenv('CLICKHOUSE_PORT', 9000), help='ClickHouse port')
    
    # Новые аргументы для выбора операций
    parser.add_argument('--historical', action='store_true', help='Обработать исторические данные тура')
    parser.add_argument('--fixtures', action='store_true', help='Загрузить fixtures для следующего тура')
    parser.add_argument('--cache', action='store_true', help='Обновить кэш-таблицы')
    parser.add_argument('--all', action='store_true', help='Выполнить все операции (historical + fixtures + cache)')
        
    args = parser.parse_args()
        
    print(f"🚀 Запуск обработки в {datetime.now()}")
    print(f"📊 Подключение к БД: {args.host}:{args.port}/{args.database}")
    
    # Проверяем состояние базы
    print("\n🔍 Проверяем текущее состояние базы данных...")
    has_data = check_database_state(args.host, args.user, args.password, args.database)
    
    orchestrator = FootballDataOrchestrator(
        ch_host=args.host,
        ch_user=args.user,
        ch_password=args.password,
        ch_database=args.database,
        tournament_id=args.tournament,
        season_id=args.season
    )
        
    try:
        if not args.round:
            print("❌ Укажите --round для обработки данных")
            return
        
        # Определяем какие операции выполнять
        run_historical = args.historical or args.all
        run_fixtures = args.fixtures or args.all  
        run_cache = args.cache or args.all
        
        print(f"\n🎯 Операции для тура {args.round}:")
        print(f"   📊 Исторические данные: {'✅' if run_historical else '❌'}")
        print(f"   🔮 Fixtures: {'✅' if run_fixtures else '❌'}")
        print(f"   💾 Кэш-таблицы: {'✅' if run_cache else '❌'}")
        
        if run_historical:
            print(f"\n🎯 Обработка исторических данных тура {args.round}...")
            await orchestrator.process_historical_round(args.round)
        
        if run_fixtures:
            next_round = args.round + 1
            print(f"\n🔮 Подготовка fixtures для тура {next_round}...")
            await orchestrator.process_upcoming_fixtures(next_round)
        
        if run_cache:
            print(f"\n🔄 Обновление кэш-таблиц...")
            await orchestrator.update_cache_tables()
        
        print(f"\n🎉 Обработка завершена!")
        
        # Проверяем результат
        print("\n🔍 Проверяем результат после обработки...")
        check_database_state(args.host, args.user, args.password, args.database)
        
        print(f"\n✅ Все операции завершены успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка обработки: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())