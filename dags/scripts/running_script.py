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
                print("❌ Нет данных матчей для вставки")
                return
                
            print(f"🔍 Подготовка {len(matches)} матчей для вставки...")
            
            query = """
            INSERT INTO football_matches (
                match_id, tournament_id, season_id, round_number, match_date,
                home_team_id, home_team_name, away_team_id, away_team_name,
                home_score, away_score, status, venue, start_timestamp, created_at
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
                    match['venue'],
                    match['start_timestamp'],
                    datetime.now()
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Успешно вставлено {len(data)} матчей")
            
        except Exception as e:
            print(f"❌ Ошибка вставки матчей: {e}")
            import traceback
            traceback.print_exc()

    def _insert_match_stats(self, match_stats: List[Dict[str, Any]]):
        """Вставляет статистику матча в ClickHouse"""
        try:
            if not match_stats:
                print("❌ Нет данных статистики матча для вставки")
                return
            
            print(f"📊 Подготовка {len(match_stats)} записей статистики матча...")
            
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
                try:
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
                except Exception as e:
                    print(f"  ❌ Ошибка подготовки статистики: {e}")
                    print(f"     Данные: {stats}")
                    continue
            
            if not data:
                print("❌ Нет данных для вставки статистики")
                return
                
            print(f"🔍 Выполняем запрос к ClickHouse для {len(data)} записей...")
            self.ch_client.execute(query, data)
            print(f"✅ Успешно вставлено {len(data)} записей статистики матча")
            
        except Exception as e:
            print(f"❌ Ошибка вставки статистики матча: {e}")
            import traceback
            traceback.print_exc()
            
    def _insert_cards(self, incidents: List[Dict[str, Any]]):
        """Вставляет данные о карточках в отдельную таблицу ClickHouse"""
        try:
            # Фильтруем только карточки
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
            for incident in card_incidents:
                data.append((
                    int(incident['match_id']),
                    int(incident.get('player_id', 0)),
                    incident.get('player_name', ''),
                    int(incident.get('team_is_home', False)),
                    incident.get('card_type', ''),
                    incident.get('reason', ''),
                    int(incident.get('time', 0)),
                    int(incident.get('added_time', 0)),
                    datetime.now()
                ))
            
            self.ch_client.execute(query, data)
            print(f"✅ Успешно вставлено {len(data)} карточек")
            
        except Exception as e:
            print(f"❌ Ошибка вставки карточек: {e}")
    
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
                fouls, saves, punches, good_high_claim,
                saved_shots_from_inside_box, created_at
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
                        int(stats.get('saves', 0)),
                        int(stats.get('punches', 0)),
                        int(stats.get('good_high_claim', 0)),
                        int(stats.get('saved_shots_from_inside_box', 0)),
                        datetime.now()
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

    async def update_team_stats_cache(self, tournament_id: int, season_id: int):
        """Обновляет кэш статистики команд из API"""
        try:
            print(f"🔄 Обновление кэша статистики команд из API...")
            
            async with FootballDataCollector() as collector:
                # Получаем все команды турнира
                teams = self._get_tournament_teams(tournament_id, season_id)
                
                if not teams:
                    print("❌ Не найдено команд для обновления кэша")
                    return
                
                print(f"📊 Найдено {len(teams)} команд для обновления кэша")
                
                updated_count = 0
                for team_id, team_name in teams.items():
                    try:
                        print(f"🔍 Запрашиваем статистику команды {team_name} (ID: {team_id})")
                        stats = await collector.get_team_season_stats(team_id, tournament_id, season_id)
                        
                        if stats:
                            # ДЕБАГ: выводим полученные данные
                            print(f"📋 Получена статистика для {team_name}:")
                            print(f"   Матчи: {stats.get('matches', 0)}")
                            print(f"   Голы: {stats.get('goalsScored', 0)} забито, {stats.get('goalsConceded', 0)} пропущено")
                            print(f"   Владение: {stats.get('averageBallPossession', 0)}%")
                            print(f"   Удары: {stats.get('shots', 0)} всего, {stats.get('shotsOnTarget', 0)} в створ")
                            print(f"   xG: {stats.get('expectedGoals', 0)}")
                            
                            query = """
                            INSERT INTO team_stats_cache (
                                team_id, tournament_id, season_id, matches_played, goals_scored, goals_conceded,
                                avg_possession, avg_shots, avg_shots_on_target, avg_xg, avg_corners, 
                                avg_fouls, avg_yellow_cards, big_chances, big_chances_missed,
                                goals_inside_box, goals_outside_box, headed_goals, pass_accuracy, fast_breaks,
                                updated_at
                            ) VALUES (
                                %(team_id)s, %(tournament_id)s, %(season_id)s, %(matches)s, %(goals_scored)s, %(goals_conceded)s,
                                %(avg_possession)s, %(shots)s, %(shots_on_target)s, %(xg)s, %(corners)s,
                                %(fouls)s, %(yellow_cards)s, %(big_chances)s, %(big_chances_missed)s,
                                %(goals_inside_box)s, %(goals_outside_box)s, %(headed_goals)s, %(pass_accuracy)s, %(fast_breaks)s,
                                NOW()
                            )
                            """
                            
                            self.ch_client.execute(query, {
                                'team_id': team_id,
                                'tournament_id': tournament_id,
                                'season_id': season_id,
                                'matches': stats.get('matches', 0),
                                'goals_scored': stats.get('goalsScored', 0),
                                'goals_conceded': stats.get('goalsConceded', 0),
                                'avg_possession': stats.get('averageBallPossession', 0),
                                'shots': stats.get('shots', 0),
                                'shots_on_target': stats.get('shotsOnTarget', 0),
                                'xg': stats.get('expectedGoals', 0),
                                'corners': stats.get('corners', 0),
                                'fouls': stats.get('fouls', 0),
                                'yellow_cards': stats.get('yellowCards', 0),
                                'big_chances': stats.get('bigChances', 0),
                                'big_chances_missed': stats.get('bigChancesMissed', 0),
                                'goals_inside_box': stats.get('goalsFromInsideTheBox', 0),
                                'goals_outside_box': stats.get('goalsFromOutsideTheBox', 0),
                                'headed_goals': stats.get('headedGoals', 0),
                                'pass_accuracy': stats.get('accuratePassesPercentage', 0),
                                'fast_breaks': stats.get('fastBreaks', 0)
                            })
                            
                            updated_count += 1
                            print(f"✅ Статистика команды {team_name} обновлена в кэше")
                        else:
                            print(f"⚠️  Не удалось получить статистику для команды {team_name}")
                        
                        # Увеличиваем задержку между запросами
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        print(f"❌ Ошибка обработки команды {team_id} ({team_name}): {e}")
                        continue
                
                print(f"✅ Кэш статистики команд обновлен. Обработано: {updated_count}/{len(teams)} команд")
                
        except Exception as e:
            print(f"❌ Критическая ошибка обновления кэша статистики: {e}")
            import traceback
            traceback.print_exc()

    async def update_team_positions_cache(self, tournament_id: int, season_id: int):
        """Обновляет кэш позиций команд из API"""
        try:
            print(f"🔄 Обновление кэша позиций команд из API...")
            
            async with FootballDataCollector() as collector:
                # Получаем турнирную таблицу
                standings_data = await collector.get_tournament_standings(tournament_id, season_id)
                
                if not standings_data or 'standings' not in standings_data:
                    print("❌ Нет данных standings")
                    return
                
                updated_count = 0
                for standing_type in standings_data['standings']:
                    if standing_type['type'] == 'total':  # Общая таблица
                        for row in standing_type['rows']:
                            try:
                                team = row['team']
                                team_id = team['id']
                                team_name = team['name']
                                
                                # Получаем историческую динамику позиций
                                performance_data = await collector.get_team_performance_graph(team_id, tournament_id, season_id)
                                
                                # Анализируем тренд на основе исторических данных
                                trend = self._analyze_position_trend(performance_data, row['position'])
                                
                                # Получаем форму команды из последних матчей
                                team_form = self._extract_recent_form(performance_data, team_id)
                                
                                query = """
                                INSERT INTO team_positions_cache (
                                    team_id, tournament_id, season_id, position, points, goal_difference,
                                    matches_played, wins, draws, losses, goals_for, goals_against,
                                    form, trend, last_updated_round, updated_at
                                ) VALUES (
                                    %(team_id)s, %(tournament_id)s, %(season_id)s, %(position)s, %(points)s, %(goal_difference)s,
                                    %(matches_played)s, %(wins)s, %(draws)s, %(losses)s, %(goals_for)s, %(goals_against)s,
                                    %(form)s, %(trend)s, %(last_updated_round)s, NOW()
                                )
                                """
                                
                                # Парсим разницу мячей из формата "+12"
                                goal_diff_str = row.get('scoreDiffFormatted', '0')
                                try:
                                    if goal_diff_str.startswith('+'):
                                        goal_diff = int(goal_diff_str[1:])
                                    elif goal_diff_str.startswith('-'):
                                        goal_diff = int(goal_diff_str)
                                    else:
                                        goal_diff = int(goal_diff_str)
                                except:
                                    goal_diff = 0
                                
                                self.ch_client.execute(query, {
                                    'team_id': team_id,
                                    'tournament_id': tournament_id,
                                    'season_id': season_id,
                                    'position': row['position'],
                                    'points': row['points'],
                                    'goal_difference': goal_diff,
                                    'matches_played': row['matches'],
                                    'wins': row['wins'],
                                    'draws': row['draws'],
                                    'losses': row['losses'],
                                    'goals_for': row['scoresFor'],
                                    'goals_against': row['scoresAgainst'],
                                    'form': team_form,
                                    'trend': trend,
                                    'last_updated_round': self._get_current_round_from_performance(performance_data)
                                })
                                
                                updated_count += 1
                                print(f"✅ Позиция команды {team_name}: {row['position']} место ({trend})")
                                
                            except Exception as e:
                                print(f"❌ Ошибка обработки команды {team_name}: {e}")
                                continue
                    
                    print(f"✅ Кэш позиций команд обновлен. Обработано: {updated_count} команд")
                    
        except Exception as e:
            print(f"❌ Ошибка обновления кэша позиций: {e}")
    def _analyze_position_trend(self, performance_data: Dict, current_position: int) -> str:
        """Анализирует тренд позиции на основе исторических данных"""
        if not performance_data or 'graphData' not in performance_data:
            return "stable"
        
        graph_data = performance_data['graphData']
        if len(graph_data) < 2:
            return "stable"
        
        # Берем последние 3 позиции для анализа
        recent_positions = [point['position'] for point in graph_data[-3:]]
        
        if len(recent_positions) >= 2:
            previous_position = recent_positions[-2]
            if current_position < previous_position:
                return "up"  # улучшение
            elif current_position > previous_position:
                return "down"  # ухудшение
        
        return "stable"

    def _extract_recent_form(self, performance_data: Dict, team_id: int) -> str:
        """Извлекает форму команды из последних матчей"""
        if not performance_data or 'graphData' not in performance_data:
            return ""
        
        form_results = []
        graph_data = performance_data['graphData']
        
        # Берем последние 5 недель для формы
        for week_data in graph_data[-5:]:
            for event in week_data.get('events', []):
                try:
                    # ПРОВЕРЯЕМ наличие всех необходимых полей
                    if ('homeTeam' not in event or 'awayTeam' not in event or 
                        'homeScore' not in event or 'awayScore' not in event):
                        continue
                        
                    home_score = event['homeScore'].get('current')
                    away_score = event['awayScore'].get('current')
                    
                    # Пропускаем если нет данных о счете
                    if home_score is None or away_score is None:
                        continue
                    
                    if event['homeTeam']['id'] == team_id:
                        # Команда играла дома
                        if home_score > away_score:
                            form_results.append('W')
                        elif home_score < away_score:
                            form_results.append('L')
                        else:
                            form_results.append('D')
                    elif event['awayTeam']['id'] == team_id:
                        # Команда играла в гостях
                        if away_score > home_score:
                            form_results.append('W')
                        elif away_score < home_score:
                            form_results.append('L')
                        else:
                            form_results.append('D')
                except Exception as e:
                    print(f"⚠️  Ошибка обработки события формы: {e}")
                    continue
        
        # Берем последние 5 результатов
        return ''.join(form_results[-5:])

    def _get_current_round_from_performance(self, performance_data: Dict) -> int:
        """Определяет текущий тур на основе performance данных"""
        if not performance_data or 'graphData' not in performance_data:
            return 0
        
        graph_data = performance_data['graphData']
        if not graph_data:
            return 0
        
        return graph_data[-1].get('week', 0)

    def _get_tournament_teams(self, tournament_id: int, season_id: int) -> Dict[int, str]:
        """Получает список команд турнира"""
        try:
            query = """
            SELECT DISTINCT team_id, team_name 
            FROM (
                SELECT home_team_id as team_id, home_team_name as team_name
                FROM football_matches 
                WHERE tournament_id = %(tournament_id)s AND season_id = %(season_id)s
                UNION ALL  -- ИСПРАВЛЕНО: добавлено ALL
                SELECT away_team_id as team_id, away_team_name as team_name  
                FROM football_matches
                WHERE tournament_id = %(tournament_id)s AND season_id = %(season_id)s
            )
            """
            results = self.ch_client.execute(query, {
                'tournament_id': tournament_id, 
                'season_id': season_id
            })
            teams = {team_id: team_name for team_id, team_name in results}
            print(f"📋 Найдено {len(teams)} команд в турнире {tournament_id} сезона {season_id}")
            return teams
        except Exception as e:
            print(f"❌ Ошибка получения списка команд: {e}")
            return {}

    async def process_round(self, round_number: int):
        """Обрабатывает тур с полным сбором данных"""
        print(f"🎯 Запуск сбора данных для тура {round_number}")
        
        async with FootballDataCollector() as collector:
            round_data = await collector.collect_round_data(
                tournament_id=self.tournament_id,
                season_id=self.season_id, 
                round_number=round_number
            )
            
            if not round_data or not round_data.get('matches'):
                print(f"❌ Не удалось собрать данные для тура {round_number}")
                return
            
            total_players = 0
            total_cards = 0
            total_match_stats = 0
            
            for match_data in round_data['matches']:
                match_info = match_data['match_info']
                player_stats = match_data['player_stats']
                incidents = match_data['incidents']
                match_stats = match_data['match_stats']
                
                total_players += len(player_stats)
                total_cards += len(incidents)
                total_match_stats += len(match_stats)
                
                print(f"📊 Матч {match_info['match_id']}: {len(player_stats)} игроков, {len(incidents)} карточек, {len(match_stats)} записей статистики")
                
                # Сохраняем данные в ClickHouse
                if player_stats:
                    self._insert_player_stats(player_stats)
                    print(f"💾 Сохранено {len(player_stats)} записей статистики игроков")
                
                if incidents:
                    self._insert_cards(incidents)
                    print(f"🟨 Сохранено {len(incidents)} карточек")
                
                if match_stats:
                    self._insert_match_stats(match_stats)
                    print(f"📈 Сохранено {len(match_stats)} записей статистики матча")
                
                # Вставляем матчи (если еще не вставлены)
                self._insert_matches([match_info])
                print(f"🏟️  Сохранен матч {match_info['match_id']}")
            
            # ОБНОВЛЯЕМ КЭШ-ТАБЛИЦЫ ПОСЛЕ СБОРА ДАННЫХ
            print(f"🔄 Запуск обновления кэша статистики команд...")
            await self.update_team_stats_cache(self.tournament_id, self.season_id)
            await self.update_team_positions_cache(self.tournament_id, self.season_id)
            
            print(f"🎉 Тур {round_number} обработан: {total_players} игроков, {total_cards} карточек, {total_match_stats} записей статистики")

# Функция для проверки состояния базы данных
def check_database_state(host, user, password, database):
    """Проверяет состояние базы данных"""
    ch_client = Client(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    try:
        # Проверяем матчи
        matches_count = ch_client.execute("SELECT COUNT(*) FROM football_matches")[0][0]
        print(f"📊 Матчей в базе: {matches_count}")
        
        # Проверяем статистику игроков
        stats_count = ch_client.execute("SELECT COUNT(*) FROM football_player_stats")[0][0]
        print(f"👥 Записей статистики: {stats_count}")
        
        # Проверяем карточки
        cards_count = ch_client.execute("SELECT COUNT(*) FROM football_cards")[0][0]
        print(f"🟨 Карточек в базе: {cards_count}")
        
        # Проверяем кэш статистики
        stats_cache_count = ch_client.execute("SELECT COUNT(*) FROM team_stats_cache")[0][0]
        print(f"📈 Записей в кэше статистики: {stats_cache_count}")
        
        # Проверяем кэш позиций
        positions_cache_count = ch_client.execute("SELECT COUNT(*) FROM team_positions_cache")[0][0]
        print(f"🏆 Записей в кэше позиций: {positions_cache_count}")
        
        # Показываем примеры матчей
        if matches_count > 0:
            sample_matches = ch_client.execute("""
                SELECT match_id, home_team_name, away_team_name, home_score, away_score 
                FROM football_matches 
                ORDER BY match_date DESC 
                LIMIT 5
            """)
            print("\n📅 Последние матчи в базе:")
            for match in sample_matches:
                print(f"   {match[0]}: {match[1]} {match[3]}-{match[4]} {match[2]}")
        
        return matches_count > 0
        
    except Exception as e:
        print(f"❌ Ошибка проверки базы данных: {e}")
        return False

async def main():
    parser = argparse.ArgumentParser(description='Football Data Orchestrator')
    parser.add_argument('--round', type=int, required=True, help='Round number to process')
    parser.add_argument('--tournament', required=True, type=int, help='ID турнира')
    parser.add_argument('--season', required=True, type=int, help='ID сезона')
    parser.add_argument('--host', default=os.getenv('CLICKHOUSE_HOST', 'clickhouse-server'), help='ClickHouse host')
    parser.add_argument('--user', default=os.getenv('CLICKHOUSE_USER', 'username'), help='ClickHouse user')
    parser.add_argument('--password', default=os.getenv('CLICKHOUSE_PASSWORD', ''), help='ClickHouse password')
    parser.add_argument('--database', default=os.getenv('CLICKHOUSE_DB', 'football_db'), help='ClickHouse database name')
    parser.add_argument('--port', type=int, default=os.getenv('CLICKHOUSE_PORT', 9000), help='ClickHouse port')
        
    args = parser.parse_args()
        
    print(f"🚀 Запуск обработки тура {args.round} в {datetime.now()}")
    print(f"📊 Подключение к БД: {args.host}:{args.port}/{args.database}")
    
    # Сначала проверяем состояние базы
    print("\n🔍 Проверяем текущее состояние базы данных...")
    has_data = check_database_state(args.host, args.user, args.password, args.database)
    
    if has_data:
        print("✅ В базе уже есть данные")
    else:
        print("🆕 База пустая, будем заполнять данными")
        
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
        
        # Проверяем результат
        print("\n🔍 Проверяем результат после обработки...")
        check_database_state(args.host, args.user, args.password, args.database)
        
    except Exception as e:
        print(f"❌ Ошибка обработки тура {args.round}: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())