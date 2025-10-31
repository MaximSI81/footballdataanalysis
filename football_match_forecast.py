import pandas as pd
from clickhouse_driver import Client
import os
import asyncio
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta

class AdvancedFootballAnalyzer:
    def __init__(self):
        self.ch_client = None

    async def __aenter__(self):
        self.ch_client = Client(
            host='localhost',
            user='username', 
            password='password',
            database='football_db'
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.ch_client:
            self.ch_client.disconnect()

    # Словарь дерби
    DERBY_PAIRS = {
        # 🇷🇺 Российская Премьер-Лига
        (2315, 2323): "Московское дерби: Динамо Москва-Спартак Москва",
        (2320, 2323): "Московское дерби: Локомотив Москва-Спартак Москва", 
        (2325, 2323): "Московское дерби: ЦСКА Москва-Спартак Москва",
        (2315, 2320): "Московское дерби: Динамо Москва-Локомотив Москва",
        (2315, 2325): "Московское дерби: Динамо Москва-ЦСКА Москва",
        (2320, 2325): "Московское дерби: Локомотив Москва-ЦСКА Москва",
        (2321, 2323): "Противостояние: Зенит Санкт-Петербург-Спартак Москва",
        (2321, 2325): "Противостояние: Зенит Санкт-Петербург-ЦСКА Москва",
        (2321, 2315): "Противостояние: Зенит Санкт-Петербург-Динамо Москва",
        (34425, 2326): "Южное дерби: Краснодар-Ростов",
        (34425, 2321): "Противостояние: Краснодар-Зенит Санкт-Петербург",
        (2326, 2321): "Противостояние: Ростов-Зенит Санкт-Петербург",
        
        # 🏴󠁧󠁢󠁥󠁮󠁧󠁿 Английская Премьер-Лига
        (44, 35): "Северо-западное дерби: Ливерпуль-Манчестер Юнайтед",
        (42, 35): "Противостояние: Арсенал-Манчестер Юнайтед",
        (42, 44): "Противостояние: Арсенал-Ливерпуль",
        (38, 42): "Лондонское дерби: Челси-Арсенал",
        (38, 37): "Лондонское дерби: Челси-Вест Хэм Юнайтед",
        (42, 37): "Лондонское дерби: Арсенал-Вест Хэм Юнайтед",
        (35, 17): "Манчестерское дерби: Манчестер Юнайтед-Манчестер Сити",
        (33, 38): "Противостояние: Тоттенхэм Хотспур-Челси",
        (33, 42): "Северо-Лондонское дерби: Тоттенхэм Хотспур-Арсенал",
        
        # 🇩🇪 Бундеслига
        (2672, 2673): "Дерби Германии: Бавария Мюнхен-Боруссия Дортмунд",
        (2672, 2681): "Южное дерби: Бавария Мюнхен-Байер Леверкузен",
        (2673, 2681): "Рейнское дерби: Боруссия Дортмунд-Байер Леверкузен",
        (2674, 2672): "Южное дерби: Айнтрахт Франкфурт-Бавария Мюнхен",
        (2527, 2673): "Рейнское дерби: Боруссия Мёнхенгладбах-Боруссия Дортмунд",
        
        # 🇫🇷 Лига 1
        (1644, 1641): "Le Classique: Пари Сен-Жермен-Марсель",
        (1644, 1649): "Противостояние: Пари Сен-Жермен-Лион",
        (1641, 1649): "Средиземноморское дерби: Марсель-Лион",
        (1644, 1653): "Богатое дерби: Пари Сен-Жермен-Монако",
        (1641, 1653): "Прибрежное дерби: Марсель-Монако",
        
        # 🇮🇹 Серия А
        (2697, 2692): "Дерби Италии: Интер-Милан",
        (2702, 2692): "Столичное дерби: Рома-Милан",
        (2702, 2697): "Миланское дерби: Рома-Интер",
        (2714, 2692): "Южное дерби: Наполи-Милан",
        (2714, 2697): "Противостояние: Наполи-Интер",
        (2687, 2692): "Туринское дерби: Ювентус-Милан",
        (2687, 2697): "Противостояние: Ювентус-Интер",
        (2699, 2702): "Римское дерби: Лацио-Рома",
        
        # 🇪🇸 Ла Лига
        (2817, 2829): "Эль Класико: Барселона-Реал Мадрид",
        (2836, 2829): "Мадридское дерби: Атлетико Мадрид-Реал Мадрид",
        (2836, 2817): "Противостояние: Атлетико Мадрид-Барселона",
        (2828, 2817): "Средиземноморское дерби: Валенсия-Барселона",
        (2828, 2829): "Противостояние: Валенсия-Реал Мадрид",
        (2819, 2817): "Противостояние: Вильярреал-Барселона"
    }

    # Существующие методы получения статистики (без изменений)
    def get_team_stats_from_db(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику команды из кэш-таблицы"""
        try:
            query = """
            SELECT 
                matches_played, goals_scored, goals_conceded, avg_possession,
                avg_shots, avg_shots_on_target, avg_corners,
                avg_fouls, avg_yellow_cards, big_chances, big_chances_missed,
                goals_inside_box, goals_outside_box, headed_goals, pass_accuracy, fast_breaks
            FROM team_stats_cache 
            WHERE team_id = %(team_id)s 
            AND tournament_id = %(tournament_id)s 
            AND season_id = %(season_id)s
            ORDER BY updated_at DESC 
            LIMIT 1
            """
            
            result = self.ch_client.execute(query, {
                'team_id': team_id,
                'tournament_id': tournament_id,
                'season_id': season_id
            })
            
            if result:
                stats = {
                    'matches': result[0][0],
                    'goalsScored': result[0][1],
                    'goalsConceded': result[0][2],
                    'averageBallPossession': result[0][3],
                    'shots': result[0][4],
                    'shotsOnTarget': result[0][5],
                    'corners': result[0][6],
                    'fouls': result[0][7],
                    'yellowCards': result[0][8],
                    'bigChances': result[0][9],
                    'bigChancesMissed': result[0][10],
                    'goalsFromInsideTheBox': result[0][11],
                    'goalsFromOutsideTheBox': result[0][12],
                    'headedGoals': result[0][13],
                    'accuratePassesPercentage': result[0][14],
                    'fastBreaks': result[0][15]
                }
                return stats
            return {}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики команды {team_id} из БД: {e}")
            return {}

    def get_team_position_from_db(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает позицию и динамику команды из кэш-таблицы"""
        try:
            query = """
            SELECT 
                position, points, goal_difference, matches_played,
                wins, draws, losses, goals_for, goals_against,
                form, trend, last_updated_round
            FROM team_positions_cache 
            WHERE team_id = %(team_id)s 
            AND tournament_id = %(tournament_id)s 
            AND season_id = %(season_id)s
            ORDER BY updated_at DESC 
            LIMIT 1
            """
            
            result = self.ch_client.execute(query, {
                'team_id': team_id,
                'tournament_id': tournament_id,
                'season_id': season_id
            })
            
            if result:
                return {
                    'position': result[0][0],
                    'points': result[0][1],
                    'goal_difference': result[0][2],
                    'matches_played': result[0][3],
                    'wins': result[0][4],
                    'draws': result[0][5],
                    'losses': result[0][6],
                    'goals_for': result[0][7],
                    'goals_against': result[0][8],
                    'form': result[0][9],
                    'trend': result[0][10],
                    'last_updated_round': result[0][11]
                }
            return {}
            
        except Exception as e:
            print(f"❌ Ошибка получения позиции команды {team_id}: {e}")
            return {}

    def get_current_standings(self, tournament_id: int, season_id: int) -> Dict[int, int]:
        """Получает текущую таблицу турнира из кэш-таблицы"""
        try:
            query = """
            SELECT team_id, position
            FROM team_positions_cache 
            WHERE tournament_id = %(tournament_id)s 
            AND season_id = %(season_id)s
            ORDER BY position
            """
            
            results = self.ch_client.execute(query, {
                'tournament_id': tournament_id,
                'season_id': season_id
            })
            
            standings = {}
            for team_id, position in results:
                standings[team_id] = position
                
            return standings
            
        except Exception as e:
            print(f"❌ Ошибка получения таблицы из БД: {e}")
            return {}

    def get_team_xg_from_db(self, team1_id: int, team2_id: int, season_id: int) -> Tuple[float, float]:
        """Получает xG статистику команд из football_match_stats"""
        try:
            query = """
            SELECT 
                team_id,
                AVG(expected_goals) as avg_xg
            FROM football_match_stats
            WHERE team_id IN (%(team1)s, %(team2)s)
            AND match_id IN (
                SELECT match_id FROM football_matches 
                WHERE season_id = %(season_id)s 
                AND status = 'Ended'
            )
            AND expected_goals IS NOT NULL
            AND expected_goals > 0
            GROUP BY team_id
            """
            
            results = self.ch_client.execute(query, {
                'team1': team1_id, 
                'team2': team2_id, 
                'season_id': season_id
            })
            
            team1_xg = 0.0
            team2_xg = 0.0
            
            for team_id, avg_xg in results:
                if team_id == team1_id:
                    team1_xg = avg_xg or 0.0
                elif team_id == team2_id:
                    team2_xg = avg_xg or 0.0
                    
            return team1_xg, team2_xg
            
        except Exception as e:
            print(f"❌ Ошибка получения xG из БД: {e}")
            return 0.0, 0.0

    def safe_divide(self, numerator, denominator, default=0.0):
        """Безопасное деление с обработкой нулей"""
        if denominator and denominator > 0:
            return numerator / denominator
        return default

    def analyze_position_trend(self, position_data: Dict) -> Tuple[str, str]:
        """Анализирует тренд позиции команды"""
        if not position_data:
            return "N/A", ""
            
        current_position = position_data.get('position', 0)
        trend = position_data.get('trend', 'stable')
        
        trend_icons = {
            'up': '🟢 улучшение',
            'down': '🔴 ухудшение', 
            'stable': '🟡 стабильно'
        }
        
        return str(current_position), trend_icons.get(trend, '🟡 стабильно')

    def get_team_crosses_longballs_from_db(self, team1_id: int, team2_id: int, season_id: int) -> Dict[str, Any]:
        """Получает данные о кроссах и длинных передачах"""
        try:
            query = """
            SELECT 
                team_id,
                AVG(total_crosses) as avg_total_crosses,
                AVG(accurate_crosses) as avg_accurate_crosses,
                AVG(total_long_balls) as avg_total_long_balls,
                AVG(accurate_long_balls) as avg_accurate_long_balls
            FROM football_match_stats 
            WHERE team_id IN (%(team1)s, %(team2)s)
            AND match_id IN (
                SELECT match_id FROM football_matches 
                WHERE season_id = %(season_id)s AND status = 'Ended'
            )
            GROUP BY team_id
            """
            
            results = self.ch_client.execute(query, {
                'team1': team1_id,
                'team2': team2_id, 
                'season_id': season_id
            })
            
            stats = {}
            for team_id, avg_crosses, avg_accurate_crosses, avg_long_balls, avg_accurate_long_balls in results:
                stats[team_id] = {
                    'total_crosses': avg_crosses or 0,
                    'accurate_crosses': avg_accurate_crosses or 0,
                    'total_long_balls': avg_long_balls or 0,
                    'accurate_long_balls': avg_accurate_long_balls or 0
                }
            
            return stats
            
        except Exception as e:
            print(f"❌ Ошибка получения кроссов и длинных передач: {e}")
            return {}



    # НОВЫЕ МЕТОДЫ ДЛЯ УЛУЧШЕННОГО ПРОГНОЗА ЖЕЛТЫХ КАРТОЧЕК
    def _get_team_yellow_stats(self, team_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику желтых карточек команды"""
        try:
            query = """
            WITH team_matches AS (
                SELECT match_id, home_team_id as team_id FROM football_matches 
                WHERE home_team_id = %(team_id)s AND season_id = %(season_id)s AND status = 'Ended'
                UNION ALL
                SELECT match_id, away_team_id as team_id FROM football_matches 
                WHERE away_team_id = %(team_id)s AND season_id = %(season_id)s AND status = 'Ended'
            )
            SELECT 
                COUNT(DISTINCT fc.match_id) as matches_count,
                COUNT(fc.card_type) as total_yellows,
                COUNT(fc.card_type) * 1.0 / COUNT(DISTINCT fc.match_id) as avg_yellows_per_match
            FROM football_cards fc
            JOIN team_matches tm ON fc.match_id = tm.match_id
            WHERE fc.card_type = 'yellow'
            """
            
            result = self.ch_client.execute(query, {
                'team_id': team_id,
                'season_id': season_id
            })
            
            if result and result[0][0] > 0:
                matches_count, total_yellows, avg_yellows = result[0]
                return {
                    'matches_count': matches_count,
                    'total_yellows': total_yellows,
                    'avg_yellows': round(avg_yellows, 2)
                }
            return {'matches_count': 0, 'total_yellows': 0, 'avg_yellows': 2.0}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики карточек команды {team_id}: {e}")
            return {'matches_count': 0, 'total_yellows': 0, 'avg_yellows': 2.0}

    def _get_team_home_away_yellow_stats(self, team_id: int, season_id: int) -> Dict[str, Any]:
        """Получает домашние/гостевые показатели карточек"""
        try:
            query = """
            SELECT 
                CASE 
                    WHEN fm.home_team_id = %(team_id)s THEN 'home'
                    ELSE 'away'
                END as venue,
                COUNT(DISTINCT fc.match_id) as matches_count,
                COUNT(fc.card_type) as total_yellows,
                COUNT(fc.card_type) * 1.0 / COUNT(DISTINCT fc.match_id) as avg_yellows
            FROM football_cards fc
            JOIN football_matches fm ON fc.match_id = fm.match_id
            WHERE fc.card_type = 'yellow'
                AND (fm.home_team_id = %(team_id)s OR fm.away_team_id = %(team_id)s)
                AND fm.season_id = %(season_id)s
                AND fm.status = 'Ended'
            GROUP BY venue
            """
            
            results = self.ch_client.execute(query, {
                'team_id': team_id,
                'season_id': season_id
            })
            
            stats = {'home': {'avg_yellows': 2.0}, 'away': {'avg_yellows': 2.0}}
            
            for venue, matches_count, total_yellows, avg_yellows in results:
                if venue == 'home':
                    stats['home'] = {
                        'matches_count': matches_count,
                        'total_yellows': total_yellows,
                        'avg_yellows': round(avg_yellows, 2)
                    }
                elif venue == 'away':
                    stats['away'] = {
                        'matches_count': matches_count,
                        'total_yellows': total_yellows,
                        'avg_yellows': round(avg_yellows, 2)
                    }
            
            return stats
            
        except Exception as e:
            print(f"❌ Ошибка получения домашних/гостевых карточек команды {team_id}: {e}")
            return {'home': {'avg_yellows': 2.0}, 'away': {'avg_yellows': 2.0}}

    def _get_referee_yellow_stats(self, referee_id: int, tournament_id: int) -> Dict[str, Any]:
        """Получает статистику желтых карточек рефери"""
        try:
            query = """
            SELECT 
                referee_name,
                referee_yellow_cards,
                referee_games,
                referee_yellow_cards * 1.0 / NULLIF(referee_games, 0) as avg_yellows
            FROM match_fixtures 
            WHERE referee_id = %(referee_id)s
                AND tournament_id = %(tournament_id)s
                AND referee_games > 0
            ORDER BY start_timestamp DESC
            LIMIT 1
            """
            
            result = self.ch_client.execute(query, {
                'referee_id': referee_id,
                'tournament_id': tournament_id
            })
            
            if result:
                name, total_yellows, games, avg_yellows = result[0]
                return {
                    'name': name,
                    'total_yellows': total_yellows,
                    'games': games,
                    'avg_yellows': round(avg_yellows, 2)
                }
            return {'name': 'Неизвестно', 'total_yellows': 0, 'games': 0, 'avg_yellows': 3.0}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики рефери {referee_id}: {e}")
            return {'name': 'Неизвестно', 'total_yellows': 0, 'games': 0, 'avg_yellows': 3.0}

    def _get_h2h_yellow_stats(self, team1_id: int, team2_id: int) -> Dict[str, Any]:
        """Получает историю желтых карточек в личных встречах"""
        try:
            query = """
            SELECT 
                COUNT(DISTINCT fm.match_id) as matches_count,
                COUNT(fc.card_type) as total_yellows,
                COUNT(fc.card_type) * 1.0 / COUNT(DISTINCT fm.match_id) as avg_yellows
            FROM football_matches fm
            JOIN football_cards fc ON fm.match_id = fc.match_id
            WHERE ((fm.home_team_id = %(team1)s AND fm.away_team_id = %(team2)s)
                OR (fm.home_team_id = %(team2)s AND fm.away_team_id = %(team1)s))
                AND fm.status = 'Ended'
                AND fc.card_type = 'yellow'
            """
            
            result = self.ch_client.execute(query, {
                'team1': team1_id,
                'team2': team2_id
            })
            
            if result and result[0][0] > 0:
                matches_count, total_yellows, avg_yellows = result[0]
                return {
                    'matches_count': matches_count,
                    'total_yellows': total_yellows,
                    'avg_yellows': round(avg_yellows, 2)
                }
            return {'matches_count': 0, 'total_yellows': 0, 'avg_yellows': 3.0}
            
        except Exception as e:
            print(f"❌ Ошибка получения H2H статистики карточек: {e}")
            return {'matches_count': 0, 'total_yellows': 0, 'avg_yellows': 3.0}

    def _get_team_fouls_stats(self, team_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику фолов команды"""
        try:
            query = """
            SELECT 
                AVG(fouls) as avg_fouls
            FROM football_match_stats 
            WHERE team_id = %(team_id)s
                AND match_id IN (
                    SELECT match_id FROM football_matches 
                    WHERE season_id = %(season_id)s AND status = 'Ended'
                )
            """
            
            result = self.ch_client.execute(query, {
                'team_id': team_id,
                'season_id': season_id
            })
            
            if result and result[0][0] is not None:
                return {'avg_fouls': round(result[0][0], 2)}
            return {'avg_fouls': 12.0}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики фолов команды {team_id}: {e}")
            return {'avg_fouls': 12.0}

    def _get_position_diff(self, team1_id: int, team2_id: int, tournament_id: int, season_id: int) -> int:
        """Получает разницу позиций в таблице"""
        try:
            query = """
            SELECT 
                t1.position as pos1,
                t2.position as pos2
            FROM team_positions_cache t1
            CROSS JOIN team_positions_cache t2
            WHERE t1.team_id = %(team1)s 
                AND t2.team_id = %(team2)s
                AND t1.tournament_id = %(tournament_id)s
                AND t2.tournament_id = %(tournament_id)s
                AND t1.season_id = %(season_id)s
                AND t2.season_id = %(season_id)s
            ORDER BY t1.updated_at DESC, t2.updated_at DESC
            LIMIT 1
            """
            
            result = self.ch_client.execute(query, {
                'team1': team1_id,
                'team2': team2_id,
                'tournament_id': tournament_id,
                'season_id': season_id
            })
            
            if result and result[0][0] is not None and result[0][1] is not None:
                pos1, pos2 = result[0]
                return abs(pos1 - pos2)
            
            # Если данных нет - возвращаем 0 (равные команды)
            print(f"⚠️ Не удалось получить позиции для команд {team1_id} и {team2_id}")
            return 0
            
        except Exception as e:
            print(f"❌ Ошибка получения разницы позиций: {e}")
            return 0
        
    def get_opponent_yellow_cards_stats(self, team_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику желтых карточек, которые команда провоцирует у соперников"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_opponent_yellows,
                COUNT(DISTINCT fc.match_id) as matches_with_opponent_cards,
                COUNT(*) * 1.0 / COUNT(DISTINCT fc.match_id) as avg_opponent_yellows_per_match
            FROM football_cards fc
            JOIN football_matches fm ON fc.match_id = fm.match_id
            WHERE (fm.home_team_id = %(team_id)s OR fm.away_team_id = %(team_id)s)
            AND fm.season_id = %(season_id)s
            AND fm.status = 'Ended'
            AND fc.card_type = 'yellow'
            AND fc.team_is_home != CASE 
                WHEN fm.home_team_id = %(team_id)s THEN 1 
                ELSE 0 
            END  -- Карточки у СОПЕРНИКА
            """
            
            result = self.ch_client.execute(query, {'team_id': team_id, 'season_id': season_id})
            
            if result and result[0][0] > 0:
                total_opponent_yellows, matches_with_cards, avg_opponent_yellows = result[0]
                return {
                    'total_opponent_yellows': total_opponent_yellows,
                    'matches_with_opponent_cards': matches_with_cards,
                    'avg_opponent_yellows': round(avg_opponent_yellows, 2)
                }
            return {'total_opponent_yellows': 0, 'matches_with_opponent_cards': 0, 'avg_opponent_yellows': 2.0}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики карточек соперников для команды {team_id}: {e}")
        return {'total_opponent_yellows': 0, 'matches_with_opponent_cards': 0, 'avg_opponent_yellows': 2.0}
    
    def _is_derby(self, team1_id: int, team2_id: int) -> Tuple[bool, str]:
        """Проверяет, является ли матч дерби"""
        derby_key1 = (team1_id, team2_id)
        derby_key2 = (team2_id, team1_id)
        
        if derby_key1 in self.DERBY_PAIRS:
            return True, self.DERBY_PAIRS[derby_key1]
        elif derby_key2 in self.DERBY_PAIRS:
            return True, self.DERBY_PAIRS[derby_key2]
        
        return False, ""

    def predict_yellow_cards(self, team1_id: int, team2_id: int, referee_id: int, 
                            tournament_id: int, season_id: int) -> Dict[str, Any]:
        """УЛУЧШЕННЫЙ прогноз желтых карточек с реальными данными"""
        try:
            # 1. Получаем данные по командам
            team1_home_away = self._get_team_home_away_yellow_stats(team1_id, season_id)
            team2_home_away = self._get_team_home_away_yellow_stats(team2_id, season_id)
            
            team1_fouls = self._get_team_fouls_stats(team1_id, season_id)
            team2_fouls = self._get_team_fouls_stats(team2_id, season_id)
            
            # 2. Получаем данные по рефери
            referee_stats = self._get_referee_yellow_stats(referee_id, tournament_id)
            
            # 3. Получаем контекстные данные
            h2h_stats = self._get_h2h_yellow_stats(team1_id, team2_id)
            position_diff = self._get_position_diff(team1_id, team2_id, tournament_id, season_id)
            is_derby, derby_name = self._is_derby(team1_id, team2_id)
            
            # 4. Расчет прогноза (40% команды + 40% рефери + 20% контекст)
            team1_home_yellows = team1_home_away['home']['avg_yellows']
            team2_away_yellows = team2_home_away['away']['avg_yellows']
            
            team_component = (team1_home_yellows + team2_away_yellows) / 2 * 0.4
            referee_component = referee_stats['avg_yellows'] * 0.4
            context_component = h2h_stats['avg_yellows'] * 0.2
            
            # 5. Корректировки на контекст
            context_adjustment = 0.0
            if is_derby:
                context_adjustment += 0.3
            if position_diff <= 3:
                context_adjustment += 0.2
            elif position_diff >= 8:
                context_adjustment += 0.1
            
            final_prediction = team_component + referee_component + context_component + context_adjustment
            
            # 6. Определяем рекомендацию
            if final_prediction < 3.2:
                cards_total = "ТМ 4.5"
                confidence = "🔴 Высокая"
            elif final_prediction < 3.8:
                cards_total = "ТМ 4.5"
                confidence = "🟡 Средняя"
            elif final_prediction < 4.4:
                cards_total = "ТБ 4.5"
                confidence = "🟡 Средняя"
            else:
                cards_total = "ТБ 4.5"
                confidence = "🔴 Высокая"
            
            # 7. Индикаторы агрессивности
            def get_aggression_indicator(avg_yellows):
                if avg_yellows < 3.0:
                    return "🟢"
                elif avg_yellows < 4.0:
                    return "🟡"
                else:
                    return "🔴"
            
            return {
                'predicted_yellow_cards': round(final_prediction, 2),
                'team1_home_yellows': team1_home_yellows,
                'team2_away_yellows': team2_away_yellows,
                'referee_avg_yellows': referee_stats['avg_yellows'],
                'referee_name': referee_stats['name'],
                'referee_games': referee_stats['games'],
                'h2h_avg_yellows': h2h_stats['avg_yellows'],
                'position_diff': position_diff,
                'team1_avg_fouls': team1_fouls['avg_fouls'],
                'team2_avg_fouls': team2_fouls['avg_fouls'],
                'is_derby': is_derby,
                'derby_name': derby_name,
                'cards_total_prediction': cards_total,
                'confidence': confidence,
                'team1_aggression': get_aggression_indicator(team1_home_yellows),
                'team2_aggression': get_aggression_indicator(team2_away_yellows),
                'referee_aggression': get_aggression_indicator(referee_stats['avg_yellows']),
                'h2h_aggression': get_aggression_indicator(h2h_stats['avg_yellows'])
            }
            
        except Exception as e:
            print(f"❌ Ошибка улучшенного прогноза желтых карточек: {e}")
            return {}

    def get_referee_stats_from_db(self, referee_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику рефери из БД"""
        try:
            query = """
            SELECT 
                referee_id, referee_name, referee_yellow_cards, referee_red_cards,
                referee_yellow_red_cards, referee_games, referee_country
            FROM match_fixtures 
            WHERE referee_id = %(referee_id)s
            AND tournament_id = %(tournament_id)s
            AND season_id = %(season_id)s
            LIMIT 1
            """
            
            result = self.ch_client.execute(query, {
                'referee_id': referee_id,
                'tournament_id': tournament_id,
                'season_id': season_id
            })
            
            if result:
                return {
                    'referee_id': result[0][0],
                    'name': result[0][1],
                    'yellow_cards': result[0][2],
                    'red_cards': result[0][3],
                    'yellow_red_cards': result[0][4],
                    'games': result[0][5],
                    'country': result[0][6]
                }
            return {}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики рефери: {e}")
            return {}

    def get_match_referee(self, home_team_id: int, away_team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает информацию о рефери для предстоящего матча"""
        try:
            query = """
            SELECT 
                referee_id, referee_name, referee_yellow_cards, referee_red_cards,
                referee_yellow_red_cards, referee_games, referee_country
            FROM match_fixtures 
            WHERE home_team_id = %(home_team_id)s 
                AND away_team_id = %(away_team_id)s
                AND tournament_id = %(tournament_id)s 
                AND season_id = %(season_id)s
            ORDER BY start_timestamp DESC 
            LIMIT 1
            """
            
            result = self.ch_client.execute(query, {
                'home_team_id': home_team_id,
                'away_team_id': away_team_id, 
                'tournament_id': tournament_id,
                'season_id': season_id
            })
            
            if result:
                return {
                    'referee_id': result[0][0],
                    'name': result[0][1],
                    'yellow_cards': result[0][2],
                    'red_cards': result[0][3],
                    'yellow_red_cards': result[0][4],
                    'games': result[0][5],
                    'country': result[0][6]
                }
            return {}
            
        except Exception as e:
            print(f"❌ Ошибка получения рефери матча: {e}")
            return {}

    def get_team_home_away_performance(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает показатели команды дома и в гостях"""
        try:
            query = """
            SELECT 
                CASE 
                    WHEN fm.home_team_id = %(team_id)s THEN 'home'
                    ELSE 'away'
                END as match_type,
                COUNT(*) as matches,
                AVG(CASE 
                    WHEN fm.home_team_id = %(team_id)s THEN fm.home_score
                    ELSE fm.away_score 
                END) as avg_goals_scored,
                AVG(CASE 
                    WHEN fm.home_team_id = %(team_id)s THEN fm.away_score
                    ELSE fm.home_score 
                END) as avg_goals_conceded,
                AVG(CASE 
                    WHEN fm.home_team_id = %(team_id)s THEN 
                        CASE WHEN fm.home_score > fm.away_score THEN 1 ELSE 0 END
                    ELSE 
                        CASE WHEN fm.away_score > fm.home_score THEN 1 ELSE 0 END
                END) as win_rate
            FROM football_matches fm
            WHERE (fm.home_team_id = %(team_id)s OR fm.away_team_id = %(team_id)s)
            AND fm.tournament_id = %(tournament_id)s
            AND fm.season_id = %(season_id)s
            AND fm.status = 'Ended'
            GROUP BY match_type
            """
            
            results = self.ch_client.execute(query, {
                'team_id': team_id,
                'tournament_id': tournament_id,
                'season_id': season_id
            })
            
            performance = {'home': {}, 'away': {}}
            for match_type, matches, avg_scored, avg_conceded, win_rate in results:
                if match_type == 'home':
                    performance['home'] = {
                        'matches': matches,
                        'avg_goals_scored': avg_scored or 0,
                        'avg_goals_conceded': avg_conceded or 0,
                        'win_rate': (win_rate or 0) * 100
                    }
                elif match_type == 'away':
                    performance['away'] = {
                        'matches': matches,
                        'avg_goals_scored': avg_scored or 0,
                        'avg_goals_conceded': avg_conceded or 0,
                        'win_rate': (win_rate or 0) * 100
                    }
            
            return performance
            
        except Exception as e:
            print(f"❌ Ошибка получения домашних/гостевых показателей: {e}")
            return {'home': {}, 'away': {}}
        
    def get_team_corners_stats(self, team_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику угловых команды"""
        try:
            query = """
            SELECT 
                AVG(fms1.corners) as avg_corners_for,
                AVG(fms2.corners) as avg_corners_against
            FROM football_match_stats fms1
            JOIN football_match_stats fms2 ON fms1.match_id = fms2.match_id 
                AND fms2.team_id != fms1.team_id
            JOIN football_matches fm ON fms1.match_id = fm.match_id
            WHERE fms1.team_id = %(team_id)s
            AND fm.season_id = %(season_id)s
            AND fm.status = 'Ended'
            """
            
            result = self.ch_client.execute(query, {
                'team_id': team_id,
                'season_id': season_id
            })
            
            if result and result[0][0] is not None:
                avg_corners_for, avg_corners_against = result[0]
                return {
                    'avg_corners_for': round(avg_corners_for or 0, 1),
                    'avg_corners_against': round(avg_corners_against or 0, 1),
                    'corners_balance': round((avg_corners_for or 0) - (avg_corners_against or 0), 1)
                }
            return {'avg_corners_for': 0, 'avg_corners_against': 0, 'corners_balance': 0}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики угловых для команды {team_id}: {e}")
            return {'avg_corners_for': 0, 'avg_corners_against': 0, 'corners_balance': 0}
    def get_team_corners_stats_by_venue(self, team_id: int, season_id: int, venue: str) -> Dict[str, Any]:
        """Получает статистику угловых с учетом домашних/гостевых"""
        try:
            query = """
            SELECT 
                AVG(fms1.corners) as avg_corners_for,
                AVG(fms2.corners) as avg_corners_against
            FROM football_match_stats fms1
            JOIN football_match_stats fms2 ON fms1.match_id = fms2.match_id 
                AND fms2.team_id != fms1.team_id
            JOIN football_matches fm ON fms1.match_id = fm.match_id
            WHERE fms1.team_id = %(team_id)s
            AND fms1.team_type = %(venue)s  -- 'home' или 'away'
            AND fm.season_id = %(season_id)s
            AND fm.status = 'Ended'
            """
            
            result = self.ch_client.execute(query, {
                'team_id': team_id,
                'venue': venue,
                'season_id': season_id
            })
        
            if result and result[0][0] is not None:
                    avg_corners_for, avg_corners_against = result[0]
                    return {
                        'avg_corners_for': round(avg_corners_for or 0, 1),
                        'avg_corners_against': round(avg_corners_against or 0, 1),
                        'corners_balance': round((avg_corners_for or 0) - (avg_corners_against or 0), 1)
                    }
            return {'avg_corners_for': 0, 'avg_corners_against': 0, 'corners_balance': 0}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики угловых для команды {team_id}: {e}")
            return {'avg_corners_for': 0, 'avg_corners_against': 0, 'corners_balance': 0}
        
    def predict_match_result_with_home_away(self, team1_id: int, team2_id: int, 
                                          team1_name: str, team2_name: str,
                                          tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Прогнозирует результат матча с учетом домашних/гостевых показателей"""
        try:
            team1_performance = self.get_team_home_away_performance(team1_id, tournament_id, season_id)
            team2_performance = self.get_team_home_away_performance(team2_id, tournament_id, season_id)
            
            if not team1_performance.get('home') or not team2_performance.get('away'):
                return {}
            
            team1_home = team1_performance['home']
            team2_away = team2_performance['away']
            
            # Прогноз счета на основе домашних/гостевых показателей
            predicted_home_goals = (team1_home['avg_goals_scored'] + team2_away['avg_goals_conceded']) / 2
            predicted_away_goals = (team2_away['avg_goals_scored'] + team1_home['avg_goals_conceded']) / 2
            
            # Вероятности исходов с учетом домашнего преимущества
            home_win_prob = (team1_home['win_rate'] + (100 - team2_away['win_rate'])) / 2
            away_win_prob = (team2_away['win_rate'] + (100 - team1_home['win_rate'])) / 2
            draw_prob = max(0, 100 - home_win_prob - away_win_prob)
            
            # Нормализуем вероятности
            total_prob = home_win_prob + draw_prob + away_win_prob
            home_win_prob = (home_win_prob / total_prob) * 100
            draw_prob = (draw_prob / total_prob) * 100
            away_win_prob = (away_win_prob / total_prob) * 100
            
            return {
                'predicted_score': f"{predicted_home_goals:.1f}-{predicted_away_goals:.1f}",
                'probabilities': {
                    'home_win': round(home_win_prob, 1),
                    'draw': round(draw_prob, 1),
                    'away_win': round(away_win_prob, 1)
                },
                'home_stats': team1_home,
                'away_stats': team2_away
            }
            
        except Exception as e:
            print(f"❌ Ошибка прогноза результата: {e}")
            return {}

    # МЕТОДЫ АНАЛИЗА 
    async def get_players_analysis(self, team1_id: int, team2_id: int, team1_name: str, team2_name: str, 
                              tournament_id: int, season_id: int):
        """Анализ только ключевых игроков"""
        print(f"⭐ АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ:")
        print("=" * 50)
        
        try:
            team1_key_players = self.get_key_players_progress(team1_id, season_id)
            team2_key_players = self.get_key_players_progress(team2_id, season_id)

            print(f"\n🔑 {team1_name} - ключевые игроки:")
            if team1_key_players:
                for player in team1_key_players[:3]:
                    trend_icon = "📈" if player['trend'] == 'up' else "📉" if player['trend'] == 'down' else "➡️"
                    print(f"   • {player['name']} ({player['position']}) {trend_icon}")
                    print(f"     Голы: {player['goals']} | Ассисты: {player['assists']} | Удары: {player['shots']}")
            else:
                print(f"   • Статистика появится после 3-х сыгранных туров")

            print(f"\n🔑 {team2_name} - ключевые игроки:")
            if team2_key_players:
                for player in team2_key_players[:3]:
                    trend_icon = "📈" if player['trend'] == 'up' else "📉" if player['trend'] == 'down' else "➡️"
                    print(f"   • {player['name']} ({player['position']}) {trend_icon}")
                    print(f"     Голы: {player['goals']} | Ассисты: {player['assists']} | Удары: {player['shots']}")
            else:
                print(f"   • Статистика появится после 3-х сыгранных туров")
                
        except Exception as e:
            print(f"❌ Ошибка анализа игроков: {e}")

    async def get_match_analysis(self, team1_id: int, team2_id: int, team1_name: str, team2_name: str, 
                               tournament_id: int = 203, season_id: int = 77142):
        """Расширенный анализ матча с данными из БД"""
        
        print(f"🎯 РАСШИРЕННЫЙ АНАЛИЗ МАТЧА: {team1_name} vs {team2_name}")
        print("=" * 60)
        
        try:
            # 1. Получаем текущую таблицу из кэша
            standings = self.get_current_standings(tournament_id, season_id)
            
            team1_position = standings.get(team1_id, "N/A")
            team2_position = standings.get(team2_id, "N/A")
            
            # 2. Получаем статистику из кэш-таблицы
            team1_stats = self.get_team_stats_from_db(team1_id, tournament_id, season_id)
            team2_stats = self.get_team_stats_from_db(team2_id, tournament_id, season_id)
            
            # 3. Получаем данные о позиции и динамике
            team1_position_data = self.get_team_position_from_db(team1_id, tournament_id, season_id)
            team2_position_data = self.get_team_position_from_db(team2_id, tournament_id, season_id)
            
            team1_pos_trend = self.analyze_position_trend(team1_position_data)
            team2_pos_trend = self.analyze_position_trend(team2_position_data)

            # 4. Получаем исторические данные из основных таблиц
            team1_form = self.get_team_form_from_db(team1_id, season_id)
            team2_form = self.get_team_form_from_db(team2_id, season_id)

            # Обработка данных из БД
            team1_matches = team1_stats.get('matches', 1)
            team2_matches = team2_stats.get('matches', 1)

            # Расчет показателей на матч
            team1_goals_pm = self.safe_divide(team1_stats.get('goalsScored', 0), team1_matches)
            team2_goals_pm = self.safe_divide(team2_stats.get('goalsScored', 0), team2_matches)
            
            team1_shots_pm = self.safe_divide(team1_stats.get('shots', 0), team1_matches)
            team2_shots_pm = self.safe_divide(team2_stats.get('shots', 0), team2_matches)
            
            team1_shots_on_target_pm = self.safe_divide(team1_stats.get('shotsOnTarget', 0), team1_matches)
            team2_shots_on_target_pm = self.safe_divide(team2_stats.get('shotsOnTarget', 0), team2_matches)
            
            # 5. Получаем xG данные из БД
            team1_xg, team2_xg = self.get_team_xg_from_db(team1_id, team2_id, season_id)

            # ВЫВОД РЕЗУЛЬТАТОВ
            print(f"\n🏠 {team1_name} (дома) vs 🛬 {team2_name} (в гостях)")
            print("=" * 50)

            # ПОЗИЦИЯ В ТАБЛИЦЕ
            print(f"\n🏆 ПОЗИЦИЯ В ТАБЛИЦЕ:")
            print(f"   {team1_name}: {team1_position} место {team1_pos_trend[1]}")
            print(f"   {team2_name}: {team2_position} место {team2_pos_trend[1]}")

            # Анализ разницы в классе
            if team1_position != "N/A" and team2_position != "N/A":
                position_diff = abs(int(team1_position) - int(team2_position))
                if position_diff <= 3:
                    class_analysis = "Равные соперники"
                elif position_diff <= 6:
                    class_analysis = "Умеренное преимущество" 
                else:
                    class_analysis = "Значительное преимущество"
                print(f"   Разница в классе: {position_diff} позиций ({class_analysis})")
            else:
                position_diff = 0
                class_analysis = "Данные недоступны"

            # ОСНОВНАЯ СТАТИСТИКА
            print(f"\n⚽ РЕЗУЛЬТАТИВНОСТЬ (на основе {team1_matches} матчей):")
            print(f"   {team1_name}: {team1_goals_pm:.1f} голов за матч")
            print(f"   {team2_name}: {team2_goals_pm:.1f} голов за матч")

            print(f"\n🎯 УДАРЫ:")
            print(f"   {team1_name}: {team1_shots_pm:.1f} ударов ({team1_shots_on_target_pm:.1f} в створ) за матч")
            print(f"   {team2_name}: {team2_shots_pm:.1f} ударов ({team2_shots_on_target_pm:.1f} в створ) за матч")

            # xG АНАЛИЗ
            print(f"\n📈 РЕАЛЬНЫЙ xG АНАЛИЗ:")
            print(f"   {team1_name}: {team1_xg:.2f} xG")
            print(f"   {team2_name}: {team2_xg:.2f} xG")

            # ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ
            team1_efficiency = team1_goals_pm - team1_xg
            team2_efficiency = team2_goals_pm - team2_xg

            print(f"\n🎯 ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ:")
            print(f"   {team1_name}: {team1_goals_pm:.1f} голов при {team1_xg:.1f} xG ({team1_efficiency:+.2f})")
            print(f"   {team2_name}: {team2_goals_pm:.1f} голов при {team2_xg:.1f} xG ({team2_efficiency:+.2f})")

            # КОНТРОЛЬ ИГРЫ
            team1_possession = team1_stats.get('averageBallPossession', 0)
            team2_possession = team2_stats.get('averageBallPossession', 0)
            
            print(f"\n📊 КОНТРОЛЬ ИГРЫ:")
            print(f"   {team1_name}: {team1_possession:.1f}% владения")
            print(f"   {team2_name}: {team2_possession:.1f}% владения")

            # ПЕРЕДАЧИ
            team1_pass_accuracy = team1_stats.get('accuratePassesPercentage', 0)
            team2_pass_accuracy = team2_stats.get('accuratePassesPercentage', 0)

            print(f"\n🔄 ТОЧНОСТЬ ПЕРЕДАЧ:")
            print(f"   {team1_name}: {team1_pass_accuracy:.1f}%")
            print(f"   {team2_name}: {team2_pass_accuracy:.1f}%")

            # Получаем данные о кроссах и длинных передачах
            crosses_longballs_data = self.get_team_crosses_longballs_from_db(team1_id, team2_id, season_id)

            team1_crosses_data = crosses_longballs_data.get(team1_id, {})
            team2_crosses_data = crosses_longballs_data.get(team2_id, {})

            # АКТИВНОСТЬ ФЛАНГОВ
            print(f"\n🔄 АКТИВНОСТЬ ФЛАНГОВ:")
            team1_total_crosses = team1_crosses_data.get('total_crosses', 0)
            team2_total_crosses = team2_crosses_data.get('total_crosses', 0)
            team1_accurate_crosses = team1_crosses_data.get('accurate_crosses', 0)
            team2_accurate_crosses = team2_crosses_data.get('accurate_crosses', 0)

            team1_cross_accuracy = self.safe_divide(team1_accurate_crosses, team1_total_crosses) * 100
            team2_cross_accuracy = self.safe_divide(team2_accurate_crosses, team2_total_crosses) * 100

            print(f"   {team1_name}: {team1_total_crosses:.1f} кроссов ({team1_cross_accuracy:.1f}% точность)")
            print(f"   {team2_name}: {team2_total_crosses:.1f} кроссов ({team2_cross_accuracy:.1f}% точность)")

            # ДАЛЬНИЕ АТАКИ
            print(f"\n🎯 ДАЛЬНИЕ АТАКИ:")
            team1_total_long_balls = team1_crosses_data.get('total_long_balls', 0)
            team2_total_long_balls = team2_crosses_data.get('total_long_balls', 0)
            team1_accurate_long_balls = team1_crosses_data.get('accurate_long_balls', 0)
            team2_accurate_long_balls = team2_crosses_data.get('accurate_long_balls', 0)

            team1_longball_accuracy = self.safe_divide(team1_accurate_long_balls, team1_total_long_balls) * 100
            team2_longball_accuracy = self.safe_divide(team2_accurate_long_balls, team2_total_long_balls) * 100

            print(f"   {team1_name}: {team1_total_long_balls:.1f} длинных передач ({team1_longball_accuracy:.1f}% точность)")
            print(f"   {team2_name}: {team2_total_long_balls:.1f} длинных передач ({team2_longball_accuracy:.1f}% точность)")

            # ОБОРОНА
            team1_goals_conceded_pm = self.safe_divide(team1_stats.get('goalsConceded', 0), team1_matches)
            team2_goals_conceded_pm = self.safe_divide(team2_stats.get('goalsConceded', 0), team2_matches)

            print(f"\n🛡️ ОБОРОНА:")
            print(f"   {team1_name}: {team1_goals_conceded_pm:.1f} пропущенных за матч")
            print(f"   {team2_name}: {team2_goals_conceded_pm:.1f} пропущенных за матч")
            # УГЛОВЫЕ
            print(f"\n🎯 СТАТИСТИКА УГЛОВЫХ:")
            team1_corners_stats = self.get_team_corners_stats(team1_id, season_id)
            team2_corners_stats = self.get_team_corners_stats(team2_id, season_id)
            team1_corners_stats = self.get_team_corners_stats_by_venue(team1_id, season_id, 'home')  # Хозяева
            team2_corners_stats = self.get_team_corners_stats_by_venue(team2_id, season_id, 'away')  # Гости

            
            print(f"   {team1_name} (общие):")
            print(f"      • Атака: {team1_corners_stats['avg_corners_for']} угловых за матч")
            print(f"      • Оборона: {team1_corners_stats['avg_corners_against']} пропускает")
            print(f"      • Баланс: {team1_corners_stats['corners_balance']:+.1f}")

            print(f"   {team2_name} (общие):")
            print(f"      • Атака: {team2_corners_stats['avg_corners_for']} угловых за матч") 
            print(f"      • Оборона: {team2_corners_stats['avg_corners_against']} пропускает")
            print(f"      • Баланс: {team2_corners_stats['corners_balance']:+.1f}")

            print(f"   {team1_name} (дома):")
            print(f"      • Атака: {team1_corners_stats['avg_corners_for']} угловых за матч")
            print(f"      • Оборона: {team1_corners_stats['avg_corners_against']} пропускает")
            print(f"      • Баланс: {team1_corners_stats['corners_balance']:+.1f}")

            print(f"   {team2_name} (в гостях):")
            print(f"      • Атака: {team2_corners_stats['avg_corners_for']} угловых за матч")
            print(f"      • Оборона: {team2_corners_stats['avg_corners_against']} пропускает")
            print(f"      • Баланс: {team2_corners_stats['corners_balance']:+.1f}")


            # Анализ прессинга
            total_corners = team1_corners_stats['avg_corners_for'] + team2_corners_stats['avg_corners_for']
            print(f"\n📊 ОЖИДАЕМАЯ АКТИВНОСТЬ:")
            print(f"   • Всего угловых за матч: {total_corners:.1f}")
            print(f"   • Интенсивность атаки: {'🔴 Высокая' if total_corners > 10 else '🟡 Средняя' if total_corners > 7 else '🟢 Низкая'}")
            # АГРЕССИВНОСТЬ
            print(f"\n🟨 АГРЕССИВНОСТЬ:")
            team1_fouls_pm = self.safe_divide(team1_stats.get('fouls', 0), team1_matches)
            team2_fouls_pm = self.safe_divide(team2_stats.get('fouls', 0), team2_matches)

            # ДОБАВЛЯЕМ ЖЕЛТЫЕ КАРТОЧКИ ИЗ СУЩЕСТВУЮЩИХ МЕТОДОВ
            team1_yellow_stats = self._get_team_yellow_stats(team1_id, season_id)
            team2_yellow_stats = self._get_team_yellow_stats(team2_id, season_id)

            # ДОБАВЛЯЕМ КАРТОЧКИ СОПЕРНИКОВ
            team1_opponent_yellow_stats = self.get_opponent_yellow_cards_stats(team1_id, season_id)
            team2_opponent_yellow_stats = self.get_opponent_yellow_cards_stats(team2_id, season_id)

            print(f"   {team1_name}: {team1_fouls_pm:.1f} фолов за матч")
            print(f"   {team2_name}: {team2_fouls_pm:.1f} фолов за матч")

            print(f"\n🟨 ЖЕЛТЫЕ КАРТОЧКИ:")
            print(f"   {team1_name}: {team1_yellow_stats['avg_yellows']} в среднем за матч")
            print(f"   {team2_name}: {team2_yellow_stats['avg_yellows']} в среднем за матч")

            print(f"\n🟨 КАРТОЧКИ У СОПЕРНИКОВ:")
            print(f"   Против {team1_name}: {team1_opponent_yellow_stats['avg_opponent_yellows']} в среднем за матч")
            print(f"   Против {team2_name}: {team2_opponent_yellow_stats['avg_opponent_yellows']} в среднем за матч")

            # Анализ агрессивности
            print(f"\n📊 АНАЛИЗ АГРЕССИВНОСТИ:")
            team1_total_yellows = team1_yellow_stats['avg_yellows'] + team1_opponent_yellow_stats['avg_opponent_yellows']
            team2_total_yellows = team2_yellow_stats['avg_yellows'] + team2_opponent_yellow_stats['avg_opponent_yellows']

            print(f"   Всего карточек в матчах {team1_name}: {team1_total_yellows:.1f} за матч")
            print(f"   Всего карточек в матчах {team2_name}: {team2_total_yellows:.1f} за матч")

            # КАЧЕСТВО МОМЕНТОВ
            team1_big_chances_pm = self.safe_divide(team1_stats.get('bigChances', 0), team1_matches)
            team2_big_chances_pm = self.safe_divide(team2_stats.get('bigChances', 0), team2_matches)
            team1_big_chances_missed_pm = self.safe_divide(team1_stats.get('bigChancesMissed', 0), team1_matches)
            team2_big_chances_missed_pm = self.safe_divide(team2_stats.get('bigChancesMissed', 0), team2_matches)

            print(f"\n🎪 КАЧЕСТВО МОМЕНТОВ:")
            print(f"   {team1_name}: {team1_big_chances_pm:.1f} больших шансов ({team1_big_chances_missed_pm:.1f} пропущено)")
            print(f"   {team2_name}: {team2_big_chances_pm:.1f} больших шансов ({team2_big_chances_missed_pm:.1f} пропущено)")

            # АНАЛИЗ ЗОН АТАК
            print(f"\n🎯 АНАЛИЗ ЗОН АТАК И УЯЗВИМОСТЕЙ:")

            team1_total_goals = team1_stats.get('goalsScored', 1)
            team2_total_goals = team2_stats.get('goalsScored', 1)

            print(f"\n🏹 {team1_name} АТАКУЕТ:")
            team1_inside = self.safe_divide(team1_stats.get('goalsFromInsideTheBox', 0), team1_total_goals) * 100
            team1_outside = self.safe_divide(team1_stats.get('goalsFromOutsideTheBox', 0), team1_total_goals) * 100  
            team1_headed = self.safe_divide(team1_stats.get('headedGoals', 0), team1_total_goals) * 100

            total_percent = team1_inside + team1_outside + team1_headed
            if total_percent > 100:
                team1_inside = (team1_inside / total_percent) * 100
                team1_outside = (team1_outside / total_percent) * 100
                team1_headed = (team1_headed / total_percent) * 100

            print(f"   • {team1_inside:.0f}% голов изнутри штрафной")
            print(f"   • {team1_outside:.0f}% голов издали")
            print(f"   • {team1_headed:.0f}% голов головой")

            print(f"\n🏹 {team2_name} АТАКУЕТ:")
            team2_inside = self.safe_divide(team2_stats.get('goalsFromInsideTheBox', 0), team2_total_goals) * 100
            team2_outside = self.safe_divide(team2_stats.get('goalsFromOutsideTheBox', 0), team2_total_goals) * 100  
            team2_headed = self.safe_divide(team2_stats.get('headedGoals', 0), team2_total_goals) * 100

            total_percent = team2_inside + team2_outside + team2_headed
            if total_percent > 100:
                team2_inside = (team2_inside / total_percent) * 100
                team2_outside = (team2_outside / total_percent) * 100
                team2_headed = (team2_headed / total_percent) * 100

            print(f"   • {team2_inside:.0f}% голов изнутри штрафной")
            print(f"   • {team2_outside:.0f}% голов издали")
            print(f"   • {team2_headed:.0f}% голов головой")

            # ФОРМА КОМАНД
            matches_count = len(team1_form) if team1_form else 0

            if matches_count == 0:
                print(f"\n📈 ФОРМА (первый тур - матчей нет)")
            else:
                form_text = "матч" if matches_count == 1 else f"последние {matches_count} матча" if matches_count < 5 else "последние 5 матчей"
                print(f"\n📈 ФОРМА (за {form_text}):")

            if team1_form:
                team1_form_results = [match[6] for match in team1_form]
                team1_form_display = team1_form_results[::-1]
                form_icons = ''.join(['🟢' if r == 'W' else '🟡' if r == 'D' else '🔴' for r in team1_form_display])
                print(f"   {team1_name}: {form_icons}")
                if matches_count > 0:
                    print(f"   Последние результаты: {', '.join([r for r in team1_form_display])}")
            else:
                print(f"   {team1_name}: нет данных")
                
            if team2_form:
                team2_form_results = [match[6] for match in team2_form]
                team2_form_display = team2_form_results[::-1]
                form_icons = ''.join(['🟢' if r == 'W' else '🟡' if r == 'D' else '🔴' for r in team2_form_display])
                print(f"   {team2_name}: {form_icons}")
                if matches_count > 0:
                    print(f"   Последние результаты: {', '.join([r for r in team2_form_display])}")
            else:
                print(f"   {team2_name}: нет данных")

            # ИСТОРИЯ ЛИЧНЫХ ВСТРЕЧ
            print(f"\n📊 ИСТОРИЯ ЛИЧНЫХ ВСТРЕЧ ({team1_name} vs {team2_name}):")
            h2h_all_time = self.get_team_all_time_stats(team1_id, team2_id)

            if h2h_all_time:
                print(f"\n🤝 ВСЕГО МАТЧЕЙ: {h2h_all_time['total_matches']}")

                if h2h_all_time['total_matches'] > 0:
                    print(f"   • {team1_name}: {h2h_all_time['team1_wins']} побед ({h2h_all_time['team1_win_rate']:.1f}%)")
                    print(f"   • {team2_name}: {h2h_all_time['team2_wins']} побед ({h2h_all_time['team2_win_rate']:.1f}%)")
                    print(f"   • Ничьих: {h2h_all_time['draws']} ({h2h_all_time['draws']/h2h_all_time['total_matches']*100:.1f}%)")
                else:
                    print(f"   • {team1_name}: {h2h_all_time['team1_wins']} побед (0%)")
                    print(f"   • {team2_name}: {h2h_all_time['team2_wins']} побед (0%)")
                    print(f"   • Ничьих: {h2h_all_time['draws']} (0%)")
                    print("   🤝 Команды ранее не встречались")
                
                print(f"\n⚽ ОБЩАЯ РЕЗУЛЬТАТИВНОСТЬ:")
                print(f"   • {team1_name}: {h2h_all_time['team1_goals']} голов")
                print(f"   • {team2_name}: {h2h_all_time['team2_goals']} голов")
                print(f"   • Разница: +{h2h_all_time['team1_goals'] - h2h_all_time['team2_goals']}")
                
                print(f"\n📈 СРЕДНИЕ ПОКАЗАТЕЛИ ЗА МАТЧ:")
                print(f"   • {team1_name}: {h2h_all_time['team1_avg_goals']:.1f} голов")
                print(f"   • {team2_name}: {h2h_all_time['team2_avg_goals']:.1f} голов")
                print(f"   • Всего голов за матч: {h2h_all_time['team1_avg_goals'] + h2h_all_time['team2_avg_goals']:.1f}")

            # КЛЮЧЕВЫЕ ИГРОКИ
            print(f"\n⭐ АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ:")
            team1_key_players = self.get_key_players_progress(team1_id, season_id)
            team2_key_players = self.get_key_players_progress(team2_id, season_id)

            print(f"\n🔑 {team1_name} - ключевые игроки:")
            if team1_key_players:
                for player in team1_key_players[:3]:
                    trend_icon = "📈" if player['trend'] == 'up' else "📉" if player['trend'] == 'down' else "➡️"
                    print(f"   • {player['name']} ({player['position']}) {trend_icon}")
                    print(f"     Голы: {player['goals']} | Ассисты: {player['assists']} | Удары: {player['shots']}")
            else:
                print(f"   • Статистика появится после 3-х сыгранных туров")

            print(f"\n🔑 {team2_name} - ключевые игроки:")
            if team2_key_players:
                for player in team2_key_players[:3]:
                    trend_icon = "📈" if player['trend'] == 'up' else "📉" if player['trend'] == 'down' else "➡️"
                    print(f"   • {player['name']} ({player['position']}) {trend_icon}")
                    print(f"     Голы: {player['goals']} | Ассисты: {player['assists']} | Удары: {player['shots']}")
            else:
                print(f"   • Статистика появится после 3-х сыгранных туров")

            # СУЩЕСТВУЮЩИЕ ПРОГНОЗЫ
            total_goals = team1_goals_pm + team2_goals_pm
            
            print(f"\n🏆 ПРОГНОЗ С УЧЕТОМ ПОЗИЦИИ В ТАБЛИЦЕ:")
            print(f"   • Ожидаемые голы: {total_goals:.1f}")
            
            if position_diff > 0:
                print(f"   • Разница в классе: {position_diff} позиций")

            # ОБНОВЛЕННЫЕ ПРОГНОЗЫ
            if team1_position != "N/A" and team2_position != "N/A":
                if total_goals > 2.8 and position_diff <= 4:
                    total_pred = "ТБ 2.5 (высокая вероятность)"
                    confidence = "🔴 Высокая"
                elif total_goals > 2.2 and position_diff <= 6:
                    total_pred = "ТБ 2.5 (средняя вероятность)"
                    confidence = "🟡 Средняя"
                elif total_goals > 1.8:
                    total_pred = "ТМ 2.5 (низкая вероятность)" 
                    confidence = "🟢 Низкая"
                else:
                    total_pred = "ТМ 2.5 (высокая вероятность)"
                    confidence = "🔴 Высокая"
            else:
                if total_goals > 2.8:
                    total_pred = "ТБ 2.5 (средняя вероятность)"
                    confidence = "🟡 Средняя"
                elif total_goals > 2.2:
                    total_pred = "ТБ 2.5 (низкая вероятность)"
                    confidence = "🟢 Низкая"
                else:
                    total_pred = "ТМ 2.5 (средняя вероятность)"
                    confidence = "🟡 Средняя"

            print(f"\n💰 РЕКОМЕНДАЦИИ:")
            print(f"   • Тоталы голов: {total_pred}")
            print(f"   • Уверенность: {confidence}")
            if position_diff > 0:
                print(f"   • Фактор позиции: {class_analysis}")

            # КЛЮЧЕВЫЕ ИНСАЙТЫ
            print(f"\n📈 КЛЮЧЕВЫЕ ИНСАЙТЫ:")
            insights = self.generate_insights(
                team1_name, team2_name, team1_stats, team2_stats, 
                team1_position, team2_position, team1_matches, team2_matches,
                team1_xg, team2_xg
            )
            
            for i, insight in enumerate(insights, 1):
                print(f"   {i}. {insight}")

            # НОВЫЕ УНИКАЛЬНЫЕ ПРОГНОЗЫ (без дублирования)
            print(f"\n🎲 ЭКСКЛЮЗИВНЫЕ ПРОГНОЗЫ:")
            print("=" * 40)

            # ПРОГНОЗ ЖЕЛТЫХ КАРТОЧЕК С УЧЕТОМ РЕФЕРИ
            print(f"\n🟨 АНАЛИЗ ДИСЦИПЛИНЫ С УЧЕТОМ РЕФЕРИ:")
            try:
                referee_info = self.get_match_referee(team1_id, team2_id, tournament_id, season_id)
                if referee_info and referee_info.get('referee_id'):
                    yellow_cards_prediction = self.predict_yellow_cards(
                        team1_id, team2_id, referee_info['referee_id'], 
                        tournament_id, season_id
                    )
                    
                    if yellow_cards_prediction:
                        print(f"   👨‍⚖️ Рефери: {yellow_cards_prediction['referee_name']}")
                        if yellow_cards_prediction['referee_games'] > 0:
                            print(f"   📊 Среднее у рефери: {yellow_cards_prediction['referee_avg_yellows']} желтых за матч")
                        print(f"   🎯 Прогноз желтых карточек: {yellow_cards_prediction['predicted_yellow_cards']}")
                        print(f"   💰 Рекомендация: {yellow_cards_prediction['cards_total_prediction']}")
                        print(f"   🎯 Уверенность: {yellow_cards_prediction['confidence']}")
                    else:
                        print(f"   ℹ️ Данные для прогноза карточек недоступны")
                else:
                    print(f"   ℹ️ Информация о рефери недоступна")
                    
            except Exception as e:
                print(f"   ❌ Ошибка прогноза карточек: {e}")

            # ПРОГНОЗ РЕЗУЛЬТАТА С УЧЕТОМ ДОМАШНИХ/ГОСТЕВЫХ ПОКАЗАТЕЛЕЙ
            print(f"\n🏠🛬 ПРОГНОЗ РЕЗУЛЬТАТА С УЧЕТОМ ДОМАШНЕГО СТАДИОНА:")
            try:
                result_prediction = self.predict_match_result_with_home_away(
                    team1_id, team2_id, team1_name, team2_name, tournament_id, season_id
                )
                
                if result_prediction:
                    home_stats = result_prediction['home_stats']
                    away_stats = result_prediction['away_stats']
                    
                    print(f"   🏠 {team1_name} дома:")
                    print(f"      • Побед: {home_stats['win_rate']:.1f}%")
                    print(f"      • Забивает: {home_stats['avg_goals_scored']:.1f} голов")
                    print(f"      • Пропускает: {home_stats['avg_goals_conceded']:.1f} голов")
                    
                    print(f"   🛬 {team2_name} в гостях:")
                    print(f"      • Побед: {away_stats['win_rate']:.1f}%")
                    print(f"      • Забивает: {away_stats['avg_goals_scored']:.1f} голов")
                    print(f"      • Пропускает: {away_stats['avg_goals_conceded']:.1f} голов")
                    
                    print(f"   🎯 Прогноз счета: {result_prediction['predicted_score']}")
                    
                    probs = result_prediction['probabilities']
                    print(f"   📊 Вероятности исходов:")
                    print(f"      • П1 ({team1_name}): {probs['home_win']}%")
                    print(f"      • Ничья: {probs['draw']}%")
                    print(f"      • П2 ({team2_name}): {probs['away_win']}%")
                    
            except Exception as e:
                print(f"   ❌ Ошибка улучшенного прогноза: {e}")

        except Exception as e:
            print(f"❌ Ошибка анализа: {e}")
            import traceback
            traceback.print_exc()

    # СУЩЕСТВУЮЩИЕ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (без изменений)
    def get_team_form_from_db(self, team_id: int, season_id: int) -> List:
        try:
            query = """
            SELECT 
                fm.match_id, fm.home_team_id, fm.away_team_id, 
                fm.home_score, fm.away_score, fm.match_date,
                CASE 
                    WHEN fm.home_team_id = %(team_id)s AND fm.home_score > fm.away_score THEN 'W'
                    WHEN fm.away_team_id = %(team_id)s AND fm.away_score > fm.home_score THEN 'W' 
                    WHEN fm.home_score = fm.away_score THEN 'D'
                    ELSE 'L'
                END as result
            FROM football_matches fm
            WHERE (fm.home_team_id = %(team_id)s OR fm.away_team_id = %(team_id)s)
            AND fm.status = 'Ended'
            AND fm.season_id = %(season_id)s
            ORDER BY fm.match_date DESC
            LIMIT 5
            """
            return self.ch_client.execute(query, {'team_id': team_id, 'season_id': season_id})
        except Exception as e:
            print(f"❌ Ошибка получения формы команды {team_id}: {e}")
            return []

    def get_team_all_time_stats(self, team1_id: int, team2_id: int) -> Dict[str, Any]:
        """Получает историческую статистику только в матчах между командами"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_matches,
                SUM(CASE 
                    WHEN (home_team_id = %(team1)s AND home_score > away_score) 
                    OR (away_team_id = %(team1)s AND away_score > home_score) THEN 1 
                    ELSE 0 
                END) as team1_wins,
                SUM(CASE 
                    WHEN (home_team_id = %(team2)s AND home_score > away_score) 
                    OR (away_team_id = %(team2)s AND away_score > home_score) THEN 1 
                    ELSE 0 
                END) as team2_wins,
                SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END) as draws,
                SUM(CASE WHEN home_team_id = %(team1)s THEN home_score ELSE away_score END) as team1_goals,
                SUM(CASE WHEN home_team_id = %(team2)s THEN home_score ELSE away_score END) as team2_goals,
                AVG(CASE WHEN home_team_id = %(team1)s THEN home_score ELSE away_score END) as team1_avg_goals,
                AVG(CASE WHEN home_team_id = %(team2)s THEN home_score ELSE away_score END) as team2_avg_goals
            FROM football_matches 
            WHERE ((home_team_id = %(team1)s AND away_team_id = %(team2)s)
                OR (home_team_id = %(team2)s AND away_team_id = %(team1)s))
            AND status = 'Ended'
            """
            
            results = self.ch_client.execute(query, {'team1': team1_id, 'team2': team2_id})
            if results:
                total_matches, team1_wins, team2_wins, draws, team1_goals, team2_goals, team1_avg_goals, team2_avg_goals = results[0]
                
                return {
                    'total_matches': total_matches or 0,
                    'team1_wins': team1_wins or 0,
                    'team2_wins': team2_wins or 0,
                    'draws': draws or 0,
                    'team1_goals': team1_goals or 0,
                    'team2_goals': team2_goals or 0,
                    'team1_avg_goals': team1_avg_goals or 0,
                    'team2_avg_goals': team2_avg_goals or 0,
                    'team1_win_rate': (team1_wins / total_matches * 100) if total_matches > 0 else 0,
                    'team2_win_rate': (team2_wins / total_matches * 100) if total_matches > 0 else 0
                }
            return {}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики встреч: {e}")
            return {}

    def get_key_players_progress(self, team_id: int, season_id: int) -> List[Dict]:
        """Получает данные по ключевым игрокам с анализом прогресса"""
        try:
            query = """
            WITH player_totals AS (
                SELECT 
                    player_id,
                    player_name,
                    position,
                    COUNT(*) as matches_played,
                    SUM(goals) as total_goals,
                    SUM(goal_assist) as total_assists,
                    SUM(total_shot) as total_shots,
                    SUM(key_pass) as total_key_passes,
                    AVG(rating) as avg_rating
                FROM football_player_stats fps
                JOIN football_matches fm ON fps.match_id = fm.match_id
                WHERE fps.team_id = %(team_id)s 
                AND fm.season_id = %(season_id)s
                AND fps.minutes_played > 45
                GROUP BY player_id, player_name, position
                HAVING COUNT(*) >= 3
            ),
            player_recent AS (
                SELECT 
                    player_id,
                    AVG(rating) as recent_rating,
                    SUM(goals) as recent_goals,
                    SUM(goal_assist) as recent_assists
                FROM football_player_stats fps
                JOIN football_matches fm ON fps.match_id = fm.match_id
                WHERE fps.team_id = %(team_id)s 
                AND fm.season_id = %(season_id)s
                AND fps.minutes_played > 45
                AND fm.match_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                GROUP BY player_id
            )
            SELECT 
                pt.player_id,
                pt.player_name,
                pt.position,
                pt.matches_played,
                pt.total_goals,
                pt.total_assists,
                pt.total_shots,
                pt.avg_rating,
                COALESCE(pr.recent_rating, 0) as recent_rating,
                COALESCE(pr.recent_goals, 0) as recent_goals,
                COALESCE(pr.recent_assists, 0) as recent_assists
            FROM player_totals pt
            LEFT JOIN player_recent pr ON pt.player_id = pr.player_id
            WHERE pt.total_goals + pt.total_assists > 0
            OR pt.avg_rating > 7.0
            ORDER BY (pt.total_goals + pt.total_assists) DESC, pt.avg_rating DESC
            LIMIT 5
            """
            
            results = self.ch_client.execute(query, {'team_id': team_id, 'season_id': season_id})
            key_players = []
            
            for row in results:
                player_id, name, position, matches, goals, assists, shots, avg_rating, recent_rating, recent_goals, recent_assists = row
                
                if recent_rating > avg_rating + 0.3:
                    trend = 'up'
                elif recent_rating < avg_rating - 0.3:
                    trend = 'down' 
                else:
                    trend = 'stable'
                
                key_players.append({
                    'id': player_id,
                    'name': name,
                    'position': position,
                    'matches': matches,
                    'goals': goals,
                    'assists': assists,
                    'shots': shots,
                    'avg_rating': avg_rating,
                    'recent_rating': recent_rating,
                    'trend': trend
                })
            
            return key_players
            
        except Exception as e:
            print(f"❌ Ошибка получения ключевых игроков: {e}")
            return []

    def generate_insights(self, team1_name: str, team2_name: str, team1_stats: Dict, team2_stats: Dict,
                     team1_pos: str, team2_pos: str, team1_matches: int, team2_matches: int,
                     team1_xg: float = 0.0, team2_xg: float = 0.0) -> List[str]:
        """Генерирует ключевые инсайты"""
        insights = []
        
        team1_goals_pm = self.safe_divide(team1_stats.get('goalsScored', 0), team1_matches)
        team2_goals_pm = self.safe_divide(team2_stats.get('goalsScored', 0), team2_matches)
        team1_efficiency = team1_goals_pm - team1_xg
        team2_efficiency = team2_goals_pm - team2_xg
        
        if abs(team1_efficiency) > 0.3:
            if team1_efficiency > 0:
                insights.append(f"{team1_name} показывает высокую реализацию (+{team1_efficiency:.2f} xG)")
            else:
                insights.append(f"{team1_name} имеет проблемы с реализацией ({team1_efficiency:.2f} xG)")
                
        if abs(team2_efficiency) > 0.3:
            if team2_efficiency > 0:
                insights.append(f"{team2_name} показывает высокую реализацию (+{team2_efficiency:.2f} xG)")
            else:
                insights.append(f"{team2_name} имеет проблемы с реализацией ({team2_efficiency:.2f} xG)")
        
        if team1_pos != "N/A" and team2_pos != "N/A":
            pos1 = int(team1_pos)
            pos2 = int(team2_pos)
            pos_diff = abs(pos1 - pos2)
            
            if pos_diff >= 5:
                if pos1 < pos2:
                    insights.append(f"{team1_name} значительно выше в таблице (-{pos_diff} позиций)")
                else:
                    insights.append(f"{team2_name} значительно выше в таблице (-{pos_diff} позиций)")

        possession_diff = abs(team1_stats.get('averageBallPossession', 0) - team2_stats.get('averageBallPossession', 0))
        if possession_diff > 10:
            if team1_stats.get('averageBallPossession', 0) > team2_stats.get('averageBallPossession', 0):
                insights.append(f"{team1_name} доминирует в контроле мяча (+{possession_diff:.1f}%)")
            else:
                insights.append(f"{team2_name} доминирует в контроле мяча (+{possession_diff:.1f}%)")

        team1_defense_pm = self.safe_divide(team1_stats.get('goalsConceded', 0), team1_matches)
        team2_defense_pm = self.safe_divide(team2_stats.get('goalsConceded', 0), team2_matches)
        
        if team1_defense_pm < team2_defense_pm - 0.3:
            insights.append(f"{team1_name} надежнее в обороне ({team1_defense_pm:.1f} vs {team2_defense_pm:.1f} пропущенных)")
        elif team2_defense_pm < team1_defense_pm - 0.3:
            insights.append(f"{team2_name} надежнее в обороне ({team2_defense_pm:.1f} vs {team1_defense_pm:.1f} пропущенных)")

        return insights[:5] or ["Обе команды показывают сбалансированную игру"]