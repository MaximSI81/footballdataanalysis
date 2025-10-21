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

    def get_team_stats_from_db(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику команды из кэш-таблицы"""
        try:
            query = """
            SELECT 
                matches_played, goals_scored, goals_conceded, avg_possession,
                avg_shots, avg_shots_on_target, avg_xg, avg_corners,
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
                    'expectedGoals': result[0][6],
                    'corners': result[0][7],
                    'fouls': result[0][8],
                    'yellowCards': result[0][9],
                    'bigChances': result[0][10],
                    'bigChancesMissed': result[0][11],
                    'goalsFromInsideTheBox': result[0][12],
                    'goalsFromOutsideTheBox': result[0][13],
                    'headedGoals': result[0][14],
                    'accuratePassesPercentage': result[0][15],
                    'fastBreaks': result[0][16]
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
            AND updated_at >= NOW() - INTERVAL 1 DAY
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
        """Получает xG статистику команд из основной таблицы статистики"""
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
        
    async def get_players_analysis(self, team1_id: int, team2_id: int, team1_name: str, team2_name: str, 
                              tournament_id: int, season_id: int):
        """Анализ только ключевых игроков"""
        
        print(f"⭐ АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ:")
        print("=" * 50)
        
        try:
            # Получаем данные о ключевых игроках
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
            import traceback
            traceback.print_exc()

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

            # ОБОРОНА
            team1_goals_conceded_pm = self.safe_divide(team1_stats.get('goalsConceded', 0), team1_matches)
            team2_goals_conceded_pm = self.safe_divide(team2_stats.get('goalsConceded', 0), team2_matches)

            print(f"\n🛡️ ОБОРОНА:")
            print(f"   {team1_name}: {team1_goals_conceded_pm:.1f} пропущенных за матч")
            print(f"   {team2_name}: {team2_goals_conceded_pm:.1f} пропущенных за матч")

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
            # РАСЧЕТ ПРОЦЕНТОВ С НОРМАЛИЗАЦИЕЙ
            team1_inside = self.safe_divide(team1_stats.get('goalsFromInsideTheBox', 0), team1_total_goals) * 100
            team1_outside = self.safe_divide(team1_stats.get('goalsFromOutsideTheBox', 0), team1_total_goals) * 100  
            team1_headed = self.safe_divide(team1_stats.get('headedGoals', 0), team1_total_goals) * 100

            # НОРМАЛИЗАЦИЯ ПРОЦЕНТОВ
            total_percent = team1_inside + team1_outside + team1_headed
            if total_percent > 100:
                team1_inside = (team1_inside / total_percent) * 100
                team1_outside = (team1_outside / total_percent) * 100
                team1_headed = (team1_headed / total_percent) * 100

            print(f"   • {team1_inside:.0f}% голов изнутри штрафной")
            print(f"   • {team1_outside:.0f}% голов издали")
            print(f"   • {team1_headed:.0f}% голов головой")

            print(f"\n🏹 {team2_name} АТАКУЕТ:")
            # ТАКЖЕ ДЛЯ ВТОРОЙ КОМАНДЫ
            team2_inside = self.safe_divide(team2_stats.get('goalsFromInsideTheBox', 0), team2_total_goals) * 100
            team2_outside = self.safe_divide(team2_stats.get('goalsFromOutsideTheBox', 0), team2_total_goals) * 100  
            team2_headed = self.safe_divide(team2_stats.get('headedGoals', 0), team2_total_goals) * 100

            # НОРМАЛИЗАЦИЯ ПРОЦЕНТОВ
            total_percent = team2_inside + team2_outside + team2_headed
            if total_percent > 100:
                team2_inside = (team2_inside / total_percent) * 100
                team2_outside = (team2_outside / total_percent) * 100
                team2_headed = (team2_headed / total_percent) * 100

            print(f"   • {team2_inside:.0f}% голов изнутри штрафной")
            print(f"   • {team2_outside:.0f}% голов издали")
            print(f"   • {team2_headed:.0f}% голов головой")

            # ДАННЫЕ ИЗ БД
            matches_count = len(team1_form) if team1_form else 0

            if matches_count == 0:
                print(f"\n📈 ФОРМА (первый тур - матчей нет)")
            else:
                form_text = "матч" if matches_count == 1 else f"последние {matches_count} матча" if matches_count < 5 else "последние 5 матчей"
                print(f"\n📈 ФОРМА (за {form_text}):")

            if team1_form:
                team1_form_results = [match[6] for match in team1_form]
                team1_form_display = team1_form_results[::-1]  # Переворачиваем чтобы последний матч был справа
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

            # ПРОГНОЗЫ
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

        except Exception as e:
            print(f"❌ Ошибка анализа: {e}")
            import traceback
            traceback.print_exc()

    # Методы для работы с БД (остаются без изменений)
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

    def get_h2h_from_db(self, team1_id: int, team2_id: int) -> List:
        """Получает исторические встречи из БД"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_matches,
                SUM(CASE 
                    WHEN fm.home_team_id = %(team1)s AND fm.home_score > fm.away_score THEN 1
                    WHEN fm.away_team_id = %(team1)s AND fm.away_score > fm.home_score THEN 1
                    ELSE 0 
                END) as team1_wins,
                SUM(CASE 
                    WHEN fm.home_team_id = %(team2)s AND fm.home_score > fm.away_score THEN 1
                    WHEN fm.away_team_id = %(team2)s AND fm.away_score > fm.home_score THEN 1
                    ELSE 0 
                END) as team2_wins,
                SUM(CASE WHEN fm.home_score = fm.away_score THEN 1 ELSE 0 END) as draws,
                AVG(fm.home_score + fm.away_score) as avg_goals
            FROM football_matches fm
            WHERE ((fm.home_team_id = %(team1)s AND fm.away_team_id = %(team2)s)
                OR (fm.home_team_id = %(team2)s AND fm.away_team_id = %(team1)s))
              AND fm.status = 'Ended'
            """
            return self.ch_client.execute(query, {'team1': team1_id, 'team2': team2_id})
        except Exception as e:
            print(f"❌ Ошибка получения H2H: {e}")
            return []

    def get_home_away_stats_from_db(self, team1_id: int, team2_id: int) -> List:
        """Получает домашние/гостевые показатели из БД"""
        try:
            query = """
            SELECT 
                team_id, venue, matches, goals, conceded
            FROM (
                SELECT home_team_id as team_id, 'home' as venue, COUNT(*) as matches,
                       AVG(home_score) as goals, AVG(away_score) as conceded
                FROM football_matches WHERE home_team_id = %(team1)s AND status = 'Ended' GROUP BY home_team_id
                UNION ALL
                SELECT away_team_id as team_id, 'away' as venue, COUNT(*) as matches,
                       AVG(away_score) as goals, AVG(home_score) as conceded  
                FROM football_matches WHERE away_team_id = %(team1)s AND status = 'Ended' GROUP BY away_team_id
                UNION ALL
                SELECT home_team_id as team_id, 'home' as venue, COUNT(*) as matches,
                       AVG(home_score) as goals, AVG(away_score) as conceded
                FROM football_matches WHERE home_team_id = %(team2)s AND status = 'Ended' GROUP BY home_team_id
                UNION ALL
                SELECT away_team_id as team_id, 'away' as venue, COUNT(*) as matches,
                       AVG(away_score) as goals, AVG(home_score) as conceded
                FROM football_matches WHERE away_team_id = %(team2)s AND status = 'Ended' GROUP BY away_team_id
            )
            ORDER BY team_id, venue
            """
            return self.ch_client.execute(query, {'team1': team1_id, 'team2': team2_id})
        except Exception as e:
            print(f"❌ Ошибка получения домашних/гостевых stats: {e}")
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
