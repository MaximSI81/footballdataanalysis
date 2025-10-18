import pandas as pd
from clickhouse_driver import Client
import os
import asyncio
from sofascore_wrapper.api import SofascoreAPI
from typing import Dict, Any, List, Tuple

class AdvancedFootballAnalyzer:
    def __init__(self):
        self.api = None
        self.ch_client = None

    async def __aenter__(self):
        self.api = SofascoreAPI()
        self.ch_client = Client(
            host='localhost',
            user='username', 
            password='password',
            database='football_db'
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.api:
            await self.api.close()

    async def get_team_stats_from_api(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает статистику команды из API - правильный эндпоинт"""
        try:
            endpoint = f"/team/{team_id}/unique-tournament/{tournament_id}/season/{season_id}/statistics/overall"
            data = await self.api._get(endpoint)
            return data.get('statistics', {}) if data else {}
        except Exception as e:
            print(f"❌ Ошибка получения статистики команды {team_id}: {e}")
            return {}

    async def get_team_position_data(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает данные о позиции команды в таблице - правильный эндпоинт"""
        try:
            endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data"
            data = await self.api._get(endpoint)
            return data or {}
        except Exception as e:
            print(f"❌ Ошибка получения позиции команды {team_id}: {e}")
            return {}

    async def get_current_standings(self, tournament_id: int, season_id: int) -> Dict[int, int]:
        """Получает текущую таблицу турнира"""
        try:
            endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/standings/total"
            data = await self.api._get(endpoint)
            
            standings = {}
            if data and 'standings' in data:
                for row in data['standings']:
                    if 'rows' in row:
                        for team_row in row['rows']:
                            team_id = team_row['team']['id']
                            position = team_row['position']
                            standings[team_id] = position
            return standings
        except Exception as e:
            print(f"❌ Ошибка получения таблицы: {e}")
            return {}
    def get_team_xg_from_db(self, team1_id: int, team2_id: int, season_id: int) -> Tuple[float, float]:
        """Получает xG статистику команд из БД"""
        try:
            query = """
            SELECT 
                team_id,
                AVG(expected_goals) as avg_xg
            FROM football_match_stats
            WHERE team_id IN (%(team1)s, %(team2)s)
            AND match_id IN (
                SELECT match_id FROM football_matches WHERE season_id = %(season_id)s
            )
            GROUP BY team_id
            """
            results = self.ch_client.execute(query, {'team1': team1_id, 'team2': team2_id, 'season_id': season_id})
            
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

    def analyze_position_trend(self, performance_data: Dict) -> Tuple[str, str]:
        """Анализирует тренд позиции команды из performance graph"""
        if not performance_data or 'graphData' not in performance_data:
            return "N/A", ""
            
        graph_data = performance_data['graphData']
        if not graph_data:
            return "N/A", ""
            
        # Берем последние 3 позиции для анализа тренда
        valid_positions = [point['position'] for point in graph_data if 'position' in point and point['position'] is not None]
        if len(valid_positions) < 2:
            return str(valid_positions[0]) if valid_positions else "N/A", ""
        
        current_pos = valid_positions[-1]
        previous_pos = valid_positions[-2] if len(valid_positions) >= 2 else valid_positions[0]
        
        # Анализируем изменение относительно предыдущей позиции
        if current_pos < previous_pos:
            trend = "🟢 улучшение"
        elif current_pos > previous_pos:
            trend = "🔴 ухудшение"
        else:
            trend = "🟡 стабильно"
            
        return str(current_pos), trend

    async def get_match_analysis(self, team1_id: int, team2_id: int, team1_name: str, team2_name: str, 
                               tournament_id: int = 203, season_id: int = 77142):
        """Расширенный анализ матча с учетом положения в таблице"""
        
        print(f"🎯 РАСШИРЕННЫЙ АНАЛИЗ МАТЧА: {team1_name} vs {team2_name}")
        print("=" * 60)
        
        try:
            # 1. Получаем текущую таблицу турнира
            standings = await self.get_current_standings(tournament_id, season_id)
            
            team1_position = standings.get(team1_id, "N/A")
            team2_position = standings.get(team2_id, "N/A")
            
            # 2. Получаем статистику из API (правильные эндпоинты)
            team1_stats = await self.get_team_stats_from_api(team1_id, tournament_id, season_id)
            team2_stats = await self.get_team_stats_from_api(team2_id, tournament_id, season_id)
            
            # 3. Получаем данные о динамике позиции
            team1_performance = await self.get_team_position_data(team1_id, tournament_id, season_id)
            team2_performance = await self.get_team_position_data(team2_id, tournament_id, season_id)
            
            team1_pos_trend = self.analyze_position_trend(team1_performance)
            team2_pos_trend = self.analyze_position_trend(team2_performance)

            # 4. Получаем исторические данные из БД
            team1_form = self.get_team_form_from_db(team1_id, season_id)
            team2_form = self.get_team_form_from_db(team2_id, season_id)
            h2h_data = self.get_h2h_from_db(team1_id, team2_id)
            home_away_stats = self.get_home_away_stats_from_db(team1_id, team2_id)

            # Обработка данных из API
            team1_matches = team1_stats.get('matches', 1)
            team2_matches = team2_stats.get('matches', 1)

            # Расчет показателей на матч из правильных полей API
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
                    class_confidence = "🟡 Средняя"
                elif position_diff <= 6:
                    class_analysis = "Умеренное преимущество" 
                    class_confidence = "🟢 Низкая"
                else:
                    class_analysis = "Значительное преимущество"
                    class_confidence = "🔴 Высокая"
                print(f"   Разница в классе: {position_diff} позиций ({class_analysis})")
            else:
                position_diff = 0
                class_analysis = "Данные недоступны"
                class_confidence = "🟡 Средняя"

            # ОСНОВНАЯ СТАТИСТИКА ИЗ API
            print(f"\n⚽ РЕЗУЛЬТАТИВНОСТЬ (на основе {team1_matches} матчей):")
            print(f"   {team1_name}: {team1_goals_pm:.1f} голов за матч")
            print(f"   {team2_name}: {team2_goals_pm:.1f} голов за матч")

            print(f"\n🎯 УДАРЫ:")
            print(f"   {team1_name}: {team1_shots_pm:.1f} ударов ({team1_shots_on_target_pm:.1f} в створ) за матч")
            print(f"   {team2_name}: {team2_shots_pm:.1f} ударов ({team2_shots_on_target_pm:.1f} в створ) за матч")
            # xG АНАЛИЗ
            print(f"\n📈 РЕАЛЬНЫЙ xG АНАЛИЗ (из статистики матча):")
            print(f"   {team1_name}: {team1_xg:.2f} xG")
            print(f"   {team2_name}: {team2_xg:.2f} xG")

            # ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ
            team1_efficiency = team1_goals_pm - team1_xg
            team2_efficiency = team2_goals_pm - team2_xg

            print(f"\n🎯 ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ:")
            print(f"   {team1_name}: {team1_goals_pm:.1f} голов при {team1_xg:.1f} xG ({team1_efficiency:+.2f})")
            print(f"   {team2_name}: {team2_goals_pm:.1f} голов при {team2_xg:.1f} xG ({team2_efficiency:+.2f})")
            # КАЧЕСТВО МОМЕНТОВ ИЗ API
            team1_big_chances_pm = self.safe_divide(team1_stats.get('bigChances', 0), team1_matches)
            team2_big_chances_pm = self.safe_divide(team2_stats.get('bigChances', 0), team2_matches)
            team1_big_chances_missed_pm = self.safe_divide(team1_stats.get('bigChancesMissed', 0), team1_matches)
            team2_big_chances_missed_pm = self.safe_divide(team2_stats.get('bigChancesMissed', 0), team2_matches)

            print(f"\n🎪 КАЧЕСТВО МОМЕНТОВ:")
            print(f"   {team1_name}: {team1_big_chances_pm:.1f} больших шансов ({team1_big_chances_missed_pm:.1f} пропущено)")
            print(f"   {team2_name}: {team2_big_chances_pm:.1f} больших шансов ({team2_big_chances_missed_pm:.1f} пропущено)")

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

            # СТАНДАРТНЫЕ ПОЛОЖЕНИЯ
            team1_corners_pm = self.safe_divide(team1_stats.get('corners', 0), team1_matches)
            team2_corners_pm = self.safe_divide(team2_stats.get('corners', 0), team2_matches)

            print(f"\n🎪 СТАНДАРТНЫЕ ПОЛОЖЕНИЯ:")
            print(f"   {team1_name}: {team1_corners_pm:.1f} угловых за матч")
            print(f"   {team2_name}: {team2_corners_pm:.1f} угловых за матч")

            # АНАЛИЗ ЗОН АТАК И УЯЗВИМОСТЕЙ
            print(f"\n🎯 АНАЛИЗ ЗОН АТАК И УЯЗВИМОСТЕЙ:")

            # Анализ атакующих предпочтений
            team1_goals_inside = team1_stats.get('goalsFromInsideTheBox', 0)
            team1_goals_outside = team1_stats.get('goalsFromOutsideTheBox', 0)
            team1_goals_head = team1_stats.get('headedGoals', 0)
            team1_total_goals = team1_stats.get('goalsScored', 1)

            team2_goals_inside = team2_stats.get('goalsFromInsideTheBox', 0)
            team2_goals_outside = team2_stats.get('goalsFromOutsideTheBox', 0)
            team2_goals_head = team2_stats.get('headedGoals', 0)
            team2_total_goals = team2_stats.get('goalsScored', 1)

            print(f"\n🏹 {team1_name} АТАКУЕТ:")
            print(f"   • {self.safe_divide(team1_goals_inside, team1_total_goals) * 100:.0f}% голов изнутри штрафной")
            print(f"   • {self.safe_divide(team1_goals_outside, team1_total_goals) * 100:.0f}% голов издали")
            print(f"   • {self.safe_divide(team1_goals_head, team1_total_goals) * 100:.0f}% голов головой")

            print(f"\n🏹 {team2_name} АТАКУЕТ:")
            print(f"   • {self.safe_divide(team2_goals_inside, team2_total_goals) * 100:.0f}% голов изнутри штрафной")
            print(f"   • {self.safe_divide(team2_goals_outside, team2_total_goals) * 100:.0f}% голов издали")
            print(f"   • {self.safe_divide(team2_goals_head, team2_total_goals) * 100:.0f}% голов головой")

            # Анализ уязвимостей в обороне
            team1_shots_inside_against = team1_stats.get('shotsFromInsideTheBoxAgainst', 0)
            team1_shots_outside_against = team1_stats.get('shotsFromOutsideTheBoxAgainst', 0)
            team1_crosses_against = team1_stats.get('crossesSuccessfulAgainst', 0)
            team1_total_shots_against = team1_shots_inside_against + team1_shots_outside_against

            team2_shots_inside_against = team2_stats.get('shotsFromInsideTheBoxAgainst', 0)
            team2_shots_outside_against = team2_stats.get('shotsFromOutsideTheBoxAgainst', 0)
            team2_crosses_against = team2_stats.get('crossesSuccessfulAgainst', 0)
            team2_total_shots_against = team2_shots_inside_against + team2_shots_outside_against

            print(f"\n🛡️ {team1_name} ЗАЩИЩАЕТСЯ:")
            print(f"   • {self.safe_divide(team1_shots_inside_against, team1_total_shots_against) * 100:.0f}% ударов пропускает изнутри штрафной")
            print(f"   • {self.safe_divide(team1_shots_outside_against, team1_total_shots_against) * 100:.0f}% ударов пропускает издали")
            print(f"   • {team1_crosses_against} успешных навесов пропущено")

            print(f"\n🛡️ {team2_name} ЗАЩИЩАЕТСЯ:")
            print(f"   • {self.safe_divide(team2_shots_inside_against, team2_total_shots_against) * 100:.0f}% ударов пропускает изнутри штрафной")
            print(f"   • {self.safe_divide(team2_shots_outside_against, team2_total_shots_against) * 100:.0f}% ударов пропускает издали")
            print(f"   • {team2_crosses_against} успешных навесов пропущено")

            # Тактические выводы
            print(f"\n🎪 ТАКТИЧЕСКИЕ ВЫВОДЫ:")

            tactical_insights = []

            # Анализ атакующих сильных сторон против оборонительных слабостей
            if team1_goals_inside > team1_goals_outside and team2_shots_inside_against > team2_shots_outside_against:
                tactical_insights.append(f"{team1_name} может эффективно атаковать изнутри штрафной против уязвимой обороны {team2_name}")

            if team1_goals_head > 0 and team2_crosses_against > 10:
                tactical_insights.append(f"{team1_name} может использовать навесы против слабой игры {team2_name} в воздухе")

            if team2_goals_outside > team2_goals_inside and team1_shots_outside_against > team1_shots_inside_against:
                tactical_insights.append(f"{team2_name} может опасно бить издали против уязвимости {team1_name} на дальних дистанциях")

            # Анализ стандартных положений
            team1_corners_pm = self.safe_divide(team1_stats.get('corners', 0), team1_matches)
            team2_corners_pm = self.safe_divide(team2_stats.get('corners', 0), team2_matches)

            if team1_corners_pm > 5 and team2_goals_head > 0:
                tactical_insights.append(f"Ожидаем опасные моменты после угловых - {team1_name} много подает, {team2_name} хорошо играет головой")

            # Анализ контратак
            team1_fast_breaks = team1_stats.get('fastBreaks', 0)
            team2_fast_breaks = team2_stats.get('fastBreaks', 0)

            if team1_fast_breaks > team2_fast_breaks + 2:
                tactical_insights.append(f"{team1_name} опаснее в быстрых контратаках")
            elif team2_fast_breaks > team1_fast_breaks + 2:
                tactical_insights.append(f"{team2_name} опаснее в быстрых контратаках")

            for i, insight in enumerate(tactical_insights[:3], 1):
                print(f"   {i}. {insight}")

            if not tactical_insights:
                print("   • Ожидаем тактическую битву в центре поля")

            # ДАННЫЕ ИЗ БД
            print(f"\n📈 ФОРМА (последние 5 матчей):")
            if team1_form:
                team1_form_results = [match[6] for match in team1_form]
                # Разворачиваем порядок - от старых к новым (последний матч справа)
                team1_form_display = team1_form_results[::-1]
                print(f"   {team1_name}: {''.join(['🟢' if r == 'W' else '🟡' if r == 'D' else '🔴' for r in team1_form_display])}")
                print(f"   Последние результаты: {', '.join([r for r in team1_form_display])}")
            else:
                print(f"   {team1_name}: нет данных")
                
            if team2_form:
                team2_form_results = [match[6] for match in team2_form]
                # Разворачиваем порядок - от старых к новым (последний матч справа)
                team2_form_display = team2_form_results[::-1]
                print(f"   {team2_name}: {''.join(['🟢' if r == 'W' else '🟡' if r == 'D' else '🔴' for r in team2_form_display])}")
                print(f"   Последние результаты: {', '.join([r for r in team2_form_display])}")
            else:
                print(f"   {team2_name}: нет данных")

            # ИСТОРИЯ ЛИЧНЫХ ВСТРЕЧ
            print(f"\n📊 ИСТОРИЯ ЛИЧНЫХ ВСТРЕЧ ({team1_name} vs {team2_name}):")

            # Статистика только матчей между командами
            h2h_all_time = self.get_team_all_time_stats(team1_id, team2_id)

            if h2h_all_time:
                print(f"\n🤝 ВСЕГО МАТЧЕЙ: {h2h_all_time['total_matches']}")
                print(f"   • {team1_name}: {h2h_all_time['team1_wins']} побед ({h2h_all_time['team1_win_rate']:.1f}%)")
                print(f"   • {team2_name}: {h2h_all_time['team2_wins']} побед ({h2h_all_time['team2_win_rate']:.1f}%)")
                print(f"   • Ничьих: {h2h_all_time['draws']} ({h2h_all_time['draws']/h2h_all_time['total_matches']*100:.1f}%)")
                
                print(f"\n⚽ ОБЩАЯ РЕЗУЛЬТАТИВНОСТЬ:")
                print(f"   • {team1_name}: {h2h_all_time['team1_goals']} голов")
                print(f"   • {team2_name}: {h2h_all_time['team2_goals']} голов")
                print(f"   • Разница: +{h2h_all_time['team1_goals'] - h2h_all_time['team2_goals']}")
                
                print(f"\n📈 СРЕДНИЕ ПОКАЗАТЕЛИ ЗА МАТЧ:")
                print(f"   • {team1_name}: {h2h_all_time['team1_avg_goals']:.1f} голов")
                print(f"   • {team2_name}: {h2h_all_time['team2_avg_goals']:.1f} голов")
                print(f"   • Всего голов за матч: {h2h_all_time['team1_avg_goals'] + h2h_all_time['team2_avg_goals']:.1f}")

                # Анализ доминирования
                win_rate_diff = h2h_all_time['team1_win_rate'] - h2h_all_time['team2_win_rate']
                goals_diff = h2h_all_time['team1_avg_goals'] - h2h_all_time['team2_avg_goals']
                
                print(f"\n⚖️ БАЛАНС СИЛ В ЛИЧНЫХ ВСТРЕЧАХ:")
                if abs(win_rate_diff) > 20:
                    if win_rate_diff > 0:
                        print(f"   • {team1_name} доминирует в личных встречах (+{win_rate_diff:.1f}% побед)")
                    else:
                        print(f"   • {team2_name} доминирует в личных встречах (+{abs(win_rate_diff):.1f}% побед)")
                elif abs(win_rate_diff) <= 10:
                    print(f"   • Команды примерно равны в личных встречах")
                else:
                    print(f"   • Умеренное преимущество одной из команд")

                if abs(goals_diff) > 0.5:
                    if goals_diff > 0:
                        print(f"   • {team1_name} значительно результативнее (+{goals_diff:.1f} гола)")
                    else:
                        print(f"   • {team2_name} значительно результативнее (+{abs(goals_diff):.1f} гола)")
            else:
                print(f"   • Нет данных о личных встречах")

            # ДОМАШНИЕ/ГОСТЕВЫЕ ПОКАЗАТЕЛИ
            if home_away_stats:
                team1_home = next((s for s in home_away_stats if s[0] == team1_id and s[1] == 'home'), None)
                team2_away = next((s for s in home_away_stats if s[0] == team2_id and s[1] == 'away'), None)
                
                if team1_home and team2_away:
                    print(f"\n🏠 ДОМАШНИЕ/ГОСТЕВЫЕ ПОКАЗАТЕЛИ:")
                    print(f"   {team1_name} дома: {team1_home[3]:.1f}⚽ пропускает {team1_home[4]:.1f}🥅")
                    print(f"   {team2_name} в гостях: {team2_away[3]:.1f}⚽ пропускает {team2_away[4]:.1f}🥅")

            # ПРОГНОЗЫ С УЧЕТОМ ПОЗИЦИИ
            total_goals = team1_goals_pm + team2_goals_pm
            total_big_chances = team1_big_chances_pm + team2_big_chances_pm
            
            print(f"\n🏆 ПРОГНОЗ С УЧЕТОМ ПОЗИЦИИ В ТАБЛИЦЕ:")
            print(f"   • Ожидаемые голы: {total_goals:.1f}")
            print(f"   • Качество моментов: {total_big_chances:.1f} больших шансов")
            if position_diff > 0:
                print(f"   • Разница в классе: {position_diff} позиций")
            # АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ
            print(f"\n⭐ АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ:")

            key_players_insights = []

            # Получаем данные по ключевым игрокам
            team1_key_players = self.get_key_players_progress(team1_id, season_id)
            team2_key_players = self.get_key_players_progress(team2_id, season_id)

            print(f"\n🔑 {team1_name} - ключевые игроки:")
            if team1_key_players:
                for player in team1_key_players[:3]:  # Показываем топ-3
                    trend_icon = "📈" if player['trend'] == 'up' else "📉" if player['trend'] == 'down' else "➡️"
                    print(f"   • {player['name']} ({player['position']}) {trend_icon}")
                    print(f"     Голы: {player['goals']} | Ассисты: {player['assists']} | Удары: {player['shots']}")
                    print(f"     Прогресс: {player['progress_note']}")
            else:
                print(f"   • Нет данных о ключевых игроках")

            print(f"\n🔑 {team2_name} - ключевые игрока:")
            if team2_key_players:
                for player in team2_key_players[:3]:  # Показываем топ-3
                    trend_icon = "📈" if player['trend'] == 'up' else "📉" if player['trend'] == 'down' else "➡️"
                    print(f"   • {player['name']} ({player['position']}) {trend_icon}")
                    print(f"     Голы: {player['goals']} | Ассисты: {player['assists']} | Удары: {player['shots']}")
                    print(f"     Прогресс: {player['progress_note']}")
            else:
                print(f"   • Нет данных о ключевых игроках")

            # Анализ индивидуальных противостояний
            if team1_key_players and team2_key_players:
                print(f"\n⚔️ КЛЮЧЕВЫЕ ПРОТИВОСТОЯНИЯ:")
                
                # Нападающие vs Защитники
                team1_attackers = [p for p in team1_key_players if p['position'] in ['F', 'FW', 'ST', 'CF']]
                team2_defenders = [p for p in team2_key_players if p['position'] in ['D', 'DF', 'CB', 'FB']]
                
                if team1_attackers and team2_defenders:
                    attacker = team1_attackers[0]
                    defender = team2_defenders[0]
                    if attacker['trend'] == 'up' and defender['trend'] == 'down':
                        key_players_insights.append(f"{attacker['name']} в форме против слабеющего {defender['name']}")
                    elif attacker['trend'] == 'down' and defender['trend'] == 'up':
                        key_players_insights.append(f"{defender['name']} может нейтрализовать {attacker['name']}")

                # Полузащитники
                team1_midfielders = [p for p in team1_key_players if p['position'] in ['M', 'MF', 'CM', 'AM', 'DM']]
                team2_midfielders = [p for p in team2_key_players if p['position'] in ['M', 'MF', 'CM', 'AM', 'DM']]
                
                if team1_midfielders and team2_midfielders:
                    mf1 = team1_midfielders[0]
                    mf2 = team2_midfielders[0]
                    if mf1['assists'] > 2 and mf2['trend'] == 'down':
                        key_players_insights.append(f"{mf1['name']} может создавать моменты против слабого центра {team2_name}")

            for i, insight in enumerate(key_players_insights[:2], 1):
                print(f"   {i}. {insight}")
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
                # Прогноз без данных о позиции
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
                team1_xg, team2_xg  # добавляем xG параметры
            )
            
            for i, insight in enumerate(insights, 1):
                print(f"   {i}. {insight}")

        except Exception as e:
            print(f"❌ Ошибка анализа: {e}")
            import traceback
            traceback.print_exc()


    
    
    def get_team_recent_stats(self, team_id: int, limit: int = 10) -> List[Dict]:
        """Получает статистику последних матчей команды (вся история)"""
        try:
            query = """
            SELECT 
                fm.match_id,
                fm.home_team_id,
                fm.away_team_id,
                fm.home_score,
                fm.away_score,
                fm.match_date,
                fm.season_id,
                CASE 
                    WHEN fm.home_team_id = %(team_id)s AND fm.home_score > fm.away_score THEN 'W'
                    WHEN fm.away_team_id = %(team_id)s AND fm.away_score > fm.home_score THEN 'W' 
                    WHEN fm.home_score = fm.away_score THEN 'D'
                    ELSE 'L'
                END as result,
                CASE 
                    WHEN fm.home_team_id = %(team_id)s THEN fm.home_score
                    ELSE fm.away_score
                END as goals_for,
                CASE 
                    WHEN fm.home_team_id = %(team_id)s THEN fm.away_score
                    ELSE fm.home_score
                END as goals_against
            FROM football_matches fm
            WHERE (fm.home_team_id = %(team_id)s OR fm.away_team_id = %(team_id)s)
            AND fm.status = 'Ended'
            ORDER BY fm.match_date DESC
            LIMIT %(limit)s
            """
            
            results = self.ch_client.execute(query, {'team_id': team_id, 'limit': limit})
            matches = []
            
            for row in results:
                matches.append({
                    'match_id': row[0],
                    'home_team_id': row[1],
                    'away_team_id': row[2],
                    'home_score': row[3],
                    'away_score': row[4],
                    'match_date': row[5],
                    'season_id': row[6],
                    'result': row[7],
                    'goals_for': row[8],
                    'goals_against': row[9]
                })
            
            return matches
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики команды {team_id}: {e}")
            return []

    def get_detailed_h2h_stats(self, team1_id: int, team2_id: int) -> List[Dict]:
        """Получает подробную статистику личных встреч (вся история)"""
        try:
            query = """
            SELECT 
                fm.match_id,
                fm.home_team_id,
                fm.away_team_id,
                fm.home_score,
                fm.away_score,
                fm.match_date,
                fm.venue,
                fm.season_id,
                CASE 
                    WHEN fm.home_score > fm.away_score THEN fm.home_team_id
                    WHEN fm.away_score > fm.home_score THEN fm.away_team_id
                    ELSE 'draw'
                END as winner
            FROM football_matches fm
            WHERE ((fm.home_team_id = %(team1)s AND fm.away_team_id = %(team2)s)
                OR (fm.home_team_id = %(team2)s AND fm.away_team_id = %(team1)s))
            AND fm.status = 'Ended'
            ORDER BY fm.match_date DESC
            """
            
            results = self.ch_client.execute(query, {'team1': team1_id, 'team2': team2_id})
            matches = []
            
            for row in results:
                matches.append({
                    'match_id': row[0],
                    'home_team_id': row[1],
                    'away_team_id': row[2],
                    'home_score': row[3],
                    'away_score': row[4],
                    'match_date': row[5],
                    'venue': row[6],
                    'season_id': row[7],
                    'winner': row[8]
                })
            
            return matches
            
        except Exception as e:
            print(f"❌ Ошибка получения H2H статистики: {e}")
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
                HAVING COUNT(*) >= 3  # Игроки, сыгравшие минимум 3 матча
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
                AND fm.match_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)  # Последние 30 дней
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
            WHERE pt.total_goals + pt.total_assists > 0  # Игроки с гол+пас
            OR pt.avg_rating > 7.0  # Или с высоким рейтингом
            ORDER BY (pt.total_goals + pt.total_assists) DESC, pt.avg_rating DESC
            LIMIT 5
            """
            
            results = self.ch_client.execute(query, {'team_id': team_id, 'season_id': season_id})
            key_players = []
            
            for row in results:
                player_id, name, position, matches, goals, assists, shots, avg_rating, recent_rating, recent_goals, recent_assists = row
                
                # Анализ прогресса
                if recent_rating > avg_rating + 0.3:
                    trend = 'up'
                    progress_note = "Улучшение формы"
                elif recent_rating < avg_rating - 0.3:
                    trend = 'down' 
                    progress_note = "Спад формы"
                else:
                    trend = 'stable'
                    progress_note = "Стабильная игра"
                
                # Дополнительные индикаторы прогресса
                recent_contribution = recent_goals + recent_assists
                total_contribution = goals + assists
                
                if recent_contribution > total_contribution / matches:
                    progress_note += ", рост продуктивности"
                elif recent_contribution < total_contribution / matches:
                    progress_note += ", снижение продуктивности"
                
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
                    'trend': trend,
                    'progress_note': progress_note
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
        
        # Расчет эффективности
        team1_goals_pm = self.safe_divide(team1_stats.get('goalsScored', 0), team1_matches)
        team2_goals_pm = self.safe_divide(team2_stats.get('goalsScored', 0), team2_matches)
        team1_efficiency = team1_goals_pm - team1_xg
        team2_efficiency = team2_goals_pm - team2_xg
        
        # Инсайты на основе xG эффективности
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
        
        # Инсайты на основе позиции
        if team1_pos != "N/A" and team2_pos != "N/A":
            pos1 = int(team1_pos)
            pos2 = int(team2_pos)
            pos_diff = abs(pos1 - pos2)
            
            if pos_diff >= 5:
                if pos1 < pos2:
                    insights.append(f"{team1_name} значительно выше в таблице (-{pos_diff} позиций)")
                else:
                    insights.append(f"{team2_name} значительно выше в таблице (-{pos_diff} позиций)")

        # Инсайты на основе эффективности атаки
        team1_big_chances = team1_stats.get('bigChances', 0)
        team2_big_chances = team2_stats.get('bigChances', 0)
        
        team1_big_efficiency = self.safe_divide(team1_goals_pm, team1_big_chances, 0.0) if team1_big_chances > 0 else 0.0
        team2_big_efficiency = self.safe_divide(team2_goals_pm, team2_big_chances, 0.0) if team2_big_chances > 0 else 0.0
        
        if team1_big_efficiency > team2_big_efficiency + 0.1:
            insights.append(f"{team1_name} эффективнее реализует моменты ({team1_big_efficiency:.1%} vs {team2_big_efficiency:.1%})")
        elif team2_big_efficiency > team1_big_efficiency + 0.1:
            insights.append(f"{team2_name} эффективнее реализует моменты ({team2_big_efficiency:.1%} vs {team1_big_efficiency:.1%})")

        # Анализ владения
        possession_diff = abs(team1_stats.get('averageBallPossession', 0) - team2_stats.get('averageBallPossession', 0))
        if possession_diff > 10:
            if team1_stats.get('averageBallPossession', 0) > team2_stats.get('averageBallPossession', 0):
                insights.append(f"{team1_name} доминирует в контроле мяча (+{possession_diff:.1f}%)")
            else:
                insights.append(f"{team2_name} доминирует в контроле мяча (+{possession_diff:.1f}%)")

        # Анализ обороны
        team1_goals_conceded = team1_stats.get('goalsConceded', 0)
        team2_goals_conceded = team2_stats.get('goalsConceded', 0)
        
        team1_defense_pm = self.safe_divide(team1_goals_conceded, team1_matches)
        team2_defense_pm = self.safe_divide(team2_goals_conceded, team2_matches)
        
        if team1_defense_pm < team2_defense_pm - 0.3:
            insights.append(f"{team1_name} надежнее в обороне ({team1_defense_pm:.1f} vs {team2_defense_pm:.1f} пропущенных)")
        elif team2_defense_pm < team1_defense_pm - 0.3:
            insights.append(f"{team2_name} надежнее в обороне ({team2_defense_pm:.1f} vs {team1_defense_pm:.1f} пропущенных)")

        return insights[:5] or ["Обе команды показывают сбалансированную игру"]

    # Методы для работы с БД (остаются без изменений)
    def get_team_form_from_db(self, team_id: int, season_id: int) -> List:
        """Получает форму команды из БД"""
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

'''# Запуск анализа
async def main():
    async with AdvancedFootballAnalyzer() as analyzer:
        await analyzer.get_match_analysis(
            team1_id=2315,  # Динамо Москва
            team2_id=5131,  # Ахмат Грозный
            team1_name="Динамо Москва", 
            team2_name="Ахмат Грозный",
            tournament_id=203,  # РПЛ
            season_id=77142
        )

if __name__ == "__main__":
    asyncio.run(main())'''