from sofascore_wrapper.api import SofascoreAPI
from datetime import datetime
from typing import List, Dict, Any
import asyncio

class FootballDataCollector:
    def __init__(self):
        self.api = None

    async def __aenter__(self):
        self.api = SofascoreAPI()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.api:
            await self.api.close()

    async def collect_round_data(self, tournament_id: int, season_id: int, round_number: int) -> Dict[str, Any]:
        """Собирает полные данные по всем матчам тура"""
        try:
            print(f"\n🎯 Начинаем сбор данных для тура {round_number}")
            print("=" * 50)
            
            # 1. Получаем матчи тура
            matches = await self.get_round_matches(tournament_id, season_id, round_number)
            
            if not matches:
                print(f"❌ Нет матчей для тура {round_number}")
                return {}
            
            round_data = {
                'tournament_id': tournament_id,
                'season_id': season_id, 
                'round_number': round_number,
                'matches': [],
                'total_matches': len(matches),
                'collected_matches': 0
            }
            
            # 2. Для каждого матча собираем полные данные
            for match in matches:
                match_id = match['match_id']
                home_team_id = match['home_team_id']
                away_team_id = match['away_team_id']
                home_team_name = match['home_team_name']
                away_team_name = match['away_team_name']
                
                print(f"\n📊 Обрабатываем матч {match_id}: {home_team_name} vs {away_team_name}")
                
                try:
                    # Получаем полные данные матча
                    match_complete_data = await self.get_complete_match_data(
                        match_id, home_team_id, away_team_id, home_team_name, away_team_name
                    )
                                        
                    if match_complete_data:
                        match_data = {
                            'match_info': match,
                            'player_stats': match_complete_data['player_stats'],
                            'incidents': match_complete_data['incidents'],
                            'match_stats': match_complete_data['match_stats'],
                            'collected_at': datetime.now()
                        }
                        round_data['matches'].append(match_data)
                        round_data['collected_matches'] += 1
                        print(f"✅ Матч {match_id} обработан успешно")
                    else:
                        print(f"⚠️  Не удалось собрать данные для матча {match_id}")
                        
                    # Небольшая задержка чтобы не перегружать API
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f"❌ Ошибка обработки матча {match_id}: {e}")
                    continue
            
            print(f"\n🎉 Сбор данных завершен! Обработано {round_data['collected_matches']}/{round_data['total_matches']} матчей")
            return round_data
            
        except Exception as e:
            print(f"❌ Критическая ошибка сбора данных тура {round_number}: {e}")
            return {}

    async def get_team_season_stats(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает полную статистику команды за сезон"""
        try:
            endpoint = f"/team/{team_id}/unique-tournament/{tournament_id}/season/{season_id}/statistics/overall"
            print(f"📊 Запрос статистики команды: {endpoint}")
            
            data = await self.api._get(endpoint)
            
            if data and 'statistics' in data:
                print(f"✅ Получена статистика команды {team_id}")
                return data['statistics']
            return {}
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики команды {team_id}: {e}")
            return {}

    async def get_tournament_standings(self, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает турнирную таблицу"""
        try:
            endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/standings/total"
            print(f"🏆 Запрос турнирной таблицы: {endpoint}")
            
            data = await self.api._get(endpoint)
            return data or {}
            
        except Exception as e:
            print(f"❌ Ошибка получения турнирной таблицы: {e}")
            return {}

    async def get_team_performance_graph(self, team_id: int, tournament_id: int, season_id: int) -> Dict[str, Any]:
        """Получает историческую динамику позиций команды"""
        try:
            endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data"
            print(f"📈 Запрос графика производительности команды: {endpoint}")
            
            data = await self.api._get(endpoint)
            return data or {}
            
        except Exception as e:
            print(f"❌ Ошибка получения графика производительности команды {team_id}: {e}")
            return {}

    async def get_complete_match_data(self, match_id: int, home_team_id: int, away_team_id: int, home_team_name: str, away_team_name: str) -> Dict[str, Any]:
        """Получает полные данные матча (статистику + инциденты + общую статистику)"""
        try:
            # Получаем состав матча
            endpoint = f"/event/{match_id}/lineups"
            lineups_data = await self.api._get(endpoint)
            
            # Получаем инциденты
            incidents = await self.get_match_incidents(match_id)
            
            # Получаем общую статистику матча
            match_stats = await self.get_match_statistics(match_id, home_team_id, away_team_id, home_team_name, away_team_name)
            
            # Обрабатываем статистику игроков
            player_stats = []
            
            # Обрабатываем домашнюю команду
            if 'home' in lineups_data:
                for player_data in lineups_data['home'].get('players', []):
                    stats = self._extract_player_stats(player_data, match_id, home_team_id)
                    if stats:
                        player_stats.append(stats)
                print(f"🏠 Обработано {len(lineups_data['home'].get('players', []))} игроков домашней команды")
            
            # Обрабатываем гостевую команду
            if 'away' in lineups_data:
                for player_data in lineups_data['away'].get('players', []):
                    stats = self._extract_player_stats(player_data, match_id, away_team_id)
                    if stats:
                        player_stats.append(stats)
                print(f"🛬 Обработано {len(lineups_data['away'].get('players', []))} игроков гостевой команды")
            
            # Объединяем карточки со статистикой
            player_stats_with_cards = self._merge_cards_with_stats(player_stats, incidents)
            
            return {
                'player_stats': player_stats_with_cards,
                'incidents': incidents,
                'match_stats': match_stats,
                'match_id': match_id
            }
            
        except Exception as e:
            print(f"❌ Ошибка получения полных данных матча {match_id}: {e}")
            return {}

    async def get_match_statistics(self, match_id: int, home_team_id: int, away_team_id: int, home_team_name: str, away_team_name: str):
        """Получает общую статистику матча"""
        try:
            endpoint = f"/event/{match_id}/statistics"
            print(f"📊 Запрос статистики: {endpoint}")
            
            data = await self.api._get(endpoint)
            
            stats_data = []
            
            if not data or 'statistics' not in data:
                print("❌ Нет данных статистики в ответе")
                return []
            
            # Создаем базовые структуры данных для команд
            home_stats = self._create_empty_stats(match_id, home_team_id, home_team_name, 'home')
            away_stats = self._create_empty_stats(match_id, away_team_id, away_team_name, 'away')
            
            # Обрабатываем статистику для каждого периода (ALL - общая)
            for period in data.get('statistics', []):
                if period['period'] == 'ALL':  # Берем общую статистику
                    print(f"🔍 Обрабатываем общую статистику периода ALL")
                    
                    for group in period.get('groups', []):
                        group_name = group['groupName']
                        print(f"   📋 Группа: {group_name}")
                        
                        # Обрабатываем текущую группу и объединяем с существующими данными
                        group_stats = self._extract_match_stats(
                            group['statisticsItems'], 
                            match_id,
                            home_team_id, away_team_id,
                            home_team_name, away_team_name,
                            home_stats, away_stats
                        )
                        
                        # Обновляем данные
                        home_stats = group_stats[0]
                        away_stats = group_stats[1]
                        print(f"   ✅ Обработана группа {group_name}")
                
                # Также обрабатываем другие периоды если нужно
                elif period['period'] in ['1ST', '2ND']:
                    print(f"🔍 Пропускаем период {period['period']} - используем только ALL")
            
            # Финальный расчет точности передач
            self._calculate_pass_accuracy(home_stats, away_stats)
            
            stats_data = [home_stats, away_stats]
            
            # Финальная проверка данных
            print(f"✅ ФИНАЛЬНЫЕ ДАННЫЕ статистики:")
            for stat in stats_data:
                team_name = stat['team_name']
                non_zero_fields = len([v for v in stat.values() if v not in [0, 0.0, None, '']])
                print(f"   {team_name}: {non_zero_fields} ненулевых полей")
                if stat.get('errors_lead_to_shot') or stat.get('errors_lead_to_goal'):
                    print(f"   🎯 Ошибки: shots={stat.get('errors_lead_to_shot')}, goals={stat.get('errors_lead_to_goal')}")
                if stat.get('total_passes') > 0:
                    print(f"   📊 Передачи: {stat['total_passes']} всего, {stat['accurate_passes']} точных ({stat['pass_accuracy']}%)")
        
            print(f"✅ Получено {len(stats_data)} записей статистики для матча {match_id}")
            return stats_data
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики матча {match_id}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _create_empty_stats(self, match_id, team_id, team_name, team_type):
        """Создает словарь с пустой статистикой"""
        return {
            'match_id': match_id, 
            'team_id': team_id, 
            'team_name': team_name, 
            'team_type': team_type,
            'ball_possession': 0.0,
            'expected_goals': 0.0,
            'total_shots': 0,
            'shots_on_target': 0,
            'shots_off_target': 0,
            'blocked_shots': 0,
            'corners': 0,
            'free_kicks': 0,
            'fouls': 0,
            'yellow_cards': 0,
            'big_chances': 0,
            'big_chances_scored': 0,
            'big_chances_missed': 0,
            'shots_inside_box': 0,
            'shots_outside_box': 0,
            'touches_in_penalty_area': 0,
            'total_passes': 0,
            'accurate_passes': 0,
            'pass_accuracy': 0.0,
            'total_crosses': 0,
            'accurate_crosses': 0,
            'total_long_balls': 0,
            'accurate_long_balls': 0,
            'tackles': 0,
            'tackles_won_percent': 0.0,
            'interceptions': 0,
            'recoveries': 0,
            'clearances': 0,
            'errors_lead_to_shot': 0,
            'errors_lead_to_goal': 0,
            'duel_won_percent': 0,
            'dispossessed': 0,
            'ground_duels_percentage': 0,
            'aerial_duels_percentage': 0,
            'dribbles_percentage': 0,
            'created_at': datetime.now()
        }

    def _extract_match_stats(self, stats_items, match_id, home_team_id, away_team_id, home_team_name, away_team_name, existing_home_stats=None, existing_away_stats=None):
        """Извлекает статистику из items, объединяя с существующими данными"""
        
        # Используем существующие данные или создаем новые
        if existing_home_stats is None:
            home_stats = self._create_empty_stats(match_id, home_team_id, home_team_name, 'home')
        else:
            home_stats = existing_home_stats.copy()
            
        if existing_away_stats is None:
            away_stats = self._create_empty_stats(match_id, away_team_id, away_team_name, 'away')
        else:
            away_stats = existing_away_stats.copy()
        
        print(f"🔍 Обрабатываем {len(stats_items)} показателей статистики...")
        
        for item in stats_items:
            key = item.get('key')
            home_value = item.get('homeValue')
            away_value = item.get('awayValue')
            
            if home_value is None and away_value is None:
                continue
                
            # print(f"   📊 {key}: Домашние={home_value}, Гости={away_value}")
                
            # Обрабатываем все возможные поля
            self._process_stat_item(key, home_value, away_value, home_stats, away_stats)
        
        return [home_stats, away_stats]

    def _process_stat_item(self, key, home_value, away_value, home_stats, away_stats):
        """Обрабатывает один элемент статистики"""
        if key == 'ballPossession':
            home_stats['ball_possession'] = float(home_value) if home_value is not None else 0.0
            away_stats['ball_possession'] = float(away_value) if away_value is not None else 0.0
            
        elif key == 'expectedGoals':
            home_stats['expected_goals'] = float(home_value) if home_value is not None else 0.0
            away_stats['expected_goals'] = float(away_value) if away_value is not None else 0.0
            
        elif key == 'totalShotsOnGoal':
            home_stats['total_shots'] = int(home_value) if home_value is not None else 0
            away_stats['total_shots'] = int(away_value) if away_value is not None else 0
            
        elif key == 'shotsOnGoal':
            home_stats['shots_on_target'] = int(home_value) if home_value is not None else 0
            away_stats['shots_on_target'] = int(away_value) if away_value is not None else 0
            
        elif key == 'shotsOffGoal':
            home_stats['shots_off_target'] = int(home_value) if home_value is not None else 0
            away_stats['shots_off_target'] = int(away_value) if away_value is not None else 0
            
        elif key == 'blockedScoringAttempt':
            home_stats['blocked_shots'] = int(home_value) if home_value is not None else 0
            away_stats['blocked_shots'] = int(away_value) if away_value is not None else 0
            
        elif key == 'cornerKicks':
            home_stats['corners'] = int(home_value) if home_value is not None else 0
            away_stats['corners'] = int(away_value) if away_value is not None else 0
            
        elif key == 'freeKicks':
            home_stats['free_kicks'] = int(home_value) if home_value is not None else 0
            away_stats['free_kicks'] = int(away_value) if away_value is not None else 0
            
        elif key == 'bigChanceCreated':
            home_stats['big_chances'] = int(home_value) if home_value is not None else 0
            away_stats['big_chances'] = int(away_value) if away_value is not None else 0
            
        elif key == 'bigChanceScored':
            home_stats['big_chances_scored'] = int(home_value) if home_value is not None else 0
            away_stats['big_chances_scored'] = int(away_value) if away_value is not None else 0
            
        elif key == 'bigChanceMissed':
            home_stats['big_chances_missed'] = int(home_value) if home_value is not None else 0
            away_stats['big_chances_missed'] = int(away_value) if away_value is not None else 0
            
        elif key == 'totalShotsInsideBox':
            home_stats['shots_inside_box'] = int(home_value) if home_value is not None else 0
            away_stats['shots_inside_box'] = int(away_value) if away_value is not None else 0
            
        elif key == 'totalShotsOutsideBox':
            home_stats['shots_outside_box'] = int(home_value) if home_value is not None else 0
            away_stats['shots_outside_box'] = int(away_value) if away_value is not None else 0
            
        elif key == 'touchesInOppBox':
            home_stats['touches_in_penalty_area'] = int(home_value) if home_value is not None else 0
            away_stats['touches_in_penalty_area'] = int(away_value) if away_value is not None else 0
            
        elif key == 'passes':
            home_stats['total_passes'] = int(home_value) if home_value is not None else 0
            away_stats['total_passes'] = int(away_value) if away_value is not None else 0
            
        elif key == 'accuratePasses':
            home_stats['accurate_passes'] = int(home_value) if home_value is not None else 0
            away_stats['accurate_passes'] = int(away_value) if away_value is not None else 0
            
        elif key == 'accurateCross':
            home_stats['accurate_crosses'] = int(home_value) if home_value is not None else 0
            away_stats['accurate_crosses'] = int(away_value) if away_value is not None else 0
            
        elif key == 'accurateLongBalls':
            home_stats['accurate_long_balls'] = int(home_value) if home_value is not None else 0
            away_stats['accurate_long_balls'] = int(away_value) if away_value is not None else 0
            
        elif key == 'totalTackle':
            home_stats['tackles'] = int(home_value) if home_value is not None else 0
            away_stats['tackles'] = int(away_value) if away_value is not None else 0
            
        elif key == 'wonTacklePercent':
            home_stats['tackles_won_percent'] = float(home_value) if home_value is not None else 0.0
            away_stats['tackles_won_percent'] = float(away_value) if away_value is not None else 0.0
            
        elif key == 'interceptionWon':
            home_stats['interceptions'] = int(home_value) if home_value is not None else 0
            away_stats['interceptions'] = int(away_value) if away_value is not None else 0
            
        elif key == 'ballRecovery':
            home_stats['recoveries'] = int(home_value) if home_value is not None else 0
            away_stats['recoveries'] = int(away_value) if away_value is not None else 0
            
        elif key == 'totalClearance':
            home_stats['clearances'] = int(home_value) if home_value is not None else 0
            away_stats['clearances'] = int(away_value) if away_value is not None else 0
            
        elif key == 'yellowCards':
            home_stats['yellow_cards'] = int(home_value) if home_value is not None else 0
            away_stats['yellow_cards'] = int(away_value) if away_value is not None else 0
            
        elif key == 'duelWonPercent':
            home_stats['duel_won_percent'] = int(home_value) if home_value is not None else 0
            away_stats['duel_won_percent'] = int(away_value) if away_value is not None else 0
            
        elif key == 'dispossessed':
            home_stats['dispossessed'] = int(home_value) if home_value is not None else 0
            away_stats['dispossessed'] = int(away_value) if away_value is not None else 0
            
        elif key == 'groundDuelsPercentage':
            home_stats['ground_duels_percentage'] = int(home_value) if home_value is not None else 0
            away_stats['ground_duels_percentage'] = int(away_value) if away_value is not None else 0
            
        elif key == 'aerialDuelsPercentage':
            home_stats['aerial_duels_percentage'] = int(home_value) if home_value is not None else 0
            away_stats['aerial_duels_percentage'] = int(away_value) if away_value is not None else 0
            
        elif key == 'dribblesPercentage':
            home_stats['dribbles_percentage'] = int(home_value) if home_value is not None else 0
            away_stats['dribbles_percentage'] = int(away_value) if away_value is not None else 0

        # ВАЖНО: Обрабатываем поля ошибок
        elif key == 'errorsLeadToShot':
            home_stats['errors_lead_to_shot'] = int(home_value) if home_value is not None else 0
            away_stats['errors_lead_to_shot'] = int(away_value) if away_value is not None else 0
            print(f"   🎯 УСТАНОВЛЕНО errors_lead_to_shot: Домашние={home_stats['errors_lead_to_shot']}, Гости={away_stats['errors_lead_to_shot']}")
            
        elif key == 'errorsLeadToGoal':
            home_stats['errors_lead_to_goal'] = int(home_value) if home_value is not None else 0
            away_stats['errors_lead_to_goal'] = int(away_value) if away_value is not None else 0
            print(f"   🎯 УСТАНОВЛЕНО errors_lead_to_goal: Домашние={home_stats['errors_lead_to_goal']}, Гости={away_stats['errors_lead_to_goal']}")

    def _calculate_pass_accuracy(self, home_stats, away_stats):
        """Безопасно рассчитывает точность передач"""
        try:
            if home_stats['total_passes'] > 0:
                home_stats['pass_accuracy'] = round((home_stats['accurate_passes'] / home_stats['total_passes']) * 100, 2)
            else:
                home_stats['pass_accuracy'] = 0.0
                
            if away_stats['total_passes'] > 0:
                away_stats['pass_accuracy'] = round((away_stats['accurate_passes'] / away_stats['total_passes']) * 100, 2)
            else:
                away_stats['pass_accuracy'] = 0.0
        except Exception as e:
            print(f"⚠️  Ошибка расчета точности передач: {e}")
            home_stats['pass_accuracy'] = 0.0
            away_stats['pass_accuracy'] = 0.0

    async def get_match_incidents(self, match_id: int) -> List[Dict[str, Any]]:
        """Получает инциденты матча (карточки, голы, замены)"""
        try:
            endpoint = f"/event/{match_id}/incidents"
            print(f"🔍 Запрос инцидентов: {endpoint}")
            
            data = await self.api._get(endpoint)
            
            incidents_data = []
            for incident in data.get('incidents', []):
                incident_type = incident.get('incidentType')
                incident_class = incident.get('incidentClass')
                
                # Собираем карточки
                if incident_type == 'card':
                    player_info = incident.get('player', {})
                    player_id = player_info.get('id')
                    player_name = incident.get('playerName')
                    
                    # Пропускаем карточки без player_id (тренеры, помощники)
                    if not player_id:
                        print(f"⚠️  Карточка {incident_class} без player_id: {player_name} (тренер/помощник) - пропускаем")
                        continue
                    
                    # Исправляем время
                    time_value = incident.get('time', 0)
                    if time_value > 65535:
                        time_value = 65535
                    
                    added_time = incident.get('addedTime', 0)
                    if added_time > 65535:
                        added_time = 65535
                    
                    card_data = {
                        'match_id': match_id,
                        'player_id': player_id,
                        'player_name': player_name,
                        'team_is_home': incident.get('isHome', False),
                        'card_type': incident_class,
                        'reason': incident.get('reason', ''),
                        'time': time_value,
                        'added_time': added_time
                    }
                    incidents_data.append(card_data)
                    print(f"🟨 Найдена карточка: {incident_class} - {player_name}")
            
            print(f"✅ Получено {len(incidents_data)} карточек для матча {match_id}")
            return incidents_data
            
        except Exception as e:
            print(f"❌ Ошибка получения инцидентов матча {match_id}: {e}")
            return []

    def _merge_cards_with_stats(self, player_stats: List[Dict], incidents: List[Dict]) -> List[Dict]:
        """Объединяет статистику игроков с данными о карточках"""
        if not player_stats:
            return []
        
        # Создаем словарь для быстрого поиска карточек по игроку
        cards_by_player = {}
        for incident in incidents:
            player_id = incident.get('player_id')
            if player_id:
                if player_id not in cards_by_player:
                    cards_by_player[player_id] = {'yellow': 0, 'red': 0}
                
                card_type = incident.get('card_type')
                if card_type == 'yellow':
                    cards_by_player[player_id]['yellow'] += 1
                elif card_type == 'red' or card_type == 'yellowRed':
                    cards_by_player[player_id]['red'] += 1
        
        # Обновляем статистику игроков
        for player in player_stats:
            player_id = player.get('player_id')
            if player_id in cards_by_player:
                player['yellow_cards'] = cards_by_player[player_id]['yellow']
                player['red_cards'] = cards_by_player[player_id]['red']
            else:
                player['yellow_cards'] = 0
                player['red_cards'] = 0
        
        return player_stats

    async def get_round_matches(self, tournament_id: int, season_id: int, round_number: int) -> List[Dict[str, Any]]:
        """Получает матчи указанного тура"""
        try:
            endpoint = f"/unique-tournament/{tournament_id}/season/{season_id}/events/round/{round_number}"
            print(f"🔍 API запрос: {endpoint}")
            
            data = await self.api._get(endpoint)
            
            matches_data = []
            for match in data.get('events', []):
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
                    'start_timestamp': datetime.fromtimestamp(match['startTimestamp'])
                }
                matches_data.append(match_info)
            
            print(f"✅ Получено {len(matches_data)} матчей тура {round_number}")
            return matches_data
            
        except Exception as e:
            print(f"❌ Ошибка получения матчей тура {round_number}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _extract_player_stats(self, player_data: Dict[str, Any], match_id: int, team_id: int) -> Dict[str, Any]:
        """Извлекает статистику игрока"""
        try:
            player_info = player_data.get('player', {})
            statistics = player_data.get('statistics', {})
            
            if not player_info or not statistics:
                return None
            
            # Статистика ударов
            total_shots = statistics.get('totalShots', 0)
            shots_on_target = statistics.get('onTargetScoringAttempt', 0)
            shots_off_target = statistics.get('offTargetShot', 0)
            blocked_shots = statistics.get('blockedScoringAttempt', 0)
            
            # Если offTargetShot не найден, вычисляем его
            if shots_off_target == 0 and total_shots > 0:
                shots_off_target = total_shots - shots_on_target - blocked_shots
                if shots_off_target < 0:
                    shots_off_target = 0
            
            # Проверка целостности данных
            calculated_total = shots_on_target + shots_off_target + blocked_shots
            if total_shots != calculated_total and total_shots > 0:
                print(f"⚠️  Несоответствие ударов у {player_info.get('name')}: API={total_shots}, calculated={calculated_total}")
                total_shots = max(total_shots, calculated_total)
            
            # Отладочная информация для проверки
            if total_shots > 0:
                print(f"🎯 {player_info.get('name')}: {total_shots} ударов ({shots_on_target} в створ, {shots_off_target} мимо, {blocked_shots} блок)")
            
            return {
                'match_id': match_id,
                'team_id': team_id,
                'player_id': player_info.get('id'),
                'player_name': player_info.get('name'),
                'short_name': player_info.get('shortName'),
                'position': player_info.get('position'),
                'jersey_number': int(player_info.get('jerseyNumber', 0)) if player_info.get('jerseyNumber') else 0,
                'minutes_played': statistics.get('minutesPlayed', 0),
                'rating': float(statistics.get('rating', 0)),
                'goals': statistics.get('goals', 0),
                'goal_assist': statistics.get('goalAssist', 0),
                'total_shot': total_shots,
                'on_target_shot': shots_on_target,
                'off_target_shot': shots_off_target,
                'blocked_scoring_attempt': blocked_shots,
                'total_pass': statistics.get('totalPass', 0),
                'accurate_pass': statistics.get('accuratePass', 0),
                'pass_accuracy': round((statistics.get('accuratePass', 0) / statistics.get('totalPass', 1) * 100), 2) if statistics.get('totalPass', 0) > 0 else 0,
                'key_pass': statistics.get('keyPass', 0),
                'total_long_balls': statistics.get('totalLongBalls', 0),
                'accurate_long_balls': statistics.get('accurateLongBalls', 0),
                'successful_dribbles':  statistics.get('wonContest', 0),
                'dribble_success': statistics.get('totalContest', 0) - statistics.get('wonContest', 0),
                'total_tackle': statistics.get('totalTackle', 0),
                'interception_won': statistics.get('interceptionWon', 0),
                'total_clearance': statistics.get('totalClearance', 0),
                'outfielder_block': statistics.get('outfielderBlock', 0),
                'challenge_lost': statistics.get('challengeLost', 0),
                'duel_won': statistics.get('duelWon', 0),
                'duel_lost': statistics.get('duelLost', 0),
                'aerial_won': statistics.get('aerialWon', 0),
                'duel_success': round((statistics.get('duelWon', 0) / (statistics.get('duelWon', 0) + statistics.get('duelLost', 1)) * 100), 2) if statistics.get('duelWon', 0) > 0 else 0,
                'touches': statistics.get('touches', 0),
                'possession_lost_ctrl': statistics.get('possessionLostCtrl', 0),
                'was_fouled': statistics.get('wasFouled', 0),
                'fouls': statistics.get('fouls', 0),
                'saves': statistics.get('saves', 0),
                'punches': statistics.get('punches', 0),
                'good_high_claim': statistics.get('goodHighClaim', 0),
                'saved_shots_from_inside_box': statistics.get('savedShotsFromInsideTheBox', 0),
            }
            
        except Exception as e:
            print(f"❌ Ошибка извлечения статистики: {e}")
            import traceback
            traceback.print_exc()
            return None