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
                print(f"\n📊 Обрабатываем матч {match_id}: {match['home_team_name']} vs {match['away_team_name']}")
                
                try:
                    # Получаем полные данные матча
                    match_complete_data = await self.get_complete_match_data(match_id)
                    
                    if match_complete_data:
                        match_data = {
                            'match_info': match,
                            'player_stats': match_complete_data['player_stats'],
                            'incidents': match_complete_data['incidents'],
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
    
    async def get_complete_match_data(self, match_id: int) -> Dict[str, Any]:
        """Получает полные данные матча (статистику + инциденты)"""
        try:
            # Параллельно получаем статистику и инциденты
            player_stats_task = self.get_player_statistics(match_id)
            incidents_task = self.get_match_incidents(match_id)
            
            player_stats, incidents = await asyncio.gather(
                player_stats_task, 
                incidents_task,
                return_exceptions=True
            )
            
            # Проверяем ошибки
            if isinstance(player_stats, Exception):
                print(f"❌ Ошибка статистики матча {match_id}: {player_stats}")
                player_stats = []
            if isinstance(incidents, Exception):
                print(f"❌ Ошибка инцидентов матча {match_id}: {incidents}")
                incidents = []
            
            # Объединяем карточки со статистикой
            player_stats_with_cards = self._merge_cards_with_stats(player_stats, incidents)
            
            return {
                'player_stats': player_stats_with_cards,
                'incidents': incidents,
                'match_id': match_id
            }
            
        except Exception as e:
            print(f"❌ Ошибка получения полных данных матча {match_id}: {e}")
            return {}
    
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
                    card_data = {
                        'match_id': match_id,
                        'player_id': incident.get('player', {}).get('id'),
                        'player_name': incident.get('playerName'),
                        'team_is_home': incident.get('isHome', False),
                        'card_type': incident_class,  # 'yellow' или 'red'
                        'reason': incident.get('reason', ''),
                        'time': incident.get('time', 0),
                        'added_time': incident.get('addedTime', 0)
                    }
                    incidents_data.append(card_data)
                    print(f"🟨 Найдена карточка: {incident_class} - {incident.get('playerName')}")
            
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
                elif card_type == 'red' or card_type == 'yellowRed':  # ДОБАВЬТЕ yellowRed
                    cards_by_player[player_id]['red'] += 1
        
        # Обновляем статистику игроков
        for player in player_stats:
            player_id = player.get('player_id')
            if player_id in cards_by_player:
                player['yellow_cards'] = cards_by_player[player_id]['yellow']
                player['red_cards'] = cards_by_player[player_id]['red']
            else:
                # Если карточек нет, устанавливаем 0
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
                # Очищаем данные от None значений
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

    async def get_player_statistics(self, match_id: int) -> List[Dict[str, Any]]:
        """Получает статистику всех игроков матча"""
        try:
            endpoint = f"/event/{match_id}/lineups"
            data = await self.api._get(endpoint)
            
            player_stats = []
            
            # Обрабатываем домашнюю команду
            if 'home' in data:
                player_stats.extend(self._process_team_players(data['home'], match_id))
            
            # Обрабатываем гостевую команду
            if 'away' in data:
                player_stats.extend(self._process_team_players(data['away'], match_id))
            
            print(f"Получена статистика {len(player_stats)} игроков матча {match_id}")
            return player_stats
            
        except Exception as e:
            print(f"Ошибка получения статистики матча {match_id}: {e}")
            return []
    
    def _process_team_players(self, team_data: Dict[str, Any], match_id: int) -> List[Dict[str, Any]]:
        """Обрабатывает статистику игроков команды"""
        players_stats = []
        
        for player_data in team_data.get('players', []):
            # Берем teamId ТОЛЬКО из player_data
            team_id = player_data.get('teamId')
            if team_id is None:
                player_name = player_data.get('player', {}).get('name', 'Unknown')
                print(f"⚠️  У игрока {player_name} нет teamId! Пропускаем.")
                continue
                
            stats = self._extract_player_stats(player_data, match_id, team_id)
            if stats:
                players_stats.append(stats)
        
        print(f"📊 Обработано {len(players_stats)} игроков")
        return players_stats
    
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
                
                # Основные показатели
                'goals': statistics.get('goals', 0),
                'goal_assist': statistics.get('goalAssist', 0),
                
                # Статистика ударов
                'total_shot': total_shots,
                'on_target_shot': shots_on_target,
                'off_target_shot': shots_off_target,
                'blocked_scoring_attempt': blocked_shots,
                
                # Передачи
                'total_pass': statistics.get('totalPass', 0),
                'accurate_pass': statistics.get('accuratePass', 0),
                'pass_accuracy': round((statistics.get('accuratePass', 0) / statistics.get('totalPass', 1) * 100), 2) if statistics.get('totalPass', 0) > 0 else 0,
                'key_pass': statistics.get('keyPass', 0),
                'total_long_balls': statistics.get('totalLongBalls', 0),
                'accurate_long_balls': statistics.get('accurateLongBalls', 0),
                
                # Дриблинг
                'successful_dribbles': statistics.get('successfulDribbles', 0),
                'dribble_success': round((statistics.get('successfulDribbles', 0) / (statistics.get('successfulDribbles', 0) + statistics.get('unsuccessfulDribbles', 1)) * 100), 2) if statistics.get('successfulDribbles', 0) > 0 else 0,
                
                # Оборона
                'total_tackle': statistics.get('totalTackle', 0),
                'interception_won': statistics.get('interceptionWon', 0),
                'total_clearance': statistics.get('totalClearance', 0),
                'outfielder_block': statistics.get('outfielderBlock', 0),
                'challenge_lost': statistics.get('challengeLost', 0),
                
                # Единоборства
                'duel_won': statistics.get('duelWon', 0),
                'duel_lost': statistics.get('duelLost', 0),
                'aerial_won': statistics.get('aerialWon', 0),
                'duel_success': round((statistics.get('duelWon', 0) / (statistics.get('duelWon', 0) + statistics.get('duelLost', 1)) * 100), 2) if statistics.get('duelWon', 0) > 0 else 0,
                
                # Прочее
                'touches': statistics.get('touches', 0),
                'possession_lost_ctrl': statistics.get('possessionLostCtrl', 0),
                'was_fouled': statistics.get('wasFouled', 0),
                'fouls': statistics.get('fouls', 0),
                
                # Дисциплина (будут обновлены в merge_cards_with_stats)
                'yellow_cards': statistics.get('yellowCards', 0),
                'red_cards': statistics.get('redCards', 0),
                
                # Вратарская статистика
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