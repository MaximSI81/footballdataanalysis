from sofascore_wrapper.api import SofascoreAPI
from datetime import datetime
from typing import List, Dict, Any

class FootballDataCollector:
    def __init__(self):
        self.api = None

    async def __aenter__(self):
        self.api = SofascoreAPI()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.api:
            await self.api.close()
    
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
                    'home_score': home_score if home_score is not None else 0,  # Заменяем None
                    'away_score': away_score if away_score is not None else 0,  # Заменяем None
                    'status': match['status']['description'],
                    'venue': venue if venue is not None else 'Неизвестно',  # Заменяем None
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
            
            # team_id уже пришел как число
            print(f"👤 Игрок: {player_info.get('name')}, team_id={team_id}")
            
            if not player_info or not statistics:
                return None
        
            # Основные показатели
            total_passes = statistics.get('totalPass', 0)
            accurate_passes = statistics.get('accuratePass', 0)
            pass_accuracy = round((accurate_passes / total_passes * 100), 2) if total_passes > 0 else 0
            
            total_duels = statistics.get('duelWon', 0) + statistics.get('duelLost', 0)
            duel_success = round((statistics.get('duelWon', 0) / total_duels * 100), 2) if total_duels > 0 else 0
            
            total_dribbles = statistics.get('successfulDribbles', 0) + statistics.get('unsuccessfulDribbles', 0)
            dribble_success = round((statistics.get('successfulDribbles', 0) / total_dribbles * 100), 2) if total_dribbles > 0 else 0
            
            
            
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
                
                # Удары
                'total_shot': statistics.get('totalShot', 0),
                'on_target_shot': statistics.get('onTargetShot', 0),
                'off_target_shot': statistics.get('offTargetShot', 0),
                'blocked_scoring_attempt': statistics.get('blockedScoringAttempt', 0),
                
                # Передачи
                'total_pass': total_passes,
                'accurate_pass': accurate_passes,
                'pass_accuracy': pass_accuracy,
                'key_pass': statistics.get('keyPass', 0),
                'total_long_balls': statistics.get('totalLongBalls', 0),
                'accurate_long_balls': statistics.get('accurateLongBalls', 0),
                
                # Дриблинг
                'successful_dribbles': statistics.get('successfulDribbles', 0),
                'dribble_success': dribble_success,
                
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
                'duel_success': duel_success,
                
                # Прочее
                'touches': statistics.get('touches', 0),
                'possession_lost_ctrl': statistics.get('possessionLostCtrl', 0),
                'was_fouled': statistics.get('wasFouled', 0),
                'fouls': statistics.get('fouls', 0),
                
                # Дисциплина
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
            return None
