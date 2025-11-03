import pandas as pd
from clickhouse_driver import Client
import os
import asyncio
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta

class PlayersAnalyzer:
    def __init__(self):
        self.ch_client = None
    
    async def __aenter__(self):
        """Инициализация подключения при входе в контекст"""
        self.ch_client = Client(
            host='localhost',
            user='username', 
            password='password',
            database='football_db'
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие подключения при выходе из контекста"""
        if self.ch_client:
            self.ch_client.disconnect()
    
    async def get_team_compact_dashboard(self, team_id: int, team_name: str, season_id: int) -> Dict:
        """Генерирует компактный дашборд формы команды"""
        try:
            # Получаем данные игроков
            players_data = await self._get_team_players_with_form(team_id, season_id)
            
            # Группируем по позициям и рассчитываем общую форму
            position_groups = self._group_players_by_position(players_data)
            overall_form = self._calculate_overall_form(players_data)
            
            return {
                'team_name': team_name,
                'overall_form': overall_form,
                'positions': position_groups
            }
            
        except Exception as e:
            print(f"❌ Ошибка создания дашборда для {team_name}: {e}")
            return {}
    
    async def _get_team_players_with_form(self, team_id: int, season_id: int) -> List[Dict]:
        """Получает игроков команды с анализом формы (только сыгравшие > 0 минут)"""
        try:
            query = """
            SELECT 
                player_id, player_name, position, 
                AVG(rating) as avg_rating,
                SUM(goals) as total_goals,
                SUM(goal_assist) as total_assists,
                AVG(total_shot) as avg_shots,
                AVG(pass_accuracy) as pass_accuracy,
                AVG(duel_won * 100.0 / NULLIF(duel_won + duel_lost, 0)) as duel_success,
                AVG(saves) as avg_saves,
                COUNT(*) as matches_played,
                AVG(minutes_played) as avg_minutes
            FROM football_player_stats 
            WHERE team_id = %(team_id)s 
            AND match_id IN (
                SELECT match_id FROM football_matches 
                WHERE season_id = %(season_id)s AND status = 'Ended'
            )
            AND minutes_played > 0  # Только те, кто выходил на поле
            GROUP BY player_id, player_name, position
            HAVING COUNT(*) >= 1  # Хотя бы 1 матч с минутами
            ORDER BY avg_rating DESC
            """
            
            results = self.ch_client.execute(query, {'team_id': team_id, 'season_id': season_id})
            
            players = []
            for row in results:
                player_id, name, position, rating, goals, assists, shots, pass_acc, duels, saves, matches, avg_minutes = row
                
                # Анализ формы
                form_trend = await self._calculate_player_trend(player_id, season_id)
                
                players.append({
                    'id': player_id,
                    'name': name,
                    'position': position,
                    'rating': round(rating, 1),
                    'goals': goals or 0,
                    'assists': assists or 0,
                    'shots': round(shots or 0, 1),
                    'pass_accuracy': round(pass_acc or 0, 0),
                    'duel_success': round(duels or 0, 0),
                    'saves': round(saves or 0, 1),
                    'form_trend': form_trend,
                    'matches': matches,
                    'avg_minutes': round(avg_minutes or 0, 0)
                })
            
            return players
            
        except Exception as e:
            print(f"❌ Ошибка получения игроков: {e}")
            return []
    
    async def _calculate_player_trend(self, player_id: int, season_id: int) -> Dict:
        """Исправленный расчет тренда со стрелками"""
        try:
            query = """
            SELECT rating, match_date
            FROM football_player_stats fps
            JOIN football_matches fm ON fps.match_id = fm.match_id
            WHERE fps.player_id = %(player_id)s 
            AND fm.season_id = %(season_id)s
            AND fps.minutes_played > 45
            ORDER BY fm.match_date DESC
            LIMIT 5
            """
            
            results = self.ch_client.execute(query, {'player_id': player_id, 'season_id': season_id})
            
            if len(results) < 3:
                return {'percent': 0, 'direction': 'stable', 'icon': '➡️'}
            
            # Сравниваем последние 2 матча vs предыдущие 3 матча
            recent_matches = [row[0] for row in results[:2]]  # 2 самых свежих
            older_matches = [row[0] for row in results[2:5]]  # 3 предыдущих
            
            if len(recent_matches) < 2 or len(older_matches) < 2:
                return {'percent': 0, 'direction': 'stable', 'icon': '➡️'}
            
            recent_avg = sum(recent_matches) / len(recent_matches)
            older_avg = sum(older_matches) / len(older_matches)
            
            # Защита от некорректных значений
            if older_avg < 5.0:
                return {'percent': 0, 'direction': 'stable', 'icon': '➡️'}
            
            trend_percent = ((recent_avg - older_avg) / older_avg) * 100
            
            # Ограничение и классификация со стрелками
            trend_percent = max(-50, min(50, trend_percent))
            
            if trend_percent > 10:
                icon = '📈'
                direction = 'up'
            elif trend_percent < -10:
                icon = '📉' 
                direction = 'down'
            else:
                icon = '➡️'
                direction = 'stable'
            
            return {
                'percent': round(trend_percent, 0),
                'direction': direction,
                'icon': icon
            }
            
        except Exception as e:
            print(f"❌ Ошибка расчета тренда для игрока {player_id}: {e}")
            return {'percent': 0, 'direction': 'stable', 'icon': '➡️'}
    
    def _group_players_by_position(self, players: List[Dict]) -> Dict[str, List]:
        """Группирует игроков по позициям"""
        positions = {
            'forwards': [],
            'midfielders': [],
            'defenders': [],
            'goalkeepers': []
        }
        
        position_mapping = {
            'G': 'goalkeepers',
            'D': 'defenders', 
            'M': 'midfielders',
            'F': 'forwards'
        }
        
        for player in players:
            pos_char = player['position'][0] if player['position'] else 'M'
            category = position_mapping.get(pos_char, 'midfielders')
            positions[category].append(player)
        
        # Сортируем по рейтингу в каждой категории
        for category in positions:
            positions[category].sort(key=lambda x: x['rating'], reverse=True)
        
        return positions
    
    def _calculate_overall_form(self, players: List[Dict]) -> Dict:
        """Рассчитывает общую форму команды со стрелками"""
        if not players:
            return {'rating': 0, 'trend_icon': '➡️', 'trend_text': '0%'}
        
        total_rating = sum(p['rating'] for p in players)
        avg_rating = total_rating / len(players)
        
        # Анализ тренда команды
        trends = [p['form_trend']['percent'] for p in players if p.get('form_trend')]
        avg_trend = sum(trends) / len(trends) if trends else 0
        
        # Используем стрелки вместо цветных кружков
        if avg_trend > 3:
            trend_icon = '📈'
            trend_text = f"+{abs(avg_trend):.0f}%"
        elif avg_trend < -3:
            trend_icon = '📉'
            trend_text = f"-{abs(avg_trend):.0f}%"
        else:
            trend_icon = '➡️'
            trend_text = f"{avg_trend:+.0f}%"
        
        return {
            'rating': round(avg_rating, 1),
            'trend_icon': trend_icon,
            'trend_text': trend_text
        }
    
    def format_compact_dashboard(self, team_data: Dict) -> str:
        """Форматирует данные в читаемый дашборд со стрелками тренда"""
        if not team_data:
            return "❌ Данные недоступны"
        
        output = []
        team_name = team_data['team_name']
        overall = team_data['overall_form']
        positions = team_data['positions']
        
        # Заголовок команды
        output.append(f"🏷️ {team_name.upper()}")
        output.append(f"Общий рейтинг: {overall['rating']}/10 ({overall['trend_text']})")
        output.append("")
        
        # Позиции с улучшенным форматированием
        position_config = {
            'forwards': {'icon': '⚽', 'name': 'НАПАДАЮЩИЕ'},
            'midfielders': {'icon': '🎯', 'name': 'ПОЛУЗАЩИТНИКИ'}, 
            'defenders': {'icon': '🛡️', 'name': 'ЗАЩИТНИКИ'},
            'goalkeepers': {'icon': '🥅', 'name': 'ВРАТАРИ'}
        }
        
        for pos_key, config in position_config.items():
            players = positions.get(pos_key, [])
            if players:
                # Расчет средней оценки для позиции
                pos_rating = sum(p['rating'] for p in players) / len(players)
                
                output.append(f"{config['icon']} {config['name']} ({pos_rating:.1f}/10)")
                output.append("-" * 45)
                
                for player in players[:8]:  # Ограничиваем до 8 игроков на позицию
                    trend = player['form_trend']
                    trend_emoji = trend['icon']  # 📈, 📉, ➡️
                    
                    # Форматирование в зависимости от позиции
                    if pos_key == 'goalkeepers':
                        line = (f"  {player['name']:<18} ⭐{player['rating']} "
                               f"({trend['percent']:+.0f}%) {trend_emoji} | "
                               f"{player['saves']} сейв | {player['pass_accuracy']}%пас")
                    elif pos_key == 'defenders':
                        line = (f"  {player['name']:<18} ⭐{player['rating']} "
                               f"({trend['percent']:+.0f}%) {trend_emoji} | "
                               f"{player['duel_success']}%ЕБ | {player['shots']} отб")
                    else:
                        line = (f"  {player['name']:<18} ⭐{player['rating']} "
                               f"({trend['percent']:+.0f}%) {trend_emoji} | "
                               f"{player['goals']}г+{player['assists']}п | {player['shots']} уд")
                    
                    output.append(line)
                
                output.append("")
        
        return "\n".join(output)