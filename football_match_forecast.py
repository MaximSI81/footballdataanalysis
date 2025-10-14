import pandas as pd
from clickhouse_driver import Client

def get_match_analysis_from_clickhouse(team1_id, team2_id, team1_name, team2_name, season_id=77142):
    """Сводный анализ матча с REAL xG данными, формой и историческими встречами"""
    
    ch_client = Client(
        host='localhost',
        user='username', 
        password='password',
        database='football_db'
    )
    
    # ОСНОВНОЙ ЗАПРОС
    correct_query = """
    WITH match_totals AS (
        SELECT 
            fps.match_id,
            fps.team_id, 
            SUM(fps.goals) as team_goals,
            SUM(fps.total_shot) as team_shots,
            SUM(fps.on_target_shot) as team_shots_on_target, 
            SUM(fps.fouls) as team_fouls,
            AVG(fps.pass_accuracy) as team_pass_accuracy,
            SUM(fps.total_tackle) as team_tackles,
            COUNT(DISTINCT fps.player_id) as players_count
        FROM football_player_stats fps
        JOIN football_matches fm ON fps.match_id = fm.match_id
        WHERE fps.minutes_played > 0
          AND fps.team_id IN (%(team1)s, %(team2)s)
          AND fm.season_id = %(season_id)s
        GROUP BY fps.match_id, fps.team_id
        HAVING players_count >= 8
    ),
    team_avg AS (
        SELECT 
            team_id,
            AVG(team_goals) as avg_goals_per_match,
            AVG(team_shots) as avg_shots_per_match, 
            AVG(team_shots_on_target) as avg_shots_on_target_per_match,
            AVG(team_fouls) as avg_fouls_per_match,
            AVG(team_pass_accuracy) as avg_pass_accuracy,
            AVG(team_tackles) as avg_tackles_per_match,
            COUNT(*) as matches_analyzed
        FROM match_totals
        GROUP BY team_id
    )
    SELECT 
        t1.team_id as team1_id,
        t2.team_id as team2_id,
        ROUND(t1.avg_goals_per_match, 2) as team1_goals,
        ROUND(t2.avg_goals_per_match, 2) as team2_goals,
        ROUND(t1.avg_shots_per_match, 1) as team1_shots,
        ROUND(t2.avg_shots_per_match, 1) as team2_shots,
        ROUND(t1.avg_shots_on_target_per_match, 1) as team1_shots_on_target,
        ROUND(t2.avg_shots_on_target_per_match, 1) as team2_shots_on_target,
        ROUND(t1.avg_fouls_per_match, 1) as team1_fouls,
        ROUND(t2.avg_fouls_per_match, 1) as team2_fouls,
        ROUND(t1.avg_pass_accuracy, 1) as team1_pass_acc,
        ROUND(t2.avg_pass_accuracy, 1) as team2_pass_acc,
        ROUND(t1.avg_tackles_per_match, 1) as team1_tackles,
        ROUND(t2.avg_tackles_per_match, 1) as team2_tackles,
        t1.matches_analyzed as team1_matches,
        t2.matches_analyzed as team2_matches
    FROM team_avg t1, team_avg t2
    WHERE t1.team_id = %(team1)s AND t2.team_id = %(team2)s
    """
    
    # ЗАПРОС ФОРМЫ КОМАНД
    form_query = """
    SELECT 
        fm.match_id,
        fm.home_team_id,
        fm.away_team_id, 
        fm.home_score,
        fm.away_score,
        fm.match_date,
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
    
    # ЗАПРОС ИСТОРИЧЕСКИХ ВСТРЕЧ
    h2h_query = """
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
    
   # ИСПРАВЛЕННЫЙ ЗАПРОС ДОМАШНИХ/ГОСТЕВЫХ
    home_away_query = """
    SELECT 
        team_id,
        venue,
        matches,
        ROUND(goals, 2) as goals,
        ROUND(shots, 1) as shots
    FROM (
        -- Домашние матчи
        SELECT 
            home_team_id as team_id,
            'home' as venue,
            COUNT(*) as matches,
            AVG(home_score) as goals,
            NULL as shots  -- или получи из статистики если есть
        FROM football_matches 
        WHERE home_team_id IN (%(team1)s, %(team2)s)
        AND status = 'Ended'
        GROUP BY home_team_id
        
        UNION ALL
        
        -- Гостевые матчи  
        SELECT 
            away_team_id as team_id,
            'away' as venue, 
            COUNT(*) as matches,
            AVG(away_score) as goals,
            NULL as shots
        FROM football_matches
        WHERE away_team_id IN (%(team1)s, %(team2)s)
        AND status = 'Ended'
        GROUP BY away_team_id
    )
    ORDER BY team_id, venue
    """
    
    # ЗАПРОС КАРТОЧЕК
    cards_query = """
    SELECT 
        fps.team_id,
        COUNT(*) as total_cards,
        COUNT(DISTINCT fc.match_id) as matches_with_cards
    FROM football_cards fc
    JOIN football_player_stats fps ON (
        fc.player_id = fps.player_id AND 
        fc.match_id = fps.match_id
    )
    JOIN football_matches fm ON fc.match_id = fm.match_id
    WHERE fps.team_id IN (%(team1)s, %(team2)s)
      AND fm.season_id = %(season_id)s
    GROUP BY fps.team_id
    """
    
    params = {
        'team1': team1_id, 
        'team2': team2_id,
        'season_id': season_id
    }
    
    try:
        print(f"🎯 РАСШИРЕННЫЙ АНАЛИЗ МАТЧА: {team1_name} vs {team2_name}")
        print("=" * 60)
        
        # 1. Получаем основную статистику
        print("📊 Получение основной статистики...")
        data = ch_client.execute(correct_query, params)
        
        # 2. Получаем форму команд
        print("📈 Анализ формы команд...")
        team1_form = ch_client.execute(form_query, {'team_id': team1_id, 'season_id': season_id})
        team2_form = ch_client.execute(form_query, {'team_id': team2_id, 'season_id': season_id})
        
        # 3. Получаем исторические встречи
        print("🤝 Анализ исторических встреч...")
        h2h_data = ch_client.execute(h2h_query, params)
        
        # 4. Получаем домашние/гостевые статистики
        print("🏠 Анализ домашних/гостевых показателей...")
        home_away_stats = ch_client.execute(home_away_query, params)
        
        # 5. Получаем карточки
        print("🃏 Анализ карточек...")
        cards_data = ch_client.execute(cards_query, params)
        
        if not data:
            print("❌ Нет данных для выбранных команд и сезона")
            return
        
        row = data[0]
        
        # Обрабатываем данные карточек
        team1_cards = 0
        team2_cards = 0
        team1_matches_with_cards = 1
        team2_matches_with_cards = 1
        
        for card_row in cards_data:
            if card_row[0] == team1_id:
                team1_cards = card_row[1]
                team1_matches_with_cards = max(card_row[2], 1)
            elif card_row[0] == team2_id:
                team2_cards = card_row[1]
                team2_matches_with_cards = max(card_row[2], 1)
        
        # РАСЧЕТЫ ДЛЯ ПРОГНОЗОВ
        team1_avg_cards = team1_cards / team1_matches_with_cards
        team2_avg_cards = team2_cards / team2_matches_with_cards
        total_cards = team1_avg_cards + team2_avg_cards
        total_goals = row[2] + row[3]
        
        # ДАННЫЕ ИЗ СТАТИСТИКИ
        team1_xg = 1.27
        team2_xg = 0.67
        team1_corners = 9
        team2_corners = 1
        total_xg = team1_xg + team2_xg
        
        print(f"\n🏠 {team1_name} (дома) vs 🛬 {team2_name} (в гостях)")
        print("=" * 50)
        
        # ПРАВИЛЬНЫЕ ИНДЕКСЫ
        team1_matches_count = row[14]
        team2_matches_count = row[15]
        
        print(f"\n⚽ РЕЗУЛЬТАТИВНОСТЬ (последние {team1_matches_count} матчей):")
        print(f"   {team1_name}: {row[2]} голов за матч")
        print(f"   {team2_name}: {row[3]} голов за матч")

        print(f"\n🎯 УДАРЫ:")
        print(f"   {team1_name}: {row[4]} ударов ({row[6]} в створ) за матч") 
        print(f"   {team2_name}: {row[5]} ударов ({row[7]} в створ) за матч")

        print(f"\n🟨 ДИСЦИПЛИНА:")
        print(f"   {team1_name}: {row[8]} фолов, {team1_avg_cards:.1f} карточек за матч")
        print(f"   {team2_name}: {row[9]} фолов, {team2_avg_cards:.1f} карточек за матч")

        print(f"\n📊 ТОЧНОСТЬ ПЕРЕДАЧ:")
        print(f"   {team1_name}: {row[10]}%")
        print(f"   {team2_name}: {row[11]}%")
        
        print(f"\n🛡️ ОБОРОНА:")
        print(f"   {team1_name}: {row[12]} отборов за матч")
        print(f"   {team2_name}: {row[13]} отборов за матч")
        
        # РЕАЛЬНЫЙ xG АНАЛИЗ
        print(f"\n📈 РЕАЛЬНЫЙ xG АНАЛИЗ (из статистики матча):")
        print(f"   {team1_name}: {team1_xg} xG")
        print(f"   {team2_name}: {team2_xg} xG")
        
        # Эффективность атаки
        team1_efficiency = row[2] - team1_xg
        team2_efficiency = row[3] - team2_xg
        
        print(f"\n🎯 ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ:")
        print(f"   {team1_name}: {row[2]:.1f} голов при {team1_xg:.1f} xG ({team1_efficiency:+.2f})")
        print(f"   {team2_name}: {row[3]:.1f} голов при {team2_xg:.1f} xG ({team2_efficiency:+.2f})")
        
        # АНАЛИЗ УГЛОВЫХ
        print(f"\n🎪 СТАНДАРТНЫЕ ПОЛОЖЕНИЯ:")
        print(f"   {team1_name}: {team1_corners} угловых за матч")
        print(f"   {team2_name}: {team2_corners} угловых за матч")
        
        # ФОРМА КОМАНД
        print(f"\n📈 ФОРМА (последние 5 матчей):")
        if team1_form:
            team1_form_results = [match[6] for match in team1_form]
            print(f"   {team1_name}: {''.join(['🟢' if r == 'W' else '🟡' if r == 'D' else '🔴' for r in team1_form_results])}")
        else:
            print(f"   {team1_name}: нет данных")
            
        if team2_form:
            team2_form_results = [match[6] for match in team2_form]
            print(f"   {team2_name}: {''.join(['🟢' if r == 'W' else '🟡' if r == 'D' else '🔴' for r in team2_form_results])}")
        else:
            print(f"   {team2_name}: нет данных")
        
        # H2H ВСТРЕЧИ
        if h2h_data and h2h_data[0][0] > 0:
            h2h_info = h2h_data[0]
            print(f"\n🤝 ИСТОРИЧЕСКИЕ ВСТРЕЧИ ({h2h_info[0]} матчей):")
            print(f"   {team1_name}: {h2h_info[1]} побед")
            print(f"   {team2_name}: {h2h_info[2]} побед")
            print(f"   Ничьих: {h2h_info[3]}")
            print(f"   Средние голы: {h2h_info[4]:.1f}")
        else:
            print(f"\n🤝 ИСТОРИЧЕСКИЕ ВСТРЕЧИ: нет данных")
        
        # ДОМАШНИЕ/ГОСТЕВЫЕ ПОКАЗАТЕЛИ
        if home_away_stats:
            team1_home = next((s for s in home_away_stats if s[0] == team1_id and s[1] == 'home'), None)
            team2_away = next((s for s in home_away_stats if s[0] == team2_id and s[1] == 'away'), None)
            
            if team1_home and team2_away:
                print(f"\n🏠 ДОМАШНИЕ/ГОСТЕВЫЕ ПОКАЗАТЕЛИ:")
                print(f"   {team1_name} дома: {team1_home[3]}⚽ {team1_home[4]}🎯")
                print(f"   {team2_name} в гостях: {team2_away[3]}⚽ {team2_away[4]}🎯")
        
        # ПРОГНОЗ ПО ТОТАЛАМ НА ОСНОВЕ xG
        if total_xg > 2.8:
            total_pred = "ТБ 2.5 (высокая вероятность)"
            total_market = "ТБ 2.5"
            confidence = "🔴 Высокая"
        elif total_xg > 2.2:
            total_pred = "ТБ 2.5 (средняя вероятность)"
            total_market = "ТБ 2.5"
            confidence = "🟡 Средняя"
        elif total_xg > 1.8:
            total_pred = "ТМ 2.5 (низкая вероятность)"
            total_market = "ТМ 2.5"
            confidence = "🟢 Низкая"
        else:
            total_pred = "ТМ 2.5 (высокая вероятность)"
            total_market = "ТМ 2.5"
            confidence = "🔴 Высокая"
            
        # ПРОГНОЗ ПО КАРТОЧКАМ
        if total_cards > 5.5:
            cards_pred = "ТБ 5.5 (высокая вероятность)"
            cards_market = "ТБ 5.5"
        elif total_cards > 4.5:
            cards_pred = "ТБ 4.5 (средняя вероятность)"
            cards_market = "ТБ 4.5"
        elif total_cards > 3.5:
            cards_pred = "ТМ 4.5 (низкая вероятность)"
            cards_market = "ТМ 4.5"
        else:
            cards_pred = "ТМ 3.5 (высокая вероятность)"
            cards_market = "ТМ 3.5"
            
        # ПРОГНОЗ ПО УГЛОВЫМ
        total_corners = team1_corners + team2_corners
        if total_corners > 10.5:
            corners_pred = "ТБ 10.5 (высокая вероятность)"
            corners_market = "ТБ 10.5"
        elif total_corners > 9.5:
            corners_pred = "ТБ 9.5 (средняя вероятность)"
            corners_market = "ТБ 9.5"
        elif total_corners > 8.5:
            corners_pred = "ТМ 9.5 (низкая вероятность)"
            corners_market = "ТМ 9.5"
        else:
            corners_pred = "ТМ 8.5 (высокая вероятность)"
            corners_market = "ТМ 8.5"
            
        print(f"\n🏆 ПРОГНОЗ НА ОСНОВЕ РЕАЛЬНЫХ ДАННЫХ:")
        print(f"   • Ожидаемые голы (xG): {total_xg:.1f}")
        print(f"   • Реальные голы (история): {total_goals:.1f}")
        print(f"   • Ожидаемые карточки: {total_cards:.1f}")
        print(f"   • Ожидаемые угловые: {total_corners:.1f}")
        
        print(f"\n💰 РЕКОМЕНДАЦИИ ПО СТАВКАМ:")
        print(f"   • Тоталы голов: {total_pred}")
        print(f"   • Уверенность: {confidence}")
        print(f"   • Тоталы карточек: {cards_pred}")
        print(f"   • Тоталы угловых: {corners_pred}")
        
        print(f"\n🎯 РЫНКИ ДЛЯ СТАВОК:")
        print(f"   • Голы: {total_market}")
        print(f"   • Карточки: {cards_market}")
        print(f"   • Угловые: {corners_market}")
        
        # КЛЮЧЕВЫЕ ИНСАЙТЫ
        print(f"\n📈 КЛЮЧЕВЫЕ ИНСАЙТЫ ИЗ СТАТИСТИКИ:")
        goal_diff = row[2] - row[3]
        foul_diff = row[8] - row[9]
        pass_acc_diff = row[10] - row[11]
        cards_diff = team1_avg_cards - team2_avg_cards
        xg_diff = team1_xg - team2_xg
        corners_diff = team1_corners - team2_corners

        insights = []
        
        if abs(xg_diff) > 0.3:
            if xg_diff > 0:
                insights.append(f"{team1_name} создает значительно больше опасных моментов (+{xg_diff:.2f} xG)")
            else:
                insights.append(f"{team2_name} создает значительно больше опасных моментов (+{abs(xg_diff):.2f} xG)")
        
        if team1_efficiency > 0.3:
            insights.append(f"{team1_name} показывает высокую реализацию (+{team1_efficiency:.2f})")
        elif team1_efficiency < -0.3:
            insights.append(f"{team1_name} имеет проблемы с реализацией ({team1_efficiency:.2f})")
            
        if team2_efficiency > 0.3:
            insights.append(f"{team2_name} показывает высокую реализацию (+{team2_efficiency:.2f})")
        elif team2_efficiency < -0.3:
            insights.append(f"{team2_name} имеет проблемы с реализацией ({team2_efficiency:.2f})")
        
        if corners_diff > 3:
            insights.append(f"{team1_name} доминирует в стандартных положениях (+{corners_diff} угловых)")
        elif corners_diff < -3:
            insights.append(f"{team2_name} доминирует в стандартных положениях (+{abs(corners_diff)} угловых)")
        
        if abs(goal_diff) > 0.3:
            if goal_diff > 0:
                insights.append(f"{team1_name} эффективнее в атаке (+{goal_diff:.1f} гола)")
            else:
                insights.append(f"{team2_name} эффективнее в атаке (+{abs(goal_diff):.1f} гола)")
        
        if abs(pass_acc_diff) > 3:
            if pass_acc_diff > 0:
                insights.append(f"{team1_name} лучше контролирует игру (пас +{pass_acc_diff:.1f}%)")
            else:
                insights.append(f"{team2_name} лучше контролирует игру (пас +{abs(pass_acc_diff):.1f}%)")
        
        if abs(foul_diff) > 2:
            if foul_diff > 0:
                insights.append(f"{team1_name} играет агрессивнее (+{foul_diff:.1f} фола)")
            else:
                insights.append(f"{team2_name} играет агрессивнее (+{abs(foul_diff):.1f} фола)")
        
        if not insights:
            insights = [
                "Обе команды показывают сбалансированную игру",
                "Борьба в центре поля будет ключевой",
                "Ожидаем тактическую битву"
            ]
        
        for i, insight in enumerate(insights[:5], 1):
            print(f"   {i}. {insight}")
            
        print(f"\n📖 МЕТРИКИ АНАЛИЗА:")
        print(f"   • xG (Expected Goals) - ожидаемые голы на основе качества моментов")
        print(f"   • Эффективность = Реальные голы - xG")
        print(f"   • Положительная эффективность = высокая реализация")
        print(f"   • Угловые = показатель атакующего давления")
            
    except Exception as e:
        print(f"❌ Ошибка при выполнении запроса: {e}")
        import traceback
        print(f"🔍 Детали ошибки: {traceback.format_exc()}")

# Запуск анализа
if __name__ == "__main__":
    get_match_analysis_from_clickhouse(
        team1_id=2322,
        team2_id=24118,
        team1_name="Крылья советов",
        team2_name="Оренбург", 
        season_id=77142
    )