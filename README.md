# 🤖 Football Match Analysis Bot

Telegram бот для расширенного анализа футбольных матчей с использованием современных технологий обработки данных.

## 🚀 Возможности

- **📊 Расширенная статистика**: xG, владение мячом, удары, стандартные положения
- **📈 Анализ формы**: домашние/гостевые показатели
- **🤝 История встреч**: статистика личных противостояний
- **⭐ Ключевые игроки**: анализ формы и прогресса основных футболистов
- **🎯 Тактические инсайты**: анализ сильных и слабых сторон команд
- **💰 Прогнозы**: рекомендации по тоталам на основе статистики

## 🏗️ Архитектура


## ⚙️ Технологический стек

- **Backend**: Python 3.12
- **База данных**: ClickHouse (OLAP)
- **Оркестрация**: Apache Airflow
- **API**: Sofascore Football API
- **Bot Framework**: python-telegram-bot

## 📊 Data Pipeline

1. **Airflow DAGs** загружают данные через Sofascore API (можно настроить расписание или вручную)
2. **ClickHouse** хранит историческую статистику матчей, игроков и команд
3. **Бот** анализирует данные в реальном времени при запросе пользователя
4. **Расширенная аналитика** включает xG, позиционные данные и тактический анализ

## 🎯 Особенности анализа

- **Expected Goals (xG)**: оценка качества создаваемых моментов
- **Динамика позиций**: отслеживание изменения в турнирной таблице
- **Тактические паттерны**: анализ зон атак и уязвимостей в обороне
- **Прогресс игроков**: мониторинг формы ключевых футболистов
- **Контекстные инсайты**: автоматическая генерация тактических выводов

## 🏆 Поддерживаемые турниры

- Российская Премьер-Лига
- Английская Премьер-Лига
- Немецкая Бундеслига
- Французская Лига 1
- Итальянская Серия А
- Испанская Ла Лига
- *Другие турниры могут быть добавлены*

## 💡 Использование

1. Запустите бота командой `/start`
2. Выберите команду хозяев и гостей
3. Выберите тип анализа (базовая статистика, прогнозы, игроки и т.д.)
4. Получите детальный отчет с ключевыми инсайтами
5. Выход `/cancel`

## 🔧 Предварительная настройка

#### Настройте .env

### 1. 🗄️ Настройка базы данных ClickHouse

```sql
    -- 1. Футбольные матчи с историей предыдущих сезонов
    CREATE TABLE football_matches
    (
        match_id UInt64,
        tournament_id UInt32,
        season_id UInt32,
        round_number UInt8,
        match_date Date,
        home_team_id UInt32,
        home_team_name String,
        away_team_id UInt32,
        away_team_name String,
        home_score UInt8,
        away_score UInt8,
        status String,
        start_timestamp DateTime,
        created_at DateTime DEFAULT now(),
    ) ENGINE = ReplacingMergeTree(created_at)
    ORDER BY (match_id);

    -- 2. Статистика игроков
    CREATE TABLE football_player_stats
    (
        match_id UInt64,
        team_id UInt32,
        player_id UInt32,
        player_name String,
        short_name String,
        position String,
        jersey_number UInt8,
        minutes_played UInt16,
        rating Float32,
        goals UInt8,
        goal_assist UInt8,
        total_shot UInt16,
        on_target_shot UInt16,
        off_target_shot UInt16,
        blocked_scoring_attempt UInt16,
        total_pass UInt16,
        accurate_pass UInt16,
        pass_accuracy Float32,
        key_pass UInt16,
        total_long_balls UInt16,
        accurate_long_balls UInt16,
        successful_dribbles UInt16,
        dribble_success Float32,
        total_tackle UInt16,
        interception_won UInt16,
        total_clearance UInt16,
        outfielder_block UInt16,
        challenge_lost UInt16,
        duel_won UInt16,
        duel_lost UInt16,
        aerial_won UInt16,
        duel_success Float32,
        touches UInt16,
        possession_lost_ctrl UInt16,
        was_fouled UInt16,
        fouls UInt16,
        saves UInt16,
        punches UInt16,
        good_high_claim UInt16,
        saved_shots_from_inside_box UInt16,
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(created_at)
    ORDER BY (team_id, match_id, player_id);

    -- 3. Карточки
    CREATE TABLE football_cards
    (
        match_id UInt64,
        player_id UInt32,
        player_name String,
        team_is_home UInt8,
        card_type String,
        reason String,
        time UInt16,
        added_time UInt16,
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(created_at)
    ORDER BY (match_id, player_id, card_type, `time`);

    -- 4. Статистика матчей
    CREATE TABLE football_match_stats
    (
        match_id UInt64,
        team_id UInt32,
        team_name String,
        team_type String,
        ball_possession Float32,
        expected_goals Float32,
        total_shots UInt16,
        shots_on_target UInt16,
        shots_off_target UInt16,
        blocked_shots UInt16,
        corners UInt8,
        free_kicks UInt16,
        fouls UInt16,
        yellow_cards UInt8,
        big_chances UInt8,
        big_chances_scored UInt8,
        big_chances_missed UInt8,
        shots_inside_box UInt16,
        shots_outside_box UInt16,
        touches_in_penalty_area UInt16,
        total_passes UInt16,
        accurate_passes UInt16,
        pass_accuracy Float32,
        total_crosses UInt16,
        accurate_crosses UInt16,
        total_long_balls UInt16,
        accurate_long_balls UInt16,
        tackles UInt16,
        tackles_won_percent Float32,
        interceptions UInt16,
        recoveries UInt16,
        clearances UInt16,
        duel_won_percent UInt8 DEFAULT 0,
        dispossessed UInt16 DEFAULT 0,
        ground_duels_percentage UInt8 DEFAULT 0,
        aerial_duels_percentage UInt8 DEFAULT 0,
        dribbles_percentage UInt8 DEFAULT 0,
        errors_lead_to_shot UInt16 DEFAULT 0,
        errors_lead_to_goal UInt16 DEFAULT 0,
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(created_at)
    ORDER BY (match_id, team_id);

    -- 5. Кэш статистики команд
    CREATE TABLE team_stats_cache (
        team_id UInt32,
        tournament_id UInt32,
        season_id UInt32,
        matches_played UInt16,
        goals_scored UInt16,
        goals_conceded UInt16,
        avg_possession Float32,
        avg_shots Float32,
        avg_shots_on_target Float32,
        avg_corners Float32,
        avg_fouls Float32,
        avg_yellow_cards Float32,
        big_chances UInt16,
        big_chances_missed UInt16,
        goals_inside_box UInt16,
        goals_outside_box UInt16,
        headed_goals UInt16,
        pass_accuracy Float32,
        fast_breaks UInt16,
        updated_at DateTime,
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY tournament_id
    ORDER BY (team_id, season_id, tournament_id);

    -- 6. Кэш позиций команд
    CREATE TABLE team_positions_cache (
        team_id UInt32,
        tournament_id UInt32,
        season_id UInt32,
        position UInt8,
        points UInt16,
        goal_difference Int16,
        form String,
        matches_played UInt16,
        wins UInt16,
        draws UInt16,
        losses UInt16,
        goals_for UInt16,
        goals_against UInt16,
        trend String,
        last_updated_round UInt16,
        updated_at DateTime,
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY tournament_id
    ORDER BY (tournament_id, season_id, team_id, last_updated_round);

    -- 7. Расписание матчей и рефери на игру (таблица наполняется ориентировачно за день до тура, после назначения рефери на игру)
    CREATE TABLE match_fixtures (
        match_id UInt64,
        round_number UInt16,
        season_id UInt32,
        start_timestamp DateTime,
        referee_id UInt32,
        referee_name String,
        referee_yellow_cards UInt16,
        referee_red_cards UInt16,
        referee_yellow_red_cards UInt16,
        referee_games UInt16,
        referee_country String,
        venue_id UInt32,
        venue_name String,
        venue_city String,
        venue_capacity UInt16,
        home_team_id UInt32,
        home_team_name String,
        home_team_short_name String,
        home_manager_id UInt32,
        home_manager_name String,
        home_manager_short_name String,
        away_team_id UInt32,
        away_team_name String, 
        away_team_short_name String,
        away_manager_id UInt32,
        away_manager_name String,
        away_manager_short_name String,
        tournament_id UInt32,
        tournament_name String,
        season_year String,
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMM(start_timestamp)
    ORDER BY (start_timestamp, match_id);
```
### 2. 🌀 Оркестрация Airflow
#### Запускаем DAG для загрузки исторических данных исходя из количества туров

## 🔧 Установка

```bash
git clone https://github.com/MaximSI81/footballdataanalysis.git
cd footballdataanalysis
pip install -r requirements.txt
python get_historical_matches.py # для загрузки исторических матче предыдущих туров
python bot.py
```