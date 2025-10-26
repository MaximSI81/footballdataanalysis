from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from football_match_forecast import AdvancedFootballAnalyzer
import io
import sys
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

# Состояния разговора
SELECT_LEAGUE, SELECT_HOME_TEAM, SELECT_AWAY_TEAM, SELECT_MAIN_CATEGORY, SELECT_SUB_CATEGORY = range(5)

# Словарь лиг и турниров
LEAGUES = {
    "🇷🇺 Российская Премьер-Лига": {
        "id": 203,
        "season_id": 77142,
        "teams": {
            "Динамо Москва": 2315,
            "Акрон Тольятти": 285689,
            "Локомотив Москва": 2320,
            "Спартак Москва": 2323,
            "Пари Нижний Новгород": 201654,
            "Зенит Санкт-Петербург": 2321,
            "Ахмат Грозный": 5131,
            "Оренбург": 24118,
            "Крылья Советов Самара": 2322,
            "Краснодар": 34425,
            "Сочи": 2334,
            "ЦСКА Москва": 2325,
            "Рубин Казань": 2333,
            "Ростов": 2326,
            "Балтика Калининград": 7517,
            "Динамо Махачквала": 362016
        }
    },
    "🏴󠁧󠁢󠁥󠁮󠁧󠁿 Английская Премьер-Лига": {
        "id": 17,
        "season_id": 76986,
        "teams": {
            "Ливерпуль": 44,
            "Борнмут": 60,
            "Ньюкасл Юнайтед": 39,
            "Астон Вилла": 40,
            "Брайтон": 30,
            "Фулхэм": 43,
            "Ноттингем Форест": 14,
            "Брентфорд": 50,
            "Вест Хэм Юнайтед": 37,
            "Сандерленд": 41,
            "Бернли": 6,
            "Тоттенхэм Хотспур": 33,
            "Вулверхэмптон": 3,
            "Манчестер Сити": 17,
            "Кристал Пэлас": 7,
            "Челси": 38,
            "Манчестер Юнайтед": 35,
            "Арсенал": 42,
            "Лидс Юнайтед": 34,
            "Эвертон": 48
        }
    },
    "🇩🇪 Бундеслига": {
        "id": 35,
        "season_id": 77333,
        "teams": {
            "Бавария Мюнхен": 2672,
            "РБ Лейпциг": 36360,
            "Вердер Бремен": 2534,
            "Айнтрахт Франкфурт": 2674,
            "Унион Берлин": 2547,
            "Штутгарт": 2677,
            "Боруссия Мёнхенгладбах": 2527,
            "Гамбург": 2676,
            "Санкт-Паули": 2526,
            "Боруссия Дортмунд": 2673,
            "Хоффенхайм": 2569,
            "Байер Леверкузен": 2681,
            "Майнц": 2556,
            "Кёльн": 2671,
            "Вольфсбург": 2524,
            "Хайденхайм": 5885,
            "Фрайбург": 2538,
            "Аугсбург": 2600
        }
    },
    "🇫🇷 Лига 1": {
        "id": 34,
        "season_id": 77356,
        "teams": {
            "Лилль": 1643,
            "Брест": 1715,
            "Анже": 1684,
            "Пари": 6070,
            "Осер": 1646,
            "Лорьян": 1656,
            "Ницца": 1661,
            "Тулуза": 1681,
            "Пари Сен-Жермен": 1644,
            "Нант": 1647,
            "Монако": 1653,
            "Гавр": 1662,
            "Марсель": 1641,
            "Ренн": 1658,
            "Мец": 1651,
            "Страсбур": 1659,
            "Ланс": 1648,
            "Лион": 1649
        }
    },
    "🇮🇹 Серия А": {
        "id": 23,
        "season_id": 76457,
        "teams": {
            "Торино": 2696,
            "Интер": 2697,
            "Лечче": 2689,
            "Дженоа": 2713,
            "Лацио": 2699,
            "Комо": 2704,
            "Ювентус": 2687,
            "Парма": 2690,
            "Фиорентина": 2693,
            "Кальяри": 2719,
            "Болонья": 2685,
            "Рома": 2702,
            "Наполи": 2714,
            "Сассуоло": 2793,
            "Удинезе": 2695,
            "Верона": 2701,
            "Милан": 2692,
            "Кремонезе": 2761,
            "Аталанта": 2686,
            "Пиза": 2737
        }
    },
    "🇪🇸 Ла Лига": {
        "id": 8,
        "season_id": 77559,
        "teams": {
            "Леванте": 2849,
            "Алавес": 2885,
            "Атлетик Бильбао": 2825,
            "Севилья": 2833,
            "Сельта": 2821,
            "Хетафе": 2859,
            "Реал Бетис": 2816,
            "Эльче": 2846,
            "Эспаньол": 2814,
            "Атлетико Мадрид": 2836,
            "Райо Вальекано": 2818,
            "Жирона": 24264,
            "Барселона": 2817,
            "Мальорка": 2826,
            "Осасуна": 2820,
            "Реал Мадрид": 2829,
            "Реал Сосьедад": 2824,
            "Валенсия": 2828,
            "Вильярреал": 2819,
            "Овьедо": 2851
        }
    }
}

# Основные категории анализа
MAIN_CATEGORIES = {
    "📊 Общий обзор": "overview",
    "⚽ Атака и голы": "attack", 
    "🛡️ Основные статистики": "stats",
    "📈 Форма и H2H": "form_h2h",
    "💰 Прогнозы": "predictions",
    "⭐ Игроки": "players",
    "📋 Полный отчет": "full_report",
    "🔙 Назад к выбору команд": "back_to_teams"
}

# Подкатегории для каждого раздела
SUB_CATEGORIES = {
    "overview": {
        "📊 Весь раздел 'Обзор'": "overview_all",
        "📊 Сводная таблица": "overview_summary",
        "⚡ Быстрые выводы": "overview_quick_insights",
        "🔙 Назад к разделам": "back_to_main"
    },
    "attack": {
        "⚽ Весь раздел 'Атака'": "attack_all",
        "⚽ Результативность": "attack_goals", 
        "🎯 xG и удары": "attack_shots",
        "📈 Эффективность": "attack_efficiency",
        "🔄 Кроссы и фланги": "attack_crosses",        
        "🎯 Длинные передачи": "attack_longballs",
        "🎯 Анализ зон атак": "attack_zones",
        "🔙 Назад к разделам": "back_to_main"    
    },
    "stats": {
        "🛡️ Весь раздел 'Статистика'": "stats_all",
        "⚖️ Владение мячом": "stats_possession",
        "📊 Пасы": "stats_passing",
        "🟨 Агрессивность": "stats_aggression",
        "🎪 Качество моментов": "stats_quality",
        "🔙 Назад к разделам": "back_to_main"
    },
    "form_h2h": {
        "📈 Весь раздел 'Форма'": "form_h2h_all",
        "📅 Последние матчи": "form_recent",
        "🤝 H2H история": "form_h2h",
        "🏠 Дома/в гостях": "form_home_away",
        "🔙 Назад к разделам": "back_to_main"
    },
    "predictions": {
        "💰 Весь раздел 'Прогнозы'": "predictions_all",
        "🎯 Вероятности": "predictions_probabilities",
        "💡 Рекомендации": "predictions_recommendations",
        "🔍 Инсайты": "predictions_insights",
        "🟨 Карточки": "predictions_cards",
        "🏠 Дома/в гостях": "predictions_home_away",
        "🔙 Назад к разделам": "back_to_main"
    },
    "players": {
        "⭐ Весь раздел 'Игроки'": "players_all",
        "👥 Ключевые игроки": "players_key",
        "🔙 Назад к разделам": "back_to_main"
    }
}

# Клавиатура с основными командами
MAIN_KEYBOARD = [
    ["🏟️ Старт анализа", "🔁 Продолжить анализ"],
    ["❌ Завершить"]
]

def create_keyboard_with_finish(buttons_list, include_change_league=False):
    """Создает клавиатуру с кнопкой завершения и опционально смены лиги"""
    if include_change_league:
        buttons_list.append(["🔄 Сменить лигу"])
    buttons_list.append(["❌ Завершить"])
    return ReplyKeyboardMarkup(
        buttons_list,
        one_time_keyboard=True,
        resize_keyboard=True
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало работы с ботом"""
    welcome_text = """
🏟️ Футбольный анализатор матчей

Я помогу проанализировать предстоящий матч на основе статистики.

Нажмите "🏟️ Старт анализа" для начала анализа матча или "🔁 Продолжить анализ" для возврата к предыдущему матчу.
    """
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=ReplyKeyboardMarkup(
            MAIN_KEYBOARD,
            resize_keyboard=True,
            one_time_keyboard=False
        )
    )
    
    return ConversationHandler.END

async def start_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало анализа матча"""
    if update.message.text == "❌ Завершить":
        return await cancel(update, context)
    
    welcome_text = """
🏟️ Футбольный анализатор матчей

Выберите лигу для анализа:
    """
    
    # Создаем клавиатуру с лигами
    league_buttons = [list(LEAGUES.keys())[i:i+2] for i in range(0, len(LEAGUES), 2)]
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=create_keyboard_with_finish(league_buttons)
    )
    
    return SELECT_LEAGUE

async def continue_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Продолжение анализа текущего матча"""
    if update.message.text == "❌ Завершить":
        return await cancel(update, context)
    
    if 'league' not in context.user_data:
        await update.message.reply_text("❌ Нет данных о предыдущем анализе. Начните новый анализ.")
        return await start_analysis(update, context)
    
    # Возвращаемся к выбору категорий
    league = context.user_data['league']
    home_team = context.user_data['home_team']
    away_team = context.user_data['away_team']
    
    category_buttons = [list(MAIN_CATEGORIES.keys())[i:i+2] for i in range(0, len(MAIN_CATEGORIES), 2)]
    
    await update.message.reply_text(
        f"🔄 ПРОДОЛЖЕНИЕ АНАЛИЗА:\n"
        f"🏆 Лига: {league}\n"
        f"🏠 Хозяева: {home_team}\n"
        f"🛬 Гости: {away_team}\n\n"
        f"Выберите раздел анализа:",
        reply_markup=create_keyboard_with_finish(category_buttons)
    )
    
    return SELECT_MAIN_CATEGORY

async def change_league(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Смена лиги"""
    if update.message.text == "❌ Завершить":
        return await cancel(update, context)
    
    welcome_text = "Выберите новую лигу для анализа:"
    
    # Создаем клавиатуру с лигами
    league_buttons = [list(LEAGUES.keys())[i:i+2] for i in range(0, len(LEAGUES), 2)]
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=create_keyboard_with_finish(league_buttons)
    )
    
    return SELECT_LEAGUE

async def handle_back_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка навигации Назад"""
    if update.message.text == "❌ Завершить":
        return await cancel(update, context)
    
    user_data = context.user_data
    
    if update.message.text == "🔙 Назад к выбору команд":
        # Возврат к выбору домашней команды
        league = user_data['league']
        teams = user_data['teams']
        team_buttons = [list(teams.keys())[i:i+3] for i in range(0, len(teams), 3)]
        
        await update.message.reply_text(
            f"🏆 Лига: {league}\n\n"
            f"Выберите команду хозяев:",
            reply_markup=create_keyboard_with_finish(team_buttons)
        )
        return SELECT_HOME_TEAM
        
    elif update.message.text == "🔙 Назад к разделам":
        # Возврат к выбору основной категории
        league = user_data['league']
        home_team = user_data['home_team']
        away_team = user_data['away_team']
        
        category_buttons = [list(MAIN_CATEGORIES.keys())[i:i+2] for i in range(0, len(MAIN_CATEGORIES), 2)]
        
        await update.message.reply_text(
            f"🏆 Лига: {league}\n"
            f"🏠 Хозяева: {home_team}\n"
            f"🛬 Гости: {away_team}\n\n"
            f"Выберите раздел анализа:",
            reply_markup=create_keyboard_with_finish(category_buttons)
        )
        return SELECT_MAIN_CATEGORY

async def select_league(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора лиги"""
    selected_league = update.message.text
    
    if selected_league not in LEAGUES:
        league_buttons = [list(LEAGUES.keys())[i:i+2] for i in range(0, len(LEAGUES), 2)]
        await update.message.reply_text(
            "❌ Пожалуйста, выберите лигу из списка:",
            reply_markup=create_keyboard_with_finish(league_buttons)  # БЕЗ include_change_league
        )
        return SELECT_LEAGUE
    
    # Сохраняем данные лиги
    context.user_data['league'] = selected_league
    context.user_data['tournament_id'] = LEAGUES[selected_league]['id']
    context.user_data['season_id'] = LEAGUES[selected_league]['season_id'] 
    context.user_data['teams'] = LEAGUES[selected_league]['teams']
    
    # Создаем клавиатуру для выбора домашней команды
    team_buttons = [list(LEAGUES[selected_league]['teams'].keys())[i:i+3] for i in range(0, len(LEAGUES[selected_league]['teams']), 3)]
    
    await update.message.reply_text(
        f"🏆 Лига: {selected_league}\n\n"
        f"Выберите команду хозяев:",
        reply_markup=create_keyboard_with_finish(team_buttons, include_change_league=True)  # ВКЛЮЧАЕМ кнопку смены лиги
    )
    
    return SELECT_HOME_TEAM

async def select_home_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора команды хозяев"""
    home_team = update.message.text
    teams = context.user_data['teams']
    
    if home_team == "❌ Завершить":
        return await cancel(update, context)
    
    # Обработка смены лиги
    if home_team == "🔄 Сменить лигу":
        return await change_league(update, context)
    
    if home_team not in teams:
        team_buttons = [list(teams.keys())[i:i+3] for i in range(0, len(teams), 3)]
        team_buttons.append(["🔄 Сменить лигу"])  # Добавляем кнопку смены лиги
        team_buttons.append(["❌ Завершить"])
        
        await update.message.reply_text(
            "❌ Пожалуйста, выберите команду из списка:",
            reply_markup=ReplyKeyboardMarkup(team_buttons, one_time_keyboard=True, resize_keyboard=True)
        )
        return SELECT_HOME_TEAM
    
    context.user_data['home_team'] = home_team
    context.user_data['home_team_id'] = teams[home_team]
    
    # Создаем клавиатуру для гостевой команды (исключаем домашнюю)
    other_teams = [team for team in teams.keys() if team != home_team]
    away_buttons = [other_teams[i:i+2] for i in range(0, len(other_teams), 2)]
    away_buttons.append(["🔄 Сменить лигу"])  # Добавляем кнопку смены лиги
    away_buttons.append(["❌ Завершить"])
    
    await update.message.reply_text(
        f"🏆 Лига: {context.user_data['league']}\n"
        f"🏠 Хозяева: {home_team}\n\n"
        f"Выберите команду гостей:",
        reply_markup=ReplyKeyboardMarkup(away_buttons, one_time_keyboard=True, resize_keyboard=True)
    )
    
    return SELECT_AWAY_TEAM

async def select_away_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора команды гостей"""
    away_team = update.message.text
    teams = context.user_data['teams']
    
    if away_team == "❌ Завершить":
        return await cancel(update, context)
    
    # Обработка смены лиги
    if away_team == "🔄 Сменить лигу":
        return await change_league(update, context)
    
    if away_team not in teams:
        away_buttons = [list(teams.keys())[i:i+2] for i in range(0, len(teams), 2)]
        away_buttons.append(["🔄 Сменить лигу"])  # Добавляем кнопку смены лиги
        away_buttons.append(["❌ Завершить"])
        
        await update.message.reply_text(
            "❌ Пожалуйста, выберите команду из списка:",
            reply_markup=ReplyKeyboardMarkup(away_buttons, one_time_keyboard=True, resize_keyboard=True)
        )
        return SELECT_AWAY_TEAM
    
    context.user_data['away_team'] = away_team
    context.user_data['away_team_id'] = teams[away_team]
    
    # Создаем клавиатуру для выбора основной категории
    category_buttons = [list(MAIN_CATEGORIES.keys())[i:i+2] for i in range(0, len(MAIN_CATEGORIES), 2)]
    category_buttons.append(["❌ Завершить"])
    
    await update.message.reply_text(
        f"🏆 Лига: {context.user_data['league']}\n"
        f"🏠 Хозяева: {context.user_data['home_team']}\n"
        f"🛬 Гости: {away_team}\n\n"
        f"Выберите раздел анализа:",
        reply_markup=ReplyKeyboardMarkup(category_buttons, one_time_keyboard=True, resize_keyboard=True)
    )
    
    return SELECT_MAIN_CATEGORY

async def select_main_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора основной категории"""
    selected_category = update.message.text
    
    if selected_category == "❌ Завершить":
        return await cancel(update, context)
    
    # Обработка кнопки Назад
    if selected_category == "🔙 Назад к выбору команд":
        return await handle_back_navigation(update, context)
    
    # Обработка смены лиги
    if selected_category == "🔄 Сменить лигу":
        return await change_league(update, context)
    
    if selected_category not in MAIN_CATEGORIES:
        await update.message.reply_text(
            "❌ Пожалуйста, выберите раздел из списка:",
            reply_markup=create_keyboard_with_finish([list(MAIN_CATEGORIES.keys())[i:i+2] for i in range(0, len(MAIN_CATEGORIES), 2)])
        )
        return SELECT_MAIN_CATEGORY
    
    category_key = MAIN_CATEGORIES[selected_category]
    context.user_data['selected_main_category'] = category_key
    
    # Если выбран полный отчет - сразу запускаем анализ
    if category_key == "full_report":
        return await get_analysis(update, context, "all")
    
    # Если нажата кнопка Назад
    if category_key == "back_to_teams":
        return await handle_back_navigation(update, context)
    
    # Если выбрана смена лиги
    if category_key == "change_league":
        return await change_league(update, context)
    
    # Для остальных категорий показываем подкатегории
    sub_categories = SUB_CATEGORIES.get(category_key, {})
    sub_buttons = [list(sub_categories.keys())[i:i+2] for i in range(0, len(sub_categories), 2)]
    
    await update.message.reply_text(
        f"📂 Раздел: {selected_category}\n\n"
        f"Выберите что показать:",
        reply_markup=create_keyboard_with_finish(sub_buttons)
    )
    
    return SELECT_SUB_CATEGORY

async def select_sub_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора подкатегории"""
    selected_sub = update.message.text
    main_category = context.user_data['selected_main_category']
    
    if selected_sub == "❌ Завершить":
        return await cancel(update, context)
    
    # Обработка кнопки Назад
    if selected_sub == "🔙 Назад к разделам":
        return await handle_back_navigation(update, context)
    
    sub_categories = SUB_CATEGORIES.get(main_category, {})
    
    if selected_sub not in sub_categories:
        await update.message.reply_text(
            "❌ Пожалуйста, выберите опцию из списка:",
            reply_markup=create_keyboard_with_finish([list(sub_categories.keys())[i:i+2] for i in range(0, len(sub_categories), 2)])
        )
        return SELECT_SUB_CATEGORY
    
    data_type = sub_categories[selected_sub]
    
    # Обработка кнопки Назад в подкатегориях
    if data_type == "back_to_main":
        return await handle_back_navigation(update, context)
    
    return await get_analysis(update, context, data_type)

async def get_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, data_type: str = None):
    """Получение и вывод анализа"""
    try:
        if not data_type:
            selected_data_type = update.message.text
            data_type = "all"
        
        # Получаем данные из контекста
        league = context.user_data['league']
        home_team = context.user_data['home_team']
        away_team = context.user_data['away_team']
        home_team_id = context.user_data['home_team_id']
        away_team_id = context.user_data['away_team_id']
        tournament_id = context.user_data['tournament_id']
        season_id = context.user_data['season_id']
        
        await update.message.reply_text("⏳ Анализирую данные... Это может занять несколько секунд")
        
        # Перехватываем вывод
        old_stdout = sys.stdout
        new_stdout = io.StringIO()
        sys.stdout = new_stdout
        
        try:
            # Если выбран только раздел игроков - используем специальный метод
            if data_type in ["players_all", "players_key"]:
                async with AdvancedFootballAnalyzer() as analyzer:
                    await analyzer.get_players_analysis(
                        team1_id=home_team_id,
                        team2_id=away_team_id,
                        team1_name=home_team,
                        team2_name=away_team,
                        tournament_id=tournament_id,
                        season_id=season_id
                    )
            else:
                # Полный анализ для остальных случаев
                async with AdvancedFootballAnalyzer() as analyzer:
                    await analyzer.get_match_analysis(
                        team1_id=home_team_id,
                        team2_id=away_team_id,
                        team1_name=home_team,
                        team2_name=away_team,
                        tournament_id=tournament_id,
                        season_id=season_id
                    )
            
            # Получаем вывод
            analysis_output = new_stdout.getvalue()
            sys.stdout = old_stdout
            
            if not analysis_output or len(analysis_output.strip()) < 10:
                await update.message.reply_text("❌ Не удалось получить данные анализа")
                return await show_main_menu(update, context)
            
            # Для раздела игроков просто выводим результат
            if data_type in ["players_all", "players_key"]:
                header = f"🏆 ЛИГА: {league}\n🏟️ АНАЛИЗ МАТЧА:\n{home_team} 🆚 {away_team}\n\n"
                await update.message.reply_text(header + analysis_output)
            else:
                # Фильтруем вывод по выбранному типу данных
                filtered_output = filter_output(analysis_output, data_type)
                
                # Разбиваем на части если слишком длинное сообщение
                messages = split_message(filtered_output)
                
                for i, msg in enumerate(messages):
                    # Для первого сообщения добавляем заголовок
                    if i == 0:
                        header = f"🏆 ЛИГА: {league}\n🏟️ АНАЛИЗ МАТЧА:\n{home_team} 🆚 {away_team}\n\n"
                        full_msg = header + msg
                    else:
                        full_msg = msg
                        
                    await update.message.reply_text(full_msg)
                
            # После анализа возвращаем в главное меню
            return await show_main_menu(update, context)
            
        except Exception as e:
            sys.stdout = old_stdout
            await update.message.reply_text(f"❌ Ошибка при анализе данных: {str(e)}")
            return await show_main_menu(update, context)
            
    except Exception as e:
        await update.message.reply_text(f"❌ Критическая ошибка: {str(e)}")
        return await show_main_menu(update, context)

def filter_output(output: str, data_type: str) -> str:
    """Умная фильтрация вывода по блокам/секциям"""
    if data_type == "all":
        return output
    
    # Обновленные правила для КАЖДОГО ПОДРАЗДЕЛА с реальными данными
    section_rules = {
        # === ОБЩИЙ ОБЗОР ===
        "overview_summary": {
            "sections": ["🏆 ПОЗИЦИЯ В ТАБЛИЦЕ", "⚽ РЕЗУЛЬТАТИВНОСТЬ", "🎯 УДАРЫ", 
                        "📈 РЕАЛЬНЫЙ xG", "🎯 ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ", "📊 КОНТРОЛЬ ИГРЫ", 
                        "🔄 ТОЧНОСТЬ ПЕРЕДАЧ", "🛡️ ОБОРОНА", "🟨 АГРЕССИВНОСТЬ",
                        "🎪 КАЧЕСТВО МОМЕНТОВ"]
        },
        "overview_quick_insights": {
            "sections": ["📈 КЛЮЧЕВЫЕ ИНСАЙТЫ"]
        },
        "overview_all": {
            "sections": ["🏆 ПОЗИЦИЯ В ТАБЛИЦЕ", "⚽ РЕЗУЛЬТАТИВНОСТЬ", "🎯 УДАРЫ", 
                        "📈 РЕАЛЬНЫЙ xG", "🎯 ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ", "📊 КОНТРОЛЬ ИГРЫ", 
                        "🔄 ТОЧНОСТЬ ПЕРЕДАЧ", "🛡️ ОБОРОНА", "🟨 АГРЕССИВНОСТЬ",
                        "🎪 КАЧЕСТВО МОМЕНТОВ", "📈 КЛЮЧЕВЫЕ ИНСАЙТЫ"]
        },
        
        # === АТАКА ===
        "attack_goals": {
            "sections": ["⚽ РЕЗУЛЬТАТИВНОСТЬ"]
        },
        "attack_shots": {
            "sections": ["🎯 УДАРЫ", "📈 РЕАЛЬНЫЙ xG"]
        },
        "attack_efficiency": {
            "sections": ["🎯 ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ"]
        },
        "attack_crosses": {
            "sections": ["🔄 АКТИВНОСТЬ ФЛАНГОВ"]
        },
        "attack_longballs": {
            "sections": ["🎯 ДАЛЬНИЕ АТАКИ"]
        },
        "attack_zones": {
            "sections": ["🎯 АНАЛИЗ ЗОН АТАК И УЯЗВИМОСТЕЙ", "🏹 АТАКУЕТ"]
        },
        "attack_all": {
            "sections": ["⚽ РЕЗУЛЬТАТИВНОСТЬ", "🎯 УДАРЫ", "📈 РЕАЛЬНЫЙ xG", 
                        "🎯 ЭФФЕКТИВНОСТЬ РЕАЛИЗАЦИИ", "🔄 АКТИВНОСТЬ ФЛАНГОВ", 
                        "🎯 ДАЛЬНИЕ АТАКИ", "🎯 АНАЛИЗ ЗОН АТАК И УЯЗВИМОСТЕЙ",
                        "🏹 АТАКУЕТ"]
        },
        
        # === СТАТИСТИКА ===
        "stats_possession": {
            "sections": ["📊 КОНТРОЛЬ ИГРЫ"]
        },
        "stats_passing": {
            "sections": ["🔄 ТОЧНОСТЬ ПЕРЕДАЧ"]
        },
        "stats_aggression": {
            "sections": ["🟨 АГРЕССИВНОСТЬ"]
        },
        "stats_quality": {
            "sections": ["🎪 КАЧЕСТВО МОМЕНТОВ"]
        },
        "stats_all": {
            "sections": ["📊 КОНТРОЛЬ ИГРЫ", "🔄 ТОЧНОСТЬ ПЕРЕДАЧ", "🛡️ ОБОРОНА",
                        "🟨 АГРЕССИВНОСТЬ", "🎪 КАЧЕСТВО МОМЕНТОВ"]
        },
        
        # === ФОРМА И H2H ===
        "form_recent": {
            "sections": ["📈 ФОРМА"]
        },
        "form_h2h": {
            "sections": ["📊 ИСТОРИЯ ЛИЧНЫХ ВСТРЕЧ", "🤝 ВСЕГО МАТЧЕЙ", 
                        "⚽ ОБЩАЯ РЕЗУЛЬТАТИВНОСТЬ", "📈 СРЕДНИЕ ПОКАЗАТЕЛИ ЗА МАТЧ"]
        },
        "form_home_away": {
            "sections": ["🏠🛬 ПРОГНОЗ РЕЗУЛЬТАТА С УЧЕТОМ ДОМАШНЕГО СТАДИОНА"]
        },
        "form_h2h_all": {
            "sections": ["📈 ФОРМА", "📊 ИСТОРИЯ ЛИЧНЫх ВСТРЕЧ", "🤝 ВСЕГО МАТЧЕЙ",
                        "⚽ ОБЩАЯ РЕЗУЛЬТАТИВНОСТЬ", "📈 СРЕДНИЕ ПОКАЗАТЕЛИ ЗА МАТЧ",
                        "🏠🛬 ПРОГНОЗ РЕЗУЛЬТАТА С УЧЕТОМ ДОМАШНЕГО СТАДИОНА"]
        },
        
        # === ПРОГНОЗЫ ===
        "predictions_probabilities": {
            "sections": ["🏆 ПРОГНОЗ С УЧЕТОМ ПОЗИЦИИ В ТАБЛИЦЕ"]
        },
        "predictions_recommendations": {
            "sections": ["💰 РЕКОМЕНДАЦИИ"]
        },
        "predictions_insights": {
            "sections": ["📈 КЛЮЧЕВЫЕ ИНСАЙТЫ"]
        },
        "predictions_cards": {
            "sections": ["🟨 СТАТИСТИКА ДИСЦИПЛИНЫ", "🟨 ДЕТАЛЬНЫЙ АНАЛИЗ КАРТОЧЕК", "👨‍⚖️ Рефери:",
                        "🎯 Прогноз желтых карточек:", "📊 ИСТОРИЧЕСКИЕ ДАННЫЕ:", "🎯 ФАКТОРЫ ВЛИЯНИЯ:",
                        "📊 СРЕДНИЕ КАРТОЧКИ ЗА МАТЧ:", "⚖️ АГРЕССИВНОСТЬ:", "🎯 ИНТЕНСИВНОСТЬ:"]
        },
        "predictions_home_away": {
            "sections": ["🏠🛬 ПРОГНОЗ РЕЗУЛЬТАТА С УЧЕТОМ ДОМАШНЕГО СТАДИОНА"]
        },
        "predictions_all": {
            "sections": ["🏆 ПРОГНОЗ С УЧЕТОМ ПОЗИЦИИ В ТАБЛИЦЕ", "💰 РЕКОМЕНДАЦИИ", 
                        "📈 КЛЮЧЕВЫЕ ИНСАЙТЫ", "🎲 ЭКСКЛЮЗИВНЫЕ ПРОГНОЗЫ",
                        "🟨 СТАТИСТИКА ДИСЦИПЛИНЫ", "🟨 ДЕТАЛЬНЫЙ АНАЛИЗ КАРТОЧЕК", "👨‍⚖️ Рефери:",
                        "🏠🛬 ПРОГНОЗ РЕЗУЛЬТАТА С УЧЕТОМ ДОМАШНЕГО СТАДИОНА"]
        },
        
        # === ИГРОКИ ===
        "players_key": {
            "sections": ["⭐ АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ", "🔑 КЛЮЧЕВЫЕ ИГРОКИ"]
        },
        "players_all": {
            "sections": ["⭐ АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ", "🔑 КЛЮЧЕВЫЕ ИГРОКИ"]
        }
    }
    
    rule = section_rules.get(data_type, {})
    if not rule:
        return get_brief_overview(output, data_type)
    
    target_sections = rule.get("sections", [])
    
    lines = output.split('\n')
    filtered_lines = []
    current_section = ""
    in_target_section = False
    
    for line in lines:
        # Определяем начало новой секции - улучшенная логика
        is_section_header = False
        
        # Проверяем эмодзи и ключевые слова
        section_markers = [
            "🏆", "⚽", "🎯", "📈", "📊", "🔄", "🛡️", "🟨", "🎪", 
            "🏹", "🤝", "💰", "🎲", "⭐", "🔑", "⚔️", "👨‍⚖️", "🏠🛬"
        ]
        
        section_keywords = [
            "ПОЗИЦИЯ В ТАБЛИЦЕ", "РЕЗУЛЬТАТИВНОСТЬ", "УДАРЫ", "xG", 
            "ЭФФЕКТИВНОСТЬ", "КОНТРОЛЬ", "ТОЧНОСТЬ", "ОБОРОНА",
            "АГРЕССИВНОСТЬ", "ДИСЦИПЛИНА", "КАЧЕСТВО", "ФОРМА", 
            "ИСТОРИЯ", "ВСЕГО МАТЧЕЙ", "ОБЩАЯ", "СРЕДНИЕ", 
            "ПРОГНОЗ", "РЕКОМЕНДАЦИИ", "ИНСАЙТЫ", "ЭКСКЛЮЗИВНЫЕ", 
            "АНАЛИЗ ДИСЦИПЛИНЫ", "КЛЮЧЕВЫЕ", "АТАКУЕТ", "ПРОТИВОСТОЯНИЯ",
            "ФЛАНГОВ", "ДАЛЬНИЕ", "ЗОН АТАК", "УЯЗВИМОСТЕЙ", "РЕФЕРИ",
            "СТАТИСТИКА ДИСЦИПЛИНЫ", "ДЕТАЛЬНЫЙ АНАЛИЗ КАРТОЧЕК", "ИСТОРИЧЕСКИЕ ДАННЫЕ",
            "ФАКТОРЫ ВЛИЯНИЯ", "СРЕДНИЕ КАРТОЧКИ", "АГРЕССИВНОСТЬ", "ИНТЕНСИВНОСТЬ"
        ]
        
        # Проверяем наличие маркеров секций
        has_emoji = any(marker in line for marker in section_markers)
        has_keyword = any(keyword in line for keyword in section_keywords)
        
        # Считаем строку заголовком секции если содержит эмодзи И ключевое слово
        # ИЛИ если это явно выделенная секция (много заглавных букв)
        if (has_emoji and has_keyword) or (line.isupper() and len(line) > 5):
            is_section_header = True
            current_section = line.strip()
            
            # Проверяем, является ли текущая секция целевой
            in_target_section = any(
                target_section in current_section for target_section in target_sections
            )
        
        # Добавляем строку только если мы в целевой секции
        # ИЛИ если это непустая строка и мы все еще в целевой секции
        if in_target_section and line.strip():
            filtered_lines.append(line)
    
    result = '\n'.join(filtered_lines)
    
    # Если ничего не найдено, возвращаем краткий обзор
    if not result.strip():
        return get_brief_overview(output, data_type)
    
    return result

def get_brief_overview(output: str, data_type: str) -> str:
    """Возвращает краткий обзор если фильтр пустой"""
    brief_messages = {
        # Общий обзор
        "overview_summary": "📊 Сводная таблица временно недоступна",
        "overview_key_metrics": "🎯 Раздел 'Ключевые показатели' временно недоступен",
        "overview_quick_insights": "⚡ Быстрые выводы временно недоступны",
        
        # Атака
        "attack_goals": "⚽ Данные о голах временно недоступны",
        "attack_shots": "🎯 Данные об ударах временно недоступны", 
        "attack_efficiency": "📈 Данные об эффективности временно недоступны",
        "attack_crosses": "🔄 Данные о кроссах временно недоступны",
        "attack_longballs": "🎯 Данные о длинных передачах временно недоступны",
        "attack_zones": "🎯 Анализ зон атак временно недоступен",
        
        # Статистика
        "stats_possession": "📊 Данные о владении временно недоступны",
        "stats_passing": "🔄 Данные о передачах временно недоступны", 
        "stats_discipline": "🟨 Данные о карточках временно недоступны",
        "stats_aggression": "🟨 Данные о фолах временно недоступны",
        "stats_quality": "🎪 Данные о качестве моментов временно недоступны",
        
        # Форма и H2H
        "form_recent": "📈 Данные о форме временно недоступны",
        "form_h2h": "🤝 История личных встреч временно недоступна",
        "form_home_away": "🏠 Данные о домашних/гостевых показателях временно недоступны",
        
        # Прогнозы
        "predictions_probabilities": "🎯 Вероятности исходов временно недоступны",
        "predictions_recommendations": "💰 Рекомендации временно недоступны", 
        "predictions_insights": "📈 Инсайты временно недоступны",
        "predictions_cards": "🟨 Прогноз карточек временно недоступен",
        "predictions_home_away": "🏠🛬 Прогноз с учетом стадиона временно недоступен",
        
        # Игроки
        "players_key": "⭐️ Анализ ключевых игроков появится после 3-х сыгранных туров",
        "players_matchups": "⚔️ Анализ противостояний временно недоступен"
    }
    
    return brief_messages.get(data_type, "📊 Данные для выбранного раздела временно недоступны")

def split_message(text: str, max_length: int = 4000) -> list:
    """Разбивает сообщение на части если оно слишком длинное"""
    if len(text) <= max_length:
        return [text]
    
    parts = []
    while len(text) > max_length:
        split_pos = text.rfind('\n', 0, max_length)
        if split_pos == -1:
            split_pos = max_length
        parts.append(text[:split_pos])
        text = text[split_pos:].lstrip()
    
    if text:
        parts.append(text)
    
    return parts

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает главное меню с кнопками"""
    await update.message.reply_text(
        "Анализ завершен! Хотите проанализировать другой матч или продолжить с текущим?",
        reply_markup=ReplyKeyboardMarkup(
            MAIN_KEYBOARD,
            resize_keyboard=True,
            one_time_keyboard=False
        )
    )
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Завершение работы бота"""
    await update.message.reply_text(
        "👋 Спасибо за использование футбольного анализатора! Для нового анализа используйте /start",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END

def main():
    """Запуск бота"""
    # Получаем токен из переменных окружения
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    
    if not TOKEN:
        print("❌ Токен бота не найден. Убедитесь, что переменная TELEGRAM_BOT_TOKEN установлена в .env файле")
        return
    
    application = Application.builder().token(TOKEN).build()
    
    # Создаем обработчик диалога
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            MessageHandler(filters.Text(["🏟️ Старт анализа"]), start_analysis),
            MessageHandler(filters.Text(["🔁 Продолжить анализ"]), continue_analysis)
        ],
        states={
            SELECT_LEAGUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_league)
            ],
            SELECT_HOME_TEAM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_home_team)
            ],
            SELECT_AWAY_TEAM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_away_team)
            ],
            SELECT_MAIN_CATEGORY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_main_category)
            ],
            SELECT_SUB_CATEGORY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_sub_category)
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(filters.Text(["❌ Завершить"]), cancel),
            CommandHandler('start', start)
        ],
    )
    
    application.add_handler(conv_handler)
    
    # Обработчик для главного меню
    application.add_handler(MessageHandler(filters.Text(["🏟️ Старт анализа"]), start_analysis))
    application.add_handler(MessageHandler(filters.Text(["🔁 Продолжить анализ"]), continue_analysis))
    application.add_handler(MessageHandler(filters.Text(["❌ Завершить"]), cancel))
    
    # Запускаем бота
    print("🤖 Бот запущен...")
    application.run_polling()

if __name__ == '__main__':
    main()