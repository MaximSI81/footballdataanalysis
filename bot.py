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
    "📋 Полный отчет": "full_report"
}

# Подкатегории для каждого раздела
SUB_CATEGORIES = {
    "overview": {
        "📊 Весь раздел 'Обзор'": "overview_all",
        "📊 Сводная таблица": "overview_summary",
        "🎯 Ключевые показатели": "overview_key_metrics",
        "⚡ Быстрые выводы": "overview_quick_insights"
    },
    "attack": {
        "⚽ Весь раздел 'Атака'": "attack_all",
        "⚽ Результативность": "attack_goals", 
        "🎯 xG и удары": "attack_shots",
        "📈 Эффективность": "attack_efficiency",
        "🔄 Кроссы и фланги": "attack_crosses",        
        "🎯 Длинные передачи": "attack_longballs",    
    },
    "stats": {
        "🛡️ Весь раздел 'Статистика'": "stats_all",
        "⚖️ Владение мячом": "stats_possession",
        "📊 Пасы": "stats_passing",
        "🟨 Дисциплина": "stats_discipline", 
        "🟨 Агрессивность": "stats_aggression",
    },
    "form_h2h": {
        "📈 Весь раздел 'Форма'": "form_h2h_all",
        "📅 Последние матчи": "form_recent",
        "🤝 H2H история": "form_h2h",
        "🏠 Дома/в гостях": "form_home_away"
    },
    "predictions": {
        "💰 Весь раздел 'Прогнозы'": "predictions_all",
        "🎯 Вероятности": "predictions_probabilities",
        "💡 Рекомендации": "predictions_recommendations",
        "🔍 Инсайты": "predictions_insights",
        "🟨 Карточки": "predictions_cards",
        "🏠 Дома/в гостях": "predictions_home_away"
    },
    "players": {
        "⭐ Весь раздел 'Игроки'": "players_all",
        "👥 Ключевые игроки": "players_key",
        "⚔️ Противостояния": "players_matchups"
    }
}

# Клавиатура с основными командами
MAIN_KEYBOARD = [
    ["🏟️ Старт анализа", "❌ Завершить работу"]
]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало работы с ботом"""
    welcome_text = """
🏟️ Футбольный анализатор матчей

Я помогу проанализировать предстоящий матч на основе статистики.

Нажмите "🏟️ Старт анализа" для начала анализа матча или "❌ Завершить работу" для выхода.
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
    if update.message.text == "❌ Завершить работу":
        return await cancel(update, context)
    
    welcome_text = """
🏟️ Футбольный анализатор матчей

Выберите лигу для анализа:
    """
    
    # Создаем клавиатуру с лигами
    league_buttons = [list(LEAGUES.keys())[i:i+2] for i in range(0, len(LEAGUES), 2)]
    reply_keyboard = league_buttons
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=ReplyKeyboardMarkup(
            reply_keyboard, 
            one_time_keyboard=True,
            resize_keyboard=True
        )
    )
    
    return SELECT_LEAGUE

async def select_league(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора лиги"""
    selected_league = update.message.text
    
    if selected_league not in LEAGUES:
        await update.message.reply_text("❌ Пожалуйста, выберите лигу из списка:")
        return SELECT_LEAGUE
    
    # Сохраняем данные лиги в контексте
    context.user_data['league'] = selected_league
    context.user_data['tournament_id'] = LEAGUES[selected_league]['id']
    context.user_data['season_id'] = LEAGUES[selected_league]['season_id']
    context.user_data['teams'] = LEAGUES[selected_league]['teams']
    
    # Создаем клавиатуру с командами выбранной лиги
    team_buttons = [list(LEAGUES[selected_league]['teams'].keys())[i:i+3] for i in range(0, len(LEAGUES[selected_league]['teams']), 3)]
    
    await update.message.reply_text(
        f"🏆 Лига: {selected_league}\n\n"
        f"Выберите команду хозяев:",
        reply_markup=ReplyKeyboardMarkup(
            team_buttons,
            one_time_keyboard=True,
            resize_keyboard=True
        )
    )
    
    return SELECT_HOME_TEAM

async def select_home_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора команды хозяев"""
    home_team = update.message.text
    teams = context.user_data['teams']
    
    if home_team not in teams:
        await update.message.reply_text("❌ Пожалуйста, выберите команду из списка:")
        return SELECT_HOME_TEAM
    
    context.user_data['home_team'] = home_team
    context.user_data['home_team_id'] = teams[home_team]
    
    # Создаем клавиатуру для гостевой команды (исключаем домашнюю)
    other_teams = [team for team in teams.keys() if team != home_team]
    away_buttons = [other_teams[i:i+2] for i in range(0, len(other_teams), 2)]
    
    await update.message.reply_text(
        f"🏆 Лига: {context.user_data['league']}\n"
        f"🏠 Хозяева: {home_team}\n\n"
        f"Выберите команду гостей:",
        reply_markup=ReplyKeyboardMarkup(
            away_buttons,
            one_time_keyboard=True,
            resize_keyboard=True
        )
    )
    
    return SELECT_AWAY_TEAM

async def select_away_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора команды гостей"""
    away_team = update.message.text
    teams = context.user_data['teams']
    
    if away_team not in teams:
        await update.message.reply_text("❌ Пожалуйста, выберите команду из списка:")
        return SELECT_AWAY_TEAM
    
    context.user_data['away_team'] = away_team
    context.user_data['away_team_id'] = teams[away_team]
    
    # Создаем клавиатуру для выбора основной категории
    category_buttons = [list(MAIN_CATEGORIES.keys())[i:i+2] for i in range(0, len(MAIN_CATEGORIES), 2)]
    
    await update.message.reply_text(
        f"🏆 Лига: {context.user_data['league']}\n"
        f"🏠 Хозяева: {context.user_data['home_team']}\n"
        f"🛬 Гости: {away_team}\n\n"
        f"Выберите раздел анализа:",
        reply_markup=ReplyKeyboardMarkup(
            category_buttons,
            one_time_keyboard=True,
            resize_keyboard=True
        )
    )
    
    return SELECT_MAIN_CATEGORY

async def select_main_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора основной категории"""
    selected_category = update.message.text
    
    if selected_category not in MAIN_CATEGORIES:
        await update.message.reply_text("❌ Пожалуйста, выберите раздел из списка:")
        return SELECT_MAIN_CATEGORY
    
    category_key = MAIN_CATEGORIES[selected_category]
    context.user_data['selected_main_category'] = category_key
    
    # Если выбран полный отчет - сразу запускаем анализ
    if category_key == "full_report":
        return await get_analysis(update, context, "all")
    
    # Для остальных категорий показываем подкатегории
    sub_categories = SUB_CATEGORIES.get(category_key, {})
    sub_buttons = [list(sub_categories.keys())[i:i+2] for i in range(0, len(sub_categories), 2)]
    
    await update.message.reply_text(
        f"📂 Раздел: {selected_category}\n\n"
        f"Выберите что показать:",
        reply_markup=ReplyKeyboardMarkup(
            sub_buttons,
            one_time_keyboard=True,
            resize_keyboard=True
        )
    )
    
    return SELECT_SUB_CATEGORY

async def select_sub_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора подкатегории"""
    selected_sub = update.message.text
    main_category = context.user_data['selected_main_category']
    
    sub_categories = SUB_CATEGORIES.get(main_category, {})
    
    if selected_sub not in sub_categories:
        await update.message.reply_text("❌ Пожалуйста, выберите опцию из списка:")
        return SELECT_SUB_CATEGORY
    
    data_type = sub_categories[selected_sub]
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
    """Фильтрует вывод по выбранному типу данных"""
    if data_type == "all":
        return output
    
    # Для "весь раздел" фильтруем только соответствующие секции
    if data_type.endswith("_all"):
        main_category = data_type.replace("_all", "")
        return filter_by_main_category(output, main_category)
    
    lines = output.split('\n')
    filtered_lines = []
    
    # Включаем основные секции в зависимости от типа
    include_section = False
    current_section = ""
    
    for line in lines:
        # Определяем начало новой секции
        if any(marker in line for marker in ["🎯", "⚽", "🟨", "📊", "🛡️", "📈", "🏆", "💰", "🎪", "⭐", "⚔️", "🔑"]):
            current_section = line.strip()
            include_section = should_include_section(current_section, data_type)
        
        if include_section:
            filtered_lines.append(line)
    
    # Если ничего не отфильтровалось, возвращаем краткий обзор
    if not filtered_lines:
        return get_brief_overview(output, data_type)
    
    return '\n'.join(filtered_lines)

def filter_by_main_category(output: str, main_category: str) -> str:
    """Фильтрует вывод по основной категории"""
    lines = output.split('\n')
    filtered_lines = []
    
    include_section = False
    current_section = ""
    
    for line in lines:
        # Определяем начало новой секции
        if any(marker in line for marker in ["🎯", "⚽", "🟨", "📊", "🛡️", "📈", "🏆", "💰", "🎪", "⭐", "⚔️", "🔑"]):
            current_section = line.strip()
            include_section = should_include_main_category(current_section, main_category)
        
        if include_section:
            filtered_lines.append(line)
    
    return '\n'.join(filtered_lines) if filtered_lines else get_brief_overview(output, main_category)

def should_include_main_category(section_line: str, main_category: str) -> bool:
    """Определяет, нужно ли включать секцию для основной категории"""
    category_mapping = {
        "overview": ["🏠", "🛬", "📍", "🏆 ПОЗИЦИЯ", "⚡ БЫСТРЫЕ", "🎯 КЛЮЧЕВЫЕ"],
        "attack": ["⚽", "🎯 УДАРЫ", "📈 РЕАЛЬНЫЙ xG", "🎯 ЭФФЕКТИВНОСТЬ", 
                   "🏹 АТАКУЕТ", "🔄 АКТИВНОСТЬ ФЛАНГОВ", "🎯 ДАЛЬНИЕ АТАКИ"],
        "stats": ["📊 КОНТРОЛЬ", "🔄 ТОЧНОСТЬ", "🛡️ ОБОРОНА", "🎪 КАЧЕСТВО", 
                  "🟨 ДИСЦИПЛИНА", "🟨 АГРЕССИВНОСТЬ"],
        "form_h2h": ["📈 ФОРМА", "🤝 ВСЕГО МАТЧЕЙ", "📊 ИСТОРИЯ", "📈 СРЕДНИЕ ПОКАЗАТЕЛИ"],
        "predictions": ["🏆 ПРОГНОЗ", "💰 РЕКОМЕНДАЦИИ", "📈 КЛЮЧЕВЫЕ ИНСАЙТЫ", 
                       "🎲 ЭКСКЛЮЗИВНЫЕ ПРОГНОЗЫ"],
        "players": ["⭐ АНАЛИЗ ПРОГРЕССА", "🔑 КЛЮЧЕВЫЕ ИГРОКИ"],
    }
    
    markers = category_mapping.get(main_category, [])
    return any(marker in section_line for marker in markers)

def should_include_section(section_line: str, data_type: str) -> bool:
    """Определяет, нужно ли включать секцию для выбранного типа данных"""
    section_mapping = {
        # Общий обзор
        "overview_summary": ["🏠", "🛬", "📍", "⚽", "🥅", "🎯", "⚖️", "📊", "🟨", "📈"],
        "overview_key_metrics": ["🎯 КЛЮЧЕВЫЕ", "⚡ ПРЕИМУЩЕСТВА"],
        "overview_quick_insights": ["⚡ БЫСТРЫЕ", "🎯 ВЫВОДЫ"],
        
        # Атака
        "attack_goals": ["⚽", "РЕЗУЛЬТАТИВНОСТЬ", "ГОЛОВ"],
        "attack_shots": ["🎯", "УДАРЫ", "xG"],
        "attack_efficiency": ["ЭФФЕКТИВНОСТЬ", "ТОЧНОСТЬ"],
        "attack_crosses": ["🔄", "АКТИВНОСТЬ ФЛАНГОВ", "КРОССОВ"],
        "attack_longballs": ["🎯", "ДАЛЬНИЕ АТАКИ", "ДЛИННЫХ ПЕРЕДАЧ"],
        
        # Статистика
        "stats_possession": ["⚖️", "ВЛАДЕНИЕ", "КОНТРОЛЬ"],
        "stats_passing": ["📊", "ПАСЫ", "ПЕРЕДАЧИ"],
        "stats_discipline": ["🟨", "ДИСЦИПЛИНА", "КАРТОЧКИ"],
        "stats_aggression": ["🟨", "АГРЕССИВНОСТЬ", "ФОЛОВ"],
        
        # Форма и H2H
        "form_recent": ["📅", "ПОСЛЕДНИЕ", "ФОРМА"],
        "form_h2h": ["🤝", "H2H", "ИСТОРИЧЕСКИЕ"],
        "form_home_away": ["🏠", "ДОМАШНИЕ/ГОСТЕВЫЕ"],
        
        # Прогнозы
        "predictions_probabilities": ["🎯", "ВЕРОЯТНОСТИ", "ПРОГНОЗ"],
        "predictions_recommendations": ["💡", "РЕКОМЕНДАЦИИ"],
        "predictions_insights": ["🔍", "ИНСАЙТЫ", "КЛЮЧЕВЫЕ ИНСАЙТЫ"],
        "predictions_cards": ["🎲 ЭКСКЛЮЗИВНЫЕ ПРОГНОЗЫ", "🟨 АНАЛИЗ ДИСЦИПЛИНЫ", "Рефери", "Прогноз желтых"],
        "predictions_home_away": ["🎲 ЭКСКЛЮЗИВНЫЕ ПРОГНОЗЫ", "🏠🛬 ПРОГНОЗ РЕЗУЛЬТАТА", "ДОМАШНЕГО СТАДИОНА", "Вероятности исходов"],
        
        # Игроки
        "players_key": ["⭐ АНАЛИЗ ПРОГРЕССА", "🔑 КЛЮЧЕВЫЕ ИГРОКИ"],
        "players_matchups": ["⚔️", "ПРОТИВОСТОЯНИЯ"]
    }
    
    markers = section_mapping.get(data_type, [])
    return any(marker in section_line for marker in markers)

def get_brief_overview(output: str, data_type: str) -> str:
    """Возвращает краткий обзор если фильтр пустой"""
    lines = output.split('\n')
    overview = []
    
    # Для раздела игроков ищем конкретные маркеры
    if "players" in data_type:
        for line in lines:
            if any(marker in line for marker in ["⭐", "🔑", "⚔️", "КЛЮЧЕВЫЕ ИГРОКИ", "Голы:", "Ассисты:", "Удары:"]):
                overview.append(line)
    
    # Если не нашли данные игроков, возвращаем сообщение
    if not overview and "players" in data_type:
        return "⭐️ АНАЛИЗ ПРОГРЕССА КЛЮЧЕВЫХ ИГРОКОВ:\n\n🔑 Статистика появится после 3-х сыгранных туров"
    
    return '\n'.join(overview) if overview else "📊 Данные для выбранного раздела не найдены"
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
        "Анализ завершен! Хотите проанализировать другой матч?",
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
            MessageHandler(filters.Text(["🏟️ Старт анализа"]), start_analysis)
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
            MessageHandler(filters.Text(["❌ Завершить работу"]), cancel),
            CommandHandler('start', start)
        ],
    )
    
    application.add_handler(conv_handler)
    
    # Обработчик для главного меню
    application.add_handler(MessageHandler(filters.Text(["🏟️ Старт анализа"]), start_analysis))
    application.add_handler(MessageHandler(filters.Text(["❌ Завершить работу"]), cancel))
    
    # Запускаем бота
    print("🤖 Бот запущен...")
    application.run_polling()

if __name__ == '__main__':
    main()