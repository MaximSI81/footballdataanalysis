from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from football_match_forecast import get_match_analysis_from_clickhouse
import io
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Состояния разговора
SELECT_HOME_TEAM, SELECT_AWAY_TEAM, SELECT_DATA_TYPES = range(3)

# Словарь команд
TEAMS = {
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

# Типы данных для вывода
DATA_TYPES = {
    "📊 Основная статистика": "basic",
    "⚽ Результативность": "goals", 
    "🎯 Удары и xG": "shots",
    "🟨 Дисциплина": "discipline",
    "📈 Форма и H2H": "form",
    "💰 Прогнозы": "predictions",
    "📈 Все данные": "all"
}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало работы с ботом"""
    welcome_text = """
🏟️ Футбольный анализатор матчей

Я помогу проанализировать предстоящий матч на основе статистики.

Выберите команду хозяев:
    """
    
    # Создаем клавиатуру с командами (по 3 в ряд для компактности)
    team_buttons = [list(TEAMS.keys())[i:i+3] for i in range(0, len(TEAMS), 3)]
    reply_keyboard = team_buttons
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=ReplyKeyboardMarkup(
            reply_keyboard, 
            one_time_keyboard=True,
            resize_keyboard=True
        )
    )
    
    return SELECT_HOME_TEAM

async def select_home_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора команды хозяев"""
    home_team = update.message.text
    
    if home_team not in TEAMS:
        await update.message.reply_text("❌ Пожалуйста, выберите команду из списка:")
        return SELECT_HOME_TEAM
    
    context.user_data['home_team'] = home_team
    context.user_data['home_team_id'] = TEAMS[home_team]
    
    # Создаем клавиатуру для гостевой команды (исключаем домашнюю)
    other_teams = [team for team in TEAMS.keys() if team != home_team]
    away_buttons = [other_teams[i:i+2] for i in range(0, len(other_teams), 2)]
    
    await update.message.reply_text(
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
    
    if away_team not in TEAMS:
        await update.message.reply_text("❌ Пожалуйста, выберите команду из списка:")
        return SELECT_AWAY_TEAM
    
    context.user_data['away_team'] = away_team
    context.user_data['away_team_id'] = TEAMS[away_team]
    
    # Создаем клавиатуру для выбора типа данных
    data_buttons = [list(DATA_TYPES.keys())[i:i+2] for i in range(0, len(DATA_TYPES), 2)]
    
    await update.message.reply_text(
        f"🏠 Хозяева: {context.user_data['home_team']}\n"
        f"🛬 Гости: {away_team}\n\n"
        f"Выберите какие данные показать:",
        reply_markup=ReplyKeyboardMarkup(
            data_buttons,
            one_time_keyboard=True,
            resize_keyboard=True
        )
    )
    
    return SELECT_DATA_TYPES

async def get_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получение и вывод анализа"""
    selected_data_type = update.message.text
    
    if selected_data_type not in DATA_TYPES:
        await update.message.reply_text("❌ Пожалуйста, выберите тип данных из списка:")
        return SELECT_DATA_TYPES
    
    data_type = DATA_TYPES[selected_data_type]
    
    # Получаем данные из контекста
    home_team = context.user_data['home_team']
    away_team = context.user_data['away_team']
    home_team_id = context.user_data['home_team_id']
    away_team_id = context.user_data['away_team_id']
    
    await update.message.reply_text("⏳ Анализирую данные... Это может занять несколько секунд")
    
    try:
        # Перехватываем вывод вашей функции
        old_stdout = sys.stdout
        new_stdout = io.StringIO()
        sys.stdout = new_stdout
        
        # Запускаем ваш анализ
        get_match_analysis_from_clickhouse(
            team1_id=home_team_id,
            team2_id=away_team_id,
            team1_name=home_team,
            team2_name=away_team,
            season_id=77142
        )
        
        # Получаем вывод
        analysis_output = new_stdout.getvalue()
        sys.stdout = old_stdout
        
        if not analysis_output or len(analysis_output.strip()) < 100:
            await update.message.reply_text("❌ Не удалось получить данные анализа")
            return await start(update, context)
        
        # Фильтруем вывод по выбранному типу данных
        filtered_output = filter_output(analysis_output, data_type)
        
        # Разбиваем на части если слишком длинное сообщение
        messages = split_message(filtered_output)
        
        for i, msg in enumerate(messages):
            # Для первого сообщения добавляем заголовок
            if i == 0:
                header = f"🏟️ АНАЛИЗ МАТЧА:\n{home_team} 🆚 {away_team}\n\n"
                full_msg = header + msg
            else:
                full_msg = msg
                
            # Отправляем как обычный текст вместо Markdown
            await update.message.reply_text(full_msg)
            
        # Предлагаем новый анализ
        team_buttons = [list(TEAMS.keys())[i:i+2] for i in range(0, len(TEAMS), 2)]
        await update.message.reply_text(
            "🔄 Хотите проанализировать другой матч? Выберите команду хозяев:",
            reply_markup=ReplyKeyboardMarkup(
                team_buttons,
                one_time_keyboard=True,
                resize_keyboard=True
            )
        )
        
        return SELECT_HOME_TEAM
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при анализе: {str(e)}")
        return await start(update, context)

def filter_output(output: str, data_type: str) -> str:
    """Фильтрует вывод по выбранному типу данных"""
    if data_type == "all":
        return output
    
    lines = output.split('\n')
    filtered_lines = []
    
    # Включаем основные секции в зависимости от типа
    include_section = False
    
    for line in lines:
        # Определяем начало новой секции
        if any(marker in line for marker in ["🏠", "⚽", "🎯", "🟨", "📊", "🛡️", "📈", "🏆", "💰", "🎪"]):
            include_section = should_include_section(line, data_type)
        
        if include_section:
            filtered_lines.append(line)
    
    # Если ничего не отфильтровалось, возвращаем краткий обзор
    if not filtered_lines:
        return get_brief_overview(output)
    
    return '\n'.join(filtered_lines)

def should_include_section(line: str, data_type: str) -> bool:
    """Определяет, нужно ли включать секцию"""
    section_markers = {
        "basic": ["🏠", "⚽", "🎯", "🟨", "📊", "🛡️"],
        "goals": ["⚽", "РЕЗУЛЬТАТИВНОСТЬ", "xG", "ЭФФЕКТИВНОСТЬ"],
        "shots": ["🎯", "УДАРЫ", "xG", "УГЛОВЫЕ", "СТАНДАРТНЫЕ"],
        "discipline": ["🟨", "ДИСЦИПЛИНА", "ФОЛЫ", "КАРТОЧКИ"],
        "form": ["📈", "ФОРМА", "H2H", "ИСТОРИЧЕСКИЕ", "ДОМАШНИЕ/ГОСТЕВЫЕ"],
        "predictions": ["🏆", "💰", "🎯", "ПРОГНОЗ", "РЕКОМЕНДАЦИИ", "РЫНКИ", "ИНСАЙТЫ"]
    }
    
    return any(marker in line for marker in section_markers.get(data_type, []))

def get_brief_overview(output: str) -> str:
    """Возвращает краткий обзор если фильтр пустой"""
    lines = output.split('\n')
    overview = []
    
    # Добавляем ключевые строки
    for line in lines:
        if any(keyword in line for keyword in ["ГОЛОВ ЗА МАТЧ", "xG", "ПРОГНОЗ", "РЕКОМЕНДАЦИИ"]):
            overview.append(line)
    
    return '\n'.join(overview) if overview else output

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

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена операции"""
    await update.message.reply_text(
        "Операция отменена. Используйте /start для нового анализа.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END

def main():
    """Запуск бота"""
    # ЗАМЕНИТЕ НА ВАШ ТОКЕН
    TOKEN = "YOUR_BOT_TOKEN_HERE"
    
    application = Application.builder().token(os.getenv("YOUR_BOT_TOKEN")).build()
    
    # Создаем обработчик диалога
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            SELECT_HOME_TEAM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_home_team)
            ],
            SELECT_AWAY_TEAM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_away_team)
            ],
            SELECT_DATA_TYPES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_analysis)
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    
    application.add_handler(conv_handler)
    
    # Запускаем бота
    print("🤖 Бот запущен...")
    application.run_polling()

if __name__ == '__main__':
    main()