"""
Телеграм-бот для расчёта партнёрского вознаграждения OneSpot.
Установка: pip install python-telegram-bot==21.3
Запуск: python3.11 bot.py
"""

import os
import sqlite3
import logging
from datetime import datetime, time
from collections import defaultdict
from urllib.parse import quote
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
MANAGER_USERNAME = "k_onespot"
MANAGER_CHAT_ID = -1003651651617  # Чат для логов

REMINDER_INTERVAL = 30 * 60  # 30 минут в секундах
MAX_REMINDERS = 3

# ─── Статистика за день ───────────────────────────────────────────────────────
stats = {
    "opens": 0,           # Открытий бота
    "completed": 0,       # Завершённых расчётов
    "abandoned": 0,       # Брошенных расчётов
    "kb_visits": 0,       # Переходов в базу знаний
    "users": [],          # Список пользователей
    "grades": defaultdict(int),  # Расчётов по грейдам
}

def reset_stats():
    global stats
    stats = {
        "opens": 0,
        "completed": 0,
        "abandoned": 0,
        "kb_visits": 0,
        "users": [],
        "grades": defaultdict(int),
    }

# ─── База данных ─────────────────────────────────────────────────────────────
DB_PATH = "onespot.db"

def db_init():
    """Создать таблицы если не существуют"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_seen TEXT,
            last_seen TEXT,
            opens INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS calculations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            grade TEXT,
            turnover REAL,
            total_reward_max REAL,
            platforms TEXT,
            completed_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            event_type TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def db_track_user(user_id: int, username: str):
    """Записать/обновить пользователя"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO users (user_id, username, first_seen, last_seen, opens)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            last_seen=excluded.last_seen,
            opens=opens+1
    """, (user_id, username, now, now))
    c.execute("INSERT INTO events (user_id, username, event_type, created_at) VALUES (?, ?, 'open', ?)",
              (user_id, username, now))
    conn.commit()
    conn.close()


def db_track_calc(user_id: int, username: str, grade: str, turnover: float, reward: float, platforms: str):
    """Записать завершённый расчёт"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO calculations (user_id, username, grade, turnover, total_reward_max, platforms, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, username, grade, turnover, reward, platforms, now))
    c.execute("INSERT INTO events (user_id, username, event_type, created_at) VALUES (?, ?, 'completed', ?)",
              (user_id, username, now))
    conn.commit()
    conn.close()


def db_track_event(user_id: int, username: str, event_type: str):
    """Записать событие (kb_visit, abandoned)"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO events (user_id, username, event_type, created_at) VALUES (?, ?, ?, ?)",
              (user_id, username, event_type, now))
    conn.commit()
    conn.close()


def db_get_daily_stats(date_str: str) -> dict:
    """Получить статистику за день"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM events WHERE event_type='open' AND created_at LIKE ?", (f"{date_str}%",))
    opens = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM events WHERE event_type='completed' AND created_at LIKE ?", (f"{date_str}%",))
    completed = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM events WHERE event_type='abandoned' AND created_at LIKE ?", (f"{date_str}%",))
    abandoned = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM events WHERE event_type='kb_visit' AND created_at LIKE ?", (f"{date_str}%",))
    kb_visits = c.fetchone()[0]

    c.execute("""
        SELECT DISTINCT username FROM events
        WHERE event_type='open' AND created_at LIKE ? AND username IS NOT NULL
    """, (f"{date_str}%",))
    users = [row[0] for row in c.fetchall()]

    c.execute("""
        SELECT grade, COUNT(*) FROM calculations
        WHERE completed_at LIKE ? GROUP BY grade
    """, (f"{date_str}%",))
    grades = dict(c.fetchall())

    conn.close()
    return {
        "opens": opens, "completed": completed, "abandoned": abandoned,
        "kb_visits": kb_visits, "users": users, "grades": grades
    }


# ─── Состояния диалога ───────────────────────────────────────────────────────
(WELCOME, SELECT_YANDEX, SELECT_OTHER, ENTER_TOTAL_TURNOVER, ENTER_BUDGET,
 ENTER_CLIENTS, ENTER_MEDIA, SHOW_RESULT, KNOWLEDGE_BASE) = range(9)

# ─── Тиры оборота ────────────────────────────────────────────────────────────
TIERS = [
    (1, 500_000),
    (500_001, 1_000_000),
    (1_000_001, 3_000_000),
    (3_000_001, 5_000_000),
    (5_000_001, 10_000_000),
    (10_000_001, None),
]

# ─── Ставки по площадкам ─────────────────────────────────────────────────────
RATES = {
    "Яндекс: Поиск": [
        (0, 4), (4.5, 4.5), (5, 5), (5.5, 5.5), (5.5, 5.5), (0, 6)
    ],
    "Яндекс: РСЯ": [
        (0, 6), (6, 6), (7.5, 7.5), (0, 8.5), (9, 9), (9, 9)
    ],
    "Яндекс: Продвижение приложений": [
        (10, 10), (13, 13), (15, 15), (0, 19), (20, 20), (26, 26)
    ],
    "Яндекс: ECOM (+к РСЯ/Поиск)": [
        (5, 5), (7, 7), (0, 9), (0, 11), (12, 12), (13, 13)
    ],
    "Яндекс: Медийка": [
        (5, 5), (7, 7), (9, 9), (0, 11), (11.5, 11.5), (12, 12)
    ],
    "Яндекс: Видеореклама": [
        (5, 5), (7, 7), (9, 9), (0, 11), (11.5, 11.5), (12, 12)
    ],
    "Яндекс: Медийка в картах": [
        (5, 5), (8, 8), (10, 12), (0, 16), (17, 17), (26, 26)
    ],
    "Яндекс: Промостраницы": [
        (4, 4), (8, 8), (10, 10), (0, 12), (13, 13), (13, 13)
    ],
    "Яндекс: Реклама в ТГ-каналах": [
        (1, 1), (2, 2), (2, 2), (0, 3.5), (3.5, 3.5), (4, 4)
    ],
    "Telegram Ads": [
        (3, 3), (3.5, 3.5), (4, 4), (5, 5), (5, 5), (5, 5)
    ],
    "Таргетированная реклама": [
        (0, 8), (0, 8), (0, 8), (0, 12), (0, 12), (0, 12)
    ],
    "Авито объявления": [
        (3, 3), (5, 5), (6, 6), (0, 8), (0, 8), (0, 8)
    ],
    "Авито реклама": [
        (0, 13), (13.5, 13.5), (13.5, 13.5), (15, 15), (15, 15), (15, 15)
    ],
    "Telega.in": [
        (0, 2), (2, 2), (3, 3), (0, 4), (0, 4), (0, 4)
    ],
    "TikTok Home Reg": [
        (0, 0), (0, 6), (0, 6), (0, 6), (0, 6), (0, 6)
    ],
    "Ozon": [
        (0, 17), (17, 17), (20, 20), (25, 25), (25, 25), (25, 25)
    ],
    "Wildberries": [
        (0, 0), (0, 0), (12, 12), (15, 15), (15, 15), (15, 15)
    ],
}

PLATFORMS = list(RATES.keys())
YANDEX_PLATFORMS = [i for i, name in enumerate(PLATFORMS) if name.startswith("Яндекс")]
OTHER_PLATFORMS = [i for i, name in enumerate(PLATFORMS) if not name.startswith("Яндекс")]

ECOM_NAME = "Яндекс: ECOM (+к РСЯ/Поиск)"
ECOM_IDX = PLATFORMS.index(ECOM_NAME)
SEARCH_IDX = PLATFORMS.index("Яндекс: Поиск")
RSY_IDX = PLATFORMS.index("Яндекс: РСЯ")

# ─── База знаний ─────────────────────────────────────────────────────────────
KNOWLEDGE = {
    "kb_main": {
        "title": "📚 База знаний — Реферальная программа OneSpot",
        "text": (
            "Реферальная программа OneSpot — это многоуровневая система, "
            "в которой вы можете строить собственную реферальную сеть и зарабатывать "
            "процент от рекламных расходов участников этой сети.\n\n"
            "Выберите раздел:"
        ),
        "sections": [
            ("📖 Основные понятия", "kb_terms"),
            ("📊 Грейд", "kb_grade"),
            ("🔀 Сплит", "kb_split"),
            ("🌐 Реферальная сеть", "kb_network"),
        ]
    },
    "kb_terms": {
        "title": "📖 Основные понятия",
        "text": (
            "*Партнёр* — пользователь, подключённый к реферальной программе и создающий свои реферальные ссылки.\n\n"
            "*Реферал* — пользователь, зарегистрировавшийся по ссылке.\n\n"
            "*Реферал-партнёр* — пользователь, зарегистрировавшийся по ссылке *и* подключённый к реферальной программе, создающий свои реферальные ссылки.\n\n"
            "*Реферальная сеть* — структура из участников, которых вы привели напрямую (1 уровень), и тех, кого привели ваши рефералы (2 уровень). Количество уровней не ограничено.\n\n"
            "*Грейд* — персональная ставка вознаграждения для каждого партнёра по каждой рекламной площадке.\n\n"
            "*Сплит* — правило распределения грейда между вами и вашей реферальной сетью."
        ),
    },
    "kb_grade": {
        "title": "📊 Грейд",
        "text": (
            "*Грейд* — персональная ставка вознаграждения для каждого партнёра по каждой рекламной площадке. Устанавливается индивидуально после интервью.\n\n"
            "• Грейд может различаться в зависимости от рекламной площадки.\n\n"
            "❗ *Важно:* грейд не фиксируется навсегда. Он может быть пересмотрен в зависимости от активности и качества привлечённых клиентов."
        ),
    },
    "kb_split": {
        "title": "🔀 Сплит",
        "text": (
            "*Сплит* — правило распределения грейда между вами и вашей реферальной сетью. Вы можете задавать его самостоятельно.\n\n"
            "• Сплит задаётся в процентах от вашего грейда (не от общего рекламного бюджета).\n\n"
            "• Можно настроить универсально — для всей ссылки.\n"
            "• Или индивидуально по площадкам — например, 70/30 по VK и 50/50 по Telegram.\n\n"
            "• Глубина деления — неограниченная. Бонус автоматически делится вниз по цепочке.\n\n"
            "❗ *Важно:* сплит не фиксируется навсегда. Партнёр может изменить его в разделе настройки ссылки. Изменение начинает действовать с 1 числа следующего месяца."
        ),
    },
    "kb_network": {
        "title": "🌐 Реферальная сеть",
        "text": (
            "*Реферальная сеть* — структура участников, которых вы привели.\n\n"
            "• *1 уровень* — люди, которых привели напрямую вы.\n"
            "• *2 уровень* — люди, которых привели ваши рефералы.\n"
            "• Количество уровней не ограничено.\n\n"
            "Вы получаете процент от рекламных расходов всех участников вашей сети в соответствии с вашим грейдом и настройками сплита."
        ),
    },
}


# ─── Вспомогательные функции ─────────────────────────────────────────────────

def get_tier_index(turnover: float) -> int:
    for i, (lo, hi) in enumerate(TIERS):
        if hi is None or turnover <= hi:
            return i
    return len(TIERS) - 1


def format_pct(mn, mx):
    if mn == 0 and mx == 0:
        return "0%"
    if mn == mx:
        return f"{mn}%"
    return f"до {mx}%" if mn == 0 else f"{mn}–{mx}%"


def tier_label(index: int) -> str:
    lo, hi = TIERS[index]
    lo_s = f"{lo:,}".replace(",", " ")
    if hi is None:
        return f"от {lo_s} ₽"
    hi_s = f"{hi:,}".replace(",", " ")
    return f"{lo_s} – {hi_s} ₽"


def parse_number(text: str) -> float:
    return float(text.strip().replace(" ", "").replace(",", "."))


def has_search_or_rsy(selected: set) -> bool:
    return SEARCH_IDX in selected or RSY_IDX in selected


def fmt_money(amount: float) -> str:
    return f"{amount:,.0f}".replace(",", " ") + " ₽"


def fmt_range(mn: float, mx: float) -> str:
    if mn == 0 or mn == mx:
        return fmt_money(mx)
    return f"{fmt_money(mn)} – {fmt_money(mx)}"


def manager_btn() -> list:
    return [InlineKeyboardButton(
        "📞 Связаться с менеджером",
        url=f"tg://resolve?domain={MANAGER_USERNAME}"
    )]


def log_user(user) -> str:
    username = f"@{user.username}" if user.username else "без username"
    return f"id={user.id} ({username})"


async def notify_manager(app, text: str):
    """Отправить лог менеджеру в чат"""
    if MANAGER_CHAT_ID:
        try:
            await app.bot.send_message(chat_id=MANAGER_CHAT_ID, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Не удалось отправить лог менеджеру: {e}")


async def daily_report_job(context: ContextTypes.DEFAULT_TYPE):
    """Ежедневный отчёт в 20:00"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_display = datetime.now().strftime("%d.%m.%Y")

    # Берём данные из БД
    db_stats = db_get_daily_stats(today)

    users_list = "\n".join(f"  • {u}" for u in db_stats["users"]) if db_stats["users"] else "  нет"

    grades_list = ""
    if db_stats["grades"]:
        for grade, count in sorted(db_stats["grades"].items()):
            grades_list += f"\n  • {grade}: {count} расч."
    else:
        grades_list = " нет"

    report = (
        f"📊 *Итоговый отчёт за {today_display}*\n\n"
        f"👀 Открытий бота: *{db_stats['opens']}*\n"
        f"✅ Завершённых расчётов: *{db_stats['completed']}*\n"
        f"❌ Брошенных расчётов: *{db_stats['abandoned']}*\n"
        f"📚 Переходов в базу знаний: *{db_stats['kb_visits']}*\n"
        f"\n📊 По грейдам:{grades_list}\n"
        f"\n👤 Кто заходил:\n{users_list}"
    )

    await notify_manager(context.application, report)
    reset_stats()
    logger.info("Ежедневный отчёт отправлен")


async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    """Напоминание пользователю если бросил расчёт"""
    job = context.job
    chat_id = job.data["chat_id"]
    reminder_num = job.data["reminder_num"]
    user_info = job.data["user_info"]

    reminder_num += 1
    job.data["reminder_num"] = reminder_num

    if reminder_num > MAX_REMINDERS:
        # Логируем что так и не завершил
        stats["abandoned"] += 1
        db_track_event(job.data["chat_id"], user_info, "abandoned")
        await notify_manager(
            context.application,
            f"❌ Пользователь {user_info} бросил расчёт и не вернулся после {MAX_REMINDERS} напоминаний"
        )
        return

    try:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🧮 Продолжить расчёт", callback_data="reminder_continue")],
            [InlineKeyboardButton("📞 Связаться с менеджером", url=f"tg://resolve?domain={MANAGER_USERNAME}")]
        ])
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "👋 Вы начали расчёт партнёрского вознаграждения, но не завершили его.\n\n"
                "Хотите продолжить? Напишите /start чтобы начать заново, "
                "или свяжитесь с менеджером напрямую."
            ),
            reply_markup=keyboard
        )
        logger.info(f"Напоминание #{reminder_num} отправлено: {user_info}")

        if reminder_num < MAX_REMINDERS:
            context.job_queue.run_once(
                reminder_job,
                REMINDER_INTERVAL,
                data={"chat_id": chat_id, "reminder_num": reminder_num, "user_info": user_info},
                name=f"reminder_{chat_id}"
            )
    except Exception as e:
        logger.error(f"Ошибка напоминания: {e}")


def schedule_reminder(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_info: str):
    """Запланировать напоминания"""
    cancel_reminders(context, chat_id)
    context.job_queue.run_once(
        reminder_job,
        REMINDER_INTERVAL,
        data={"chat_id": chat_id, "reminder_num": 0, "user_info": user_info},
        name=f"reminder_{chat_id}"
    )


def cancel_reminders(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Отменить все напоминания для пользователя"""
    jobs = context.job_queue.get_jobs_by_name(f"reminder_{chat_id}")
    for job in jobs:
        job.schedule_removal()


# ─── Построение итогового сообщения ──────────────────────────────────────────

def build_summary(context: ContextTypes.DEFAULT_TYPE, user) -> tuple:
    tier_idx = context.user_data["tier_idx"]
    total_turnover = context.user_data["total_turnover"]
    budgets = context.user_data["budgets"]
    selected_list = context.user_data["selected_list"]
    clients = context.user_data.get("clients", "не указано")
    media = context.user_data.get("media", "не указано")

    username = f"@{user.username}" if user.username else user.full_name
    user_id = user.id
    turnover_str = fmt_money(total_turnover)

    ecom_selected = ECOM_IDX in selected_list
    ecom_budget = budgets.get(ECOM_IDX, 0)
    ecom_pct_min, ecom_pct_max = RATES[ECOM_NAME][tier_idx]

    platform_lines = []
    manager_lines = []
    total_min = 0
    total_max = 0

    for platform_idx in selected_list:
        name = PLATFORMS[platform_idx]
        budget = budgets.get(platform_idx, 0)
        pct_min, pct_max = RATES[name][tier_idx]
        budget_str = fmt_money(budget)

        if platform_idx == ECOM_IDX:
            continue

        if pct_max == 0:
            platform_lines.append(f"▫️ *{name}*\n   Бюджет: {budget_str} — недоступно на этом обороте")
            manager_lines.append(f"• {name}: {budget_str} — недоступно")
            continue

        r_min = budget * pct_min / 100
        r_max = budget * pct_max / 100
        reward_str = fmt_range(r_min, r_max)
        pct_str = format_pct(pct_min, pct_max)

        if ecom_selected and platform_idx in (SEARCH_IDX, RSY_IDX) and ecom_pct_max > 0:
            ecom_r_min = ecom_budget * ecom_pct_min / 100
            ecom_r_max = ecom_budget * ecom_pct_max / 100
            total_min += r_min + ecom_r_min
            total_max += r_max + ecom_r_max
            ecom_reward_str = fmt_range(ecom_r_min, ecom_r_max)
            ecom_pct_str = format_pct(ecom_pct_min, ecom_pct_max)
            platform_lines.append(
                f"✅ *{name}*\n"
                f"   Бюджет: {budget_str}\n"
                f"   Ставка: {pct_str}\n"
                f"   Вознаграждение: ~{reward_str}\n"
                f"   ➕ ECOM бонус (бюджет {fmt_money(ecom_budget)}, {ecom_pct_str}): ~{ecom_reward_str}"
            )
            manager_lines.append(
                f"• {name}: {budget_str}, {pct_str}, ~{reward_str} "
                f"+ ECOM {fmt_money(ecom_budget)}, {ecom_pct_str}, ~{ecom_reward_str}"
            )
        else:
            total_min += r_min
            total_max += r_max
            platform_lines.append(
                f"✅ *{name}*\n"
                f"   Бюджет: {budget_str}\n"
                f"   Ставка: {pct_str}\n"
                f"   Вознаграждение: ~{reward_str}"
            )
            manager_lines.append(f"• {name}: {budget_str}, {pct_str}, ~{reward_str}")

    total_str = fmt_range(total_min, total_max) if total_max > 0 else "0 ₽"

    total_budgets = sum(budgets.get(i, 0) for i in selected_list)
    if abs(total_budgets - total_turnover) / max(total_turnover, 1) > 0.05:
        total_budgets_str = fmt_money(total_budgets)
        footer = f"\n⚠️ Сумма бюджетов ({total_budgets_str}) не совпадает с указанным оборотом ({turnover_str}). Финальный расчёт согласует менеджер."
    else:
        footer = "\n⚠️ Оборот — лишь один из факторов. Финальные условия согласует менеджер."

    user_msg = "\n".join([
        "📋 *Результаты расчёта*\n",
        f"💼 Суммарный оборот: *{turnover_str}*",
        f"👥 Количество клиентов: *{clients}*",
        f"🔗 Медийный вес: *{media}*\n",
        *platform_lines,
        f"\n💎 *Итого вознаграждение: ~{total_str}*",
        footer
    ])

    manager_msg = "\n".join([
        "👋 Привет! Хочу узнать финальные условия по вознаграждению.\n",
        f"💼 Суммарный оборот: {turnover_str}",
        f"📊 Грейд: {tier_label(tier_idx)}",
        f"👥 Количество клиентов: {clients}",
        f"🔗 Медийный вес: {media}\n",
        "📌 Площадки и бюджеты:",
        *manager_lines,
        f"\n💎 Расчётное вознаграждение: ~{total_str}",

    ])

    return user_msg, manager_msg


# ─── Хэндлеры ────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data["selected"] = set()
    user = update.effective_user
    user_info = log_user(user)
    logger.info(f"Новый пользователь: {user_info}")

    # Запускаем напоминания если не завершит
    schedule_reminder(context, update.effective_chat.id, user_info)

    # Статистика
    stats["opens"] += 1
    username_display = f"@{user.username}" if user.username else f"id:{user.id}"
    if username_display not in stats["users"]:
        stats["users"].append(username_display)
    db_track_user(user.id, username_display)

    await notify_manager(
        context.application,
        f"👀 Новый пользователь открыл бота: {user_info} — {datetime.now().strftime('%d.%m %H:%M')}"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🧮 Рассчитать вознаграждение", callback_data="go_calc")],
        [InlineKeyboardButton("📚 Узнать о программе", callback_data="go_kb")],
        [InlineKeyboardButton("📞 Связаться с менеджером", url=f"tg://resolve?domain={MANAGER_USERNAME}")],
    ])

    await (update.message or update.callback_query.message).reply_text(
        "👋 Добро пожаловать в реферальную программу *OneSpot*!\n\n"
        "Здесь вы можете рассчитать ориентировочное партнёрское вознаграждение "
        "или узнать больше о программе.\n\n"
        "Что вас интересует?",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    return WELCOME


async def welcome_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "go_calc":
        context.user_data["selected"] = set()
        await show_yandex_menu(update, context, edit=True)
        return SELECT_YANDEX

    if query.data == "go_kb":
        stats["kb_visits"] += 1
        user = update.effective_user
        db_track_event(user.id, f"@{user.username}" if user.username else f"id:{user.id}", "kb_visit")
        await show_kb_main(update, context, edit=True)
        return KNOWLEDGE_BASE


# ─── База знаний ─────────────────────────────────────────────────────────────

async def show_kb_main(update: Update, context: ContextTypes.DEFAULT_TYPE, edit=True):
    kb = KNOWLEDGE["kb_main"]
    keyboard = []
    for label, cb in kb["sections"]:
        keyboard.append([InlineKeyboardButton(label, callback_data=cb)])
    keyboard.append([InlineKeyboardButton("🧮 Рассчитать вознаграждение", callback_data="go_calc")])
    keyboard.append([InlineKeyboardButton("📞 Связаться с менеджером", url=f"tg://resolve?domain={MANAGER_USERNAME}")])
    markup = InlineKeyboardMarkup(keyboard)

    if edit and update.callback_query:
        await update.callback_query.edit_message_text(
            f"{kb['title']}\n\n{kb['text']}", reply_markup=markup, parse_mode="Markdown"
        )
    else:
        await (update.message or update.callback_query.message).reply_text(
            f"{kb['title']}\n\n{kb['text']}", reply_markup=markup, parse_mode="Markdown"
        )


async def kb_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "go_calc":
        context.user_data["selected"] = set()
        await show_yandex_menu(update, context, edit=True)
        return SELECT_YANDEX

    if data == "kb_back":
        await show_kb_main(update, context, edit=True)
        return KNOWLEDGE_BASE

    if data in KNOWLEDGE:
        kb = KNOWLEDGE[data]
        stats["kb_visits"] += 1
        keyboard = [
            [InlineKeyboardButton("⬅️ Назад", callback_data="kb_back")],
            [InlineKeyboardButton("🧮 Рассчитать вознаграждение", callback_data="go_calc")],
            [InlineKeyboardButton("📞 Связаться с менеджером", url=f"tg://resolve?domain={MANAGER_USERNAME}")],
        ]
        await query.edit_message_text(
            f"*{kb['title']}*\n\n{kb['text']}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return KNOWLEDGE_BASE


# ─── Калькулятор ─────────────────────────────────────────────────────────────

async def show_yandex_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit=True):
    selected: set = context.user_data.get("selected", set())
    keyboard = []

    for i in YANDEX_PLATFORMS:
        name = PLATFORMS[i]
        if i == ECOM_IDX and not has_search_or_rsy(selected):
            keyboard.append([InlineKeyboardButton(
                f"🔒 {name} (сначала выберите Поиск или РСЯ)", callback_data="ecom_locked"
            )])
            continue
        mark = "✅ " if i in selected else ""
        keyboard.append([InlineKeyboardButton(f"{mark}{name}", callback_data=f"plt_{i}")])

    keyboard.append([InlineKeyboardButton("➡️ Далее: другие площадки", callback_data="next_yandex")])
    keyboard.append([InlineKeyboardButton("🔄 Сбросить", callback_data="reset")])
    keyboard.append(manager_btn())

    markup = InlineKeyboardMarkup(keyboard)
    text = (
        "📊 *Калькулятор партнёрского вознаграждения OneSpot*\n\n"
        "*Шаг 1 из 2* — Яндекс площадки:\n\n"
        "_ECOM доступен только вместе с Поиском или РСЯ_"
    )
    if edit and update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=markup, parse_mode="Markdown")
    else:
        msg = update.message or update.callback_query.message
        await msg.reply_text(text, reply_markup=markup, parse_mode="Markdown")


async def show_other_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, edit=True):
    selected: set = context.user_data.get("selected", set())
    keyboard = []

    for i in OTHER_PLATFORMS:
        name = PLATFORMS[i]
        mark = "✅ " if i in selected else ""
        keyboard.append([InlineKeyboardButton(f"{mark}{name}", callback_data=f"plt_{i}")])

    keyboard.append([InlineKeyboardButton("⬅️ Назад: Яндекс", callback_data="back_yandex")])
    if selected:
        keyboard.append([InlineKeyboardButton("➡️ Продолжить", callback_data="next_other")])
    keyboard.append([InlineKeyboardButton("🔄 Сбросить всё", callback_data="reset")])
    keyboard.append(manager_btn())

    markup = InlineKeyboardMarkup(keyboard)
    yandex_selected = [PLATFORMS[i] for i in selected if i in YANDEX_PLATFORMS]
    yandex_str = ", ".join(yandex_selected) if yandex_selected else "не выбрано"

    text = (
        f"📊 *Шаг 2 из 2* — Другие площадки:\n\n"
        f"_Яндекс: {yandex_str}_\n\n"
        "Выберите дополнительные площадки или нажмите Продолжить:"
    )
    if edit and update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=markup, parse_mode="Markdown")
    else:
        msg = update.message or update.callback_query.message
        await msg.reply_text(text, reply_markup=markup, parse_mode="Markdown")


async def yandex_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "ecom_locked":
        await query.answer("Сначала выберите Яндекс: Поиск или Яндекс: РСЯ!", show_alert=True)
        return SELECT_YANDEX

    if data == "reset":
        context.user_data["selected"] = set()
        await show_yandex_menu(update, context, edit=True)
        return SELECT_YANDEX

    if data == "next_yandex":
        await show_other_menu(update, context, edit=True)
        return SELECT_OTHER

    if data.startswith("plt_"):
        idx = int(data.split("_")[1])
        selected: set = context.user_data.setdefault("selected", set())
        if idx in selected:
            selected.remove(idx)
            if idx in (SEARCH_IDX, RSY_IDX) and not has_search_or_rsy(selected):
                selected.discard(ECOM_IDX)
        else:
            selected.add(idx)
        context.user_data["selected"] = selected
        await show_yandex_menu(update, context, edit=True)
        return SELECT_YANDEX


async def other_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "reset":
        context.user_data["selected"] = set()
        await show_yandex_menu(update, context, edit=True)
        return SELECT_YANDEX

    if data == "back_yandex":
        await show_yandex_menu(update, context, edit=True)
        return SELECT_YANDEX

    if data == "next_other":
        selected = context.user_data.get("selected", set())
        if not selected:
            await query.answer("Выберите хотя бы одну площадку!", show_alert=True)
            return SELECT_OTHER
        await query.edit_message_text(
            "💼 Введите *суммарный ежемесячный оборот* по всем площадкам в рублях.\n\n"
            "Это нужно для определения вашего грейда вознаграждения.\n\n"
            "Пример: `2500000`",
            reply_markup=InlineKeyboardMarkup([manager_btn()]),
            parse_mode="Markdown"
        )
        return ENTER_TOTAL_TURNOVER

    if data.startswith("plt_"):
        idx = int(data.split("_")[1])
        selected: set = context.user_data.setdefault("selected", set())
        if idx in selected:
            selected.remove(idx)
        else:
            selected.add(idx)
        context.user_data["selected"] = selected
        await show_other_menu(update, context, edit=True)
        return SELECT_OTHER


async def enter_total_turnover(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        turnover = parse_number(update.message.text)
        if turnover <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "❌ Введите корректное число, например: `2500000`",
            reply_markup=InlineKeyboardMarkup([manager_btn()]),
            parse_mode="Markdown"
        )
        return ENTER_TOTAL_TURNOVER

    context.user_data["total_turnover"] = turnover
    context.user_data["tier_idx"] = get_tier_index(turnover)
    context.user_data["budgets"] = {}
    context.user_data["selected_list"] = sorted(context.user_data["selected"])
    context.user_data["budget_step"] = 0

    await ask_next_budget(update, context)
    return ENTER_BUDGET


async def ask_next_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data["budget_step"]
    selected_list = context.user_data["selected_list"]
    platform_idx = selected_list[step]
    platform_name = PLATFORMS[platform_idx]
    total = len(selected_list)

    if platform_idx == ECOM_IDX:
        text = (
            f"💰 Площадка *{step + 1} из {total}*\n\n"
            f"Введите бюджет для *{platform_name}* в рублях.\n"
            f"_Этот % будет добавлен к Поиску и/или РСЯ_\n\nПример: `300000`"
        )
    else:
        text = (
            f"💰 Площадка *{step + 1} из {total}*\n\n"
            f"Введите бюджет для *{platform_name}* в рублях:\n\nПример: `500000`"
        )
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([manager_btn()]),
        parse_mode="Markdown"
    )


async def enter_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        budget = parse_number(update.message.text)
        if budget < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "❌ Введите корректное число, например: `500000`",
            reply_markup=InlineKeyboardMarkup([manager_btn()]),
            parse_mode="Markdown"
        )
        return ENTER_BUDGET

    step = context.user_data["budget_step"]
    selected_list = context.user_data["selected_list"]
    context.user_data["budgets"][selected_list[step]] = budget

    step += 1
    context.user_data["budget_step"] = step

    if step < len(selected_list):
        await ask_next_budget(update, context)
        return ENTER_BUDGET

    await update.message.reply_text(
        "👥 Сколько у вас активных клиентов?\n\nПример: `25`",
        reply_markup=InlineKeyboardMarkup([manager_btn()]),
        parse_mode="Markdown"
    )
    return ENTER_CLIENTS


async def enter_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text:
        await update.message.reply_text(
            "❌ Введите количество, например: `25`",
            reply_markup=InlineKeyboardMarkup([manager_btn()]),
            parse_mode="Markdown"
        )
        return ENTER_CLIENTS

    context.user_data["clients"] = text
    await update.message.reply_text(
        "🔗 Есть ли у вас медийный вес?\n\n"
        "Это ваша аудитория в интернете: Telegram-канал, сайт, соцсети и т.д.\n"
        "Укажите ссылку или нажмите *Нет*",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Нет", callback_data="media_no")],
            manager_btn()
        ]),
        parse_mode="Markdown"
    )
    return ENTER_MEDIA


async def media_no_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["media"] = "нет"
    await finish_calc(query.message, context, update.effective_user, edit=False)
    return SHOW_RESULT


async def enter_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["media"] = update.message.text.strip()
    await finish_calc(update.message, context, update.effective_user, edit=False)
    return SHOW_RESULT


async def finish_calc(message, context: ContextTypes.DEFAULT_TYPE, user, edit=False):
    user_info = log_user(user)
    logger.info(f"Расчёт завершён: {user_info}")

    # Отменяем напоминания — человек дошёл до конца
    cancel_reminders(context, message.chat_id)

    user_msg, manager_msg = build_summary(context, user)

    # Статистика
    stats["completed"] += 1
    tier_idx = context.user_data.get("tier_idx", 0)
    grade = tier_label(tier_idx)
    stats["grades"][grade] += 1

    # БД
    username_display = f"@{user.username}" if user.username else f"id:{user.id}"
    selected_list = context.user_data.get("selected_list", [])
    platforms_str = ", ".join(PLATFORMS[i] for i in selected_list)
    turnover = context.user_data.get("total_turnover", 0)
    db_track_calc(user.id, username_display, grade, turnover, 0, platforms_str)

    await notify_manager(
        context.application,
        f"✅ Расчёт завершён: {user_info} — {datetime.now().strftime('%d.%m %H:%M')}\n\n{user_msg}"
    )
    tg_link = f"tg://resolve?domain={MANAGER_USERNAME}&text={quote(manager_msg)}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📨 Отправить менеджеру", url=tg_link)],
        [InlineKeyboardButton("📚 Узнать больше о программе", callback_data="go_kb_result")],
        [InlineKeyboardButton("🔁 Пересчитать", callback_data="restart")]
    ])
    await message.reply_text(user_msg, parse_mode="Markdown", reply_markup=keyboard)


async def result_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "reminder_continue":
        context.user_data.clear()
        context.user_data["selected"] = set()
        cancel_reminders(context, update.effective_chat.id)
        await start(update, context)
        return WELCOME

    if query.data == "go_kb_result":
        await show_kb_main(update, context, edit=False)
        return KNOWLEDGE_BASE

    if query.data == "restart":
        context.user_data.clear()
        context.user_data["selected"] = set()
        await start(update, context)
        return WELCOME


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Расчёт отменён. Напишите /start чтобы начать заново.")
    return ConversationHandler.END


# ─── Запуск ──────────────────────────────────────────────────────────────────

def main():
    db_init()
    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WELCOME: [CallbackQueryHandler(welcome_callback)],
            SELECT_YANDEX: [CallbackQueryHandler(yandex_callback)],
            SELECT_OTHER: [CallbackQueryHandler(other_callback)],
            ENTER_TOTAL_TURNOVER: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_total_turnover)],
            ENTER_BUDGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_budget)],
            ENTER_CLIENTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_clients)],
            ENTER_MEDIA: [
                CallbackQueryHandler(media_no_callback, pattern="^media_no$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_media)
            ],
            SHOW_RESULT: [CallbackQueryHandler(result_callback)],
            KNOWLEDGE_BASE: [CallbackQueryHandler(kb_callback)],
        },
        fallbacks=[CommandHandler("cancel", cancel), CallbackQueryHandler(result_callback, pattern="^reminder_continue$")],
        per_message=False,
    )

    app.add_handler(conv)

    # Ежедневный отчёт в 20:00
    app.job_queue.run_daily(
        daily_report_job,
        time=time(hour=20, minute=0, second=0),
        name="daily_report"
    )

    logger.info("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()
