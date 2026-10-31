"""Telegram-бот: электронная очередь на сдачу лабораторных.

Админ (/admin — первый, кто напишет, становится админом) добавляет повторяющиеся
очереди командой /add: дата и время первого появления, период повтора
и сколько очередь держится открытой. Дальше бот сам открывает очередь
каждый период и закрывает по окончании.

Очередь двигается сама: первый в очереди после сдачи жмёт «✅ Сдал»,
и бот зовёт следующего.
"""
import asyncio
import html
import json
import logging
import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (BotCommand, BotCommandScopeChat, BotCommandScopeDefault,
                           CallbackQuery, InlineKeyboardMarkup, KeyboardButton, Message,
                           ReplyKeyboardMarkup)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

import kstu

load_dotenv()

TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TOKEN") or "").strip()
if not TOKEN:
    raise SystemExit("Не задан токен бота: переменная окружения BOT_TOKEN")
# прокси для доступа к Telegram, если он заблокирован на сервере:
# socks5://user:pass@host:port или http://host:port
TG_PROXY = os.getenv("TG_PROXY", "").strip()
ENV_ADMINS = {
    int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x
}
DB_PATH = os.getenv("DB_PATH", "queue.db")
SCHEDULE_PATH = os.getenv("SCHEDULE_PATH", "schedule.json")
TZ = ZoneInfo(os.getenv("TZ_NAME", "Europe/Moscow"))
OPEN_BEFORE_HOURS = float(os.getenv("OPEN_BEFORE_HOURS", "24"))
CHECK_INTERVAL_SEC = 30

# расписание с kstu.ru (пусто в KSTU_GROUP — выключено)
KSTU_GROUP = os.getenv("KSTU_GROUP", "49396").strip()
KSTU_FACULTY = os.getenv("KSTU_FACULTY", "320").strip()
KSTU_KINDS = {k.strip().upper() for k in os.getenv("KSTU_KINDS", "ЛР").split(",") if k.strip()}
KSTU_OPEN_BEFORE_HOURS = float(os.getenv("KSTU_OPEN_BEFORE_HOURS", "0"))  # 0 = в начале пары
KSTU_REFRESH_HOURS = float(os.getenv("KSTU_REFRESH_HOURS", "3"))
KSTU_CACHE = os.getenv("KSTU_CACHE", "kstu_cache.json")

WEEKDAYS = {"пн": 0, "вт": 1, "ср": 2, "чт": 3, "пт": 4, "сб": 5, "вс": 6}
WEEKDAY_NAMES = list(WEEKDAYS)

PERIODS = {7: "каждую неделю", 14: "раз в 2 недели", 1: "каждый день", 0: "один раз"}
DURATIONS = {2: "2 часа", 4: "4 часа", 24: "сутки", -1: "до следующего раза"}

router = Router()
log = logging.getLogger("queue-bot")
admins: set[int] = set(ENV_ADMINS)
owners: set[int] = set(ENV_ADMINS)  # главные админы: могут назначать других

# кнопки постоянной клавиатуры
BTN_QUEUES = "📋 Очереди"
BTN_MINE = "🎫 Мои записи"
BTN_PROFILE = "👤 Профиль"
BTN_HELP = "ℹ️ Помощь"
BTN_LABS = "📅 Расписание"
BTN_ADD = "➕ Новая очередь"
BTN_RULES = "🔁 Повторы"
BTN_ADMINS = "👑 Админы"
MENU_BUTTONS = {BTN_QUEUES, BTN_MINE, BTN_LABS, BTN_PROFILE, BTN_HELP, BTN_ADD, BTN_RULES,
                BTN_ADMINS}

STUDENT_COMMANDS = [
    BotCommand(command="start", description="Открыть очереди"),
    BotCommand(command="my", description="Где я в очереди"),
    BotCommand(command="schedule", description="Расписание пар"),
    BotCommand(command="profile", description="Изменить имя и группу"),
    BotCommand(command="help", description="Как пользоваться"),
]
ADMIN_COMMANDS = STUDENT_COMMANDS + [
    BotCommand(command="add", description="Новая повторяющаяся очередь"),
    BotCommand(command="rules", description="Повторяющиеся очереди"),
]
OWNER_COMMANDS = ADMIN_COMMANDS + [
    BotCommand(command="admins", description="Управление админами"),
]


def reply_kb(uid: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_QUEUES), KeyboardButton(text=BTN_MINE)],
        [KeyboardButton(text=BTN_LABS), KeyboardButton(text=BTN_PROFILE),
         KeyboardButton(text=BTN_HELP)],
    ]
    if uid in admins:
        admin_row = [KeyboardButton(text=BTN_ADD), KeyboardButton(text=BTN_RULES)]
        if uid in owners:
            admin_row.append(KeyboardButton(text=BTN_ADMINS))
        rows.append(admin_row)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True,
                               input_field_placeholder="Выбери действие ниже")


# ---------- база данных ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    tg_id     INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL,
    grp       TEXT NOT NULL,
    username  TEXT
);
CREATE TABLE IF NOT EXISTS queues (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    title     TEXT NOT NULL,
    owner_id  INTEGER NOT NULL,
    is_open   INTEGER NOT NULL DEFAULT 1,
    rule_key  TEXT,
    starts_at TEXT,
    ends_at   TEXT,
    info      TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS queues_rule_key ON queues(rule_key);
CREATE TABLE IF NOT EXISTS entries (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL,
    user_id  INTEGER NOT NULL,
    lab      TEXT,
    retake   INTEGER NOT NULL DEFAULT 0,
    UNIQUE (queue_id, user_id)
);
CREATE TABLE IF NOT EXISTS rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    first_at    TEXT NOT NULL,
    period_days INTEGER NOT NULL,
    duration_h  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS admins (
    tg_id INTEGER PRIMARY KEY,
    role  TEXT NOT NULL DEFAULT 'admin'
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # миграция со старой версии бота
        async with db.execute("PRAGMA table_info(queues)") as cur:
            cols = {row[1] for row in await cur.fetchall()}
        if cols:
            for col in ("rule_key", "starts_at", "ends_at", "info"):
                if col not in cols:
                    await db.execute(f"ALTER TABLE queues ADD COLUMN {col} TEXT")
        async with db.execute("PRAGMA table_info(entries)") as cur:
            ecols = {row[1] for row in await cur.fetchall()}
        if ecols and "retake" not in ecols:
            await db.execute("ALTER TABLE entries ADD COLUMN retake INTEGER NOT NULL DEFAULT 0")
        async with db.execute("PRAGMA table_info(users)") as cur:
            ucols = {row[1] for row in await cur.fetchall()}
        if ucols and "username" not in ucols:
            await db.execute("ALTER TABLE users ADD COLUMN username TEXT")
        async with db.execute("PRAGMA table_info(admins)") as cur:
            acols = {row[1] for row in await cur.fetchall()}
        if acols and "role" not in acols:
            # в старой версии админ был один — он и становится главным
            await db.execute("ALTER TABLE admins ADD COLUMN role TEXT NOT NULL DEFAULT 'owner'")
        await db.executescript(SCHEMA)
        await db.commit()
        async with db.execute("SELECT tg_id, role FROM admins") as cur:
            for uid, role in await cur.fetchall():
                admins.add(uid)
                if role == "owner":
                    owners.add(uid)


async def execute(sql, *args):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, args)
        await db.commit()
        return cur.lastrowid


async def fetchone(sql, *args):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, args) as cur:
            return await cur.fetchone()


async def fetchall(sql, *args):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, args) as cur:
            return await cur.fetchall()


async def queue_members(qid):
    return await fetchall(
        """SELECT e.user_id, u.full_name, u.grp, e.lab
           FROM entries e JOIN users u ON u.tg_id = e.user_id
           WHERE e.queue_id = ? ORDER BY e.id""",
        qid,
    )


# ---------- утилиты ----------

def is_admin(uid: int) -> bool:
    return uid in admins


def is_owner(uid: int) -> bool:
    return uid in owners


def esc(s) -> str:
    return html.escape(str(s or ""))


def now() -> datetime:
    return datetime.now(TZ)


def fmt_dt(dt: datetime) -> str:
    return f"{WEEKDAY_NAMES[dt.weekday()]} {dt:%d.%m %H:%M}"


def member_line(i, name, grp, lab) -> str:
    line = f"{i}. {esc(name)} ({esc(grp)})"
    if lab and lab != "-":
        line += f" — лаба {esc(lab)}"
    return line


async def notify(bot: Bot, uid: int, text: str):
    try:
        await bot.send_message(uid, text)
    except (TelegramForbiddenError, TelegramBadRequest):
        pass  # пользователь заблокировал бота или чат недоступен


async def safe_edit(c: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass  # «message is not modified»


async def remove_from_queue(bot: Bot, qid: int, uid: int) -> bool:
    """Убирает человека из очереди и уведомляет тех, чья позиция стала 1 или 2."""
    members = await queue_members(qid)
    ids = [m[0] for m in members]
    if uid not in ids:
        return False
    pos = ids.index(uid)
    await execute("DELETE FROM entries WHERE queue_id = ? AND user_id = ?", qid, uid)

    q = await fetchone("SELECT title FROM queues WHERE id = ?", qid)
    title = esc(q[0]) if q else ""
    rest = ids[:pos] + ids[pos + 1:]
    if pos == 0 and rest:
        await notify(bot, rest[0], f"🔔 <b>Твоя очередь!</b> Подходи сдавать: {title}\n"
                                   "Когда сдашь — нажми «✅ Сдал» в боте.")
    if pos <= 1 and len(rest) > 1:
        await notify(bot, rest[1], f"⏳ Ты следующий в очереди «{title}» — готовься.")
    return True


async def send_to_end(bot: Bot, qid: int, uid: int) -> int | None:
    """Отправка на пересдачу: человек уходит в конец очереди. Возвращает новое место."""
    row = await fetchone("SELECT lab FROM entries WHERE queue_id = ? AND user_id = ?", qid, uid)
    if not row:
        return None
    await remove_from_queue(bot, qid, uid)  # следующий получит «Твоя очередь!»
    await execute("INSERT INTO entries (queue_id, user_id, lab, retake) VALUES (?, ?, ?, 1)",
                  qid, uid, row[0])
    return len(await queue_members(qid))


class Reg(StatesGroup):
    name = State()
    group = State()


class Join(StatesGroup):
    lab = State()


class AddAdmin(StatesGroup):
    who = State()


class AddRule(StatesGroup):
    title = State()
    when = State()


# ---------- расписание: правила из /add и из schedule.json ----------

def rule_occurrence(first: datetime, period_days: int, duration_h: int, current: datetime):
    """Текущее появление правила: (open_at, ends_at|None) или None, если сейчас очереди нет."""
    if current < first:
        return None
    if period_days:
        k = (current - first) // timedelta(days=period_days)
        open_at = first + timedelta(days=period_days * k)
    else:
        open_at = first
    if duration_h > 0:
        ends = open_at + timedelta(hours=duration_h)
    elif period_days:
        ends = open_at + timedelta(days=period_days)  # «до следующего раза»
    else:
        ends = None  # разовая и «пока не удалю»
    if ends and current >= ends:
        return None
    return open_at, ends


def next_open(first: datetime, period_days: int, current: datetime):
    if current < first:
        return first
    if not period_days:
        return None
    k = (current - first) // timedelta(days=period_days) + 1
    return first + timedelta(days=period_days * k)


def load_schedule() -> list[dict]:
    """schedule.json — необязательный файл с парами по дням недели."""
    try:
        with open(SCHEDULE_PATH, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return []
    except (OSError, json.JSONDecodeError) as e:
        log.error("Не удалось прочитать %s: %s", SCHEDULE_PATH, e)
        return []

    rules = []
    for i, r in enumerate(data):
        try:
            wd = r["weekday"]
            wd = WEEKDAYS[wd.strip().lower()[:2]] if isinstance(wd, str) else int(wd) - 1
            hh, mm = map(int, r["time"].split(":"))
            rules.append({
                "id": str(r.get("id") or i),
                "title": r["title"],
                "weekday": wd,
                "time": time(hh, mm),
                "duration": int(r.get("duration_min", 90)),
                "weeks": r.get("weeks", "all"),
                "open_before": float(r.get("open_before_hours", OPEN_BEFORE_HOURS)),
                "groups": [g.strip().lower() for g in r.get("groups", [])],
            })
        except (KeyError, ValueError, TypeError) as e:
            log.error("Ошибка в правиле №%d расписания: %s", i + 1, e)
    return rules


def week_matches(d: date, weeks: str) -> bool:
    if weeks == "all":
        return True
    odd = d.isocalendar().week % 2 == 1
    return odd if weeks == "odd" else not odd


async def create_queue(bot: Bot, key: str, title: str, starts: datetime,
                       ends: datetime | None, groups: list[str], info: str = ""):
    if await fetchone("SELECT 1 FROM queues WHERE rule_key = ?", key):
        return
    await execute(
        "INSERT OR IGNORE INTO queues (title, owner_id, rule_key, starts_at, ends_at, info) "
        "VALUES (?, 0, ?, ?, ?, ?)",
        title, key, starts.isoformat(), ends.isoformat() if ends else None, info or None,
    )
    log.info("Создана очередь: %s", title)
    await announce(bot, title, groups, info)


async def sync_schedule(bot: Bot):
    current = now()

    # 1. закрыть очереди, срок которых вышел
    for qid, title, ends_at in await fetchall(
        "SELECT id, title, ends_at FROM queues WHERE ends_at IS NOT NULL"
    ):
        if datetime.fromisoformat(ends_at) > current:
            continue
        members = await queue_members(qid)
        await execute("DELETE FROM entries WHERE queue_id = ?", qid)
        await execute("DELETE FROM queues WHERE id = ?", qid)
        for uid, *_ in members:
            await notify(bot, uid, f"⌛ Очередь «{esc(title)}» закрыта. "
                                   "Не успел — записывайся в следующий раз.")
        log.info("Закрыта очередь: %s", title)

    # 2. правила, добавленные админом через /add
    for rid, title, first_at, period, dur in await fetchall(
        "SELECT id, title, first_at, period_days, duration_h FROM rules"
    ):
        occ = rule_occurrence(datetime.fromisoformat(first_at), period, dur, current)
        if occ:
            open_at, ends = occ
            await create_queue(bot, f"r{rid}@{open_at.isoformat()}",
                               f"{title} · {open_at:%d.%m}", open_at, ends, [])

    # 3. пары из schedule.json
    for rule in load_schedule():
        for delta in range(int(rule["open_before"] // 24) + 2):
            d = current.date() + timedelta(days=delta)
            if d.weekday() != rule["weekday"] or not week_matches(d, rule["weeks"]):
                continue
            starts = datetime.combine(d, rule["time"], TZ)
            ends = starts + timedelta(minutes=rule["duration"])
            if starts - timedelta(hours=rule["open_before"]) <= current < ends:
                await create_queue(bot, f"{rule['id']}@{d.isoformat()}",
                                   f"{rule['title']} · {fmt_dt(starts)}", starts, ends,
                                   rule["groups"])

    # 4. лабы из расписания kstu.ru
    for s in kstu_state["sessions"]:
        starts = datetime.combine(s.day, s.start, TZ)
        ends = datetime.combine(s.day, s.end, TZ)
        if starts - timedelta(hours=KSTU_OPEN_BEFORE_HOURS) <= current < ends:
            await create_queue(bot, s.key, kstu_title(s), starts, ends, [], s.info())

    # 5. лабы, которые пропали из расписания (перенесли/отменили) — убрать их очереди
    if kstu_state["fresh_weeks"]:
        keys = {s.key for s in kstu_state["sessions"]}
        for qid, title, key, starts_at in await fetchall(
            "SELECT id, title, rule_key, starts_at FROM queues WHERE rule_key LIKE 'kstu:%'"
        ):
            st = datetime.fromisoformat(starts_at)
            monday = st.date() - timedelta(days=st.weekday())
            if key in keys or monday not in kstu_state["fresh_weeks"] or st <= current:
                continue
            members = await queue_members(qid)
            await execute("DELETE FROM entries WHERE queue_id = ?", qid)
            await execute("DELETE FROM queues WHERE id = ?", qid)
            for uid, *_ in members:
                await notify(bot, uid, f"❌ «{esc(title)}» пропало из расписания на сайте — "
                                       "очередь убрана. Проверь расписание.")
            log.info("Очередь убрана (нет в расписании): %s", title)


# ---------- расписание с kstu.ru ----------

kstu_state: dict = {"lessons": [], "sessions": [], "fresh_weeks": set(), "updated": None}


def kstu_title(s) -> str:
    starts = datetime.combine(s.day, s.start, TZ)
    return f"{s.subject} ({s.kind}) · {fmt_dt(starts)}"


def kstu_apply(lessons: list, updated: str | None):
    kstu_state["lessons"] = lessons
    kstu_state["sessions"] = kstu.group_sessions(lessons, KSTU_KINDS)
    kstu_state["updated"] = updated


def kstu_load_cache():
    try:
        with open(KSTU_CACHE, encoding="utf-8") as f:
            data = json.load(f)
        lessons = [kstu.Lesson(
            day=date.fromisoformat(l["day"]), start=time.fromisoformat(l["start"]),
            end=time.fromisoformat(l["end"]), room=l["room"], subject=l["subject"],
            kind=l["kind"], teacher=l["teacher"], note=l["note"]) for l in data["lessons"]]
        kstu_apply(lessons, data.get("updated"))
        log.info("Расписание из кэша: %d занятий", len(lessons))
    except FileNotFoundError:
        pass
    except Exception:
        log.exception("Не удалось прочитать кэш расписания")


def kstu_save_cache():
    data = {"updated": kstu_state["updated"], "lessons": [
        {"day": l.day.isoformat(), "start": l.start.isoformat(), "end": l.end.isoformat(),
         "room": l.room, "subject": l.subject, "kind": l.kind, "teacher": l.teacher,
         "note": l.note} for l in kstu_state["lessons"]]}
    with open(KSTU_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)


async def kstu_refresh() -> bool:
    """Скачивает текущую и следующую неделю. При ошибке оставляет старые данные."""
    today = now().date()
    weeks = [today - timedelta(days=today.weekday()) + timedelta(days=7 * i) for i in range(2)]
    fetched: dict = {}
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 queue-bot"}) as http:
            for monday in weeks:
                lessons = await kstu.fetch_week(http, KSTU_FACULTY, KSTU_GROUP, monday)
                if lessons is not None:
                    fetched[monday] = lessons
    except Exception as e:
        log.warning("Не удалось скачать расписание: %s", e)
    if not fetched:
        kstu_state["fresh_weeks"] = set()
        return False

    # старые данные по неделям, которые не скачались, сохраняем; прошедшие дни выкидываем
    keep = [l for l in kstu_state["lessons"]
            if l.day >= weeks[0] and l.day - timedelta(days=l.day.weekday()) not in fetched]
    lessons = keep + [l for ls in fetched.values() for l in ls if l.day >= weeks[0]]
    kstu_apply(lessons, now().isoformat(timespec="minutes"))
    kstu_state["fresh_weeks"] = set(fetched)
    kstu_save_cache()
    log.info("Расписание обновлено: %d занятий, лаб: %d", len(lessons), len(kstu_state["sessions"]))
    return True


async def kstu_loop():
    kstu_load_cache()
    while True:
        await kstu_refresh()
        await asyncio.sleep(KSTU_REFRESH_HOURS * 3600)


async def announce(bot: Bot, title: str, groups: list[str], info: str = ""):
    extra = f"\n<i>{esc(info)}</i>" if info else ""
    for uid, grp in await fetchall("SELECT tg_id, grp FROM users"):
        if groups and grp.strip().lower() not in groups:
            continue
        await notify(bot, uid, f"📋 Открыта очередь: <b>{esc(title)}</b>{extra}\n"
                               f"Записаться — кнопка {BTN_QUEUES}")
        await asyncio.sleep(0.05)  # не упираться в лимиты Telegram


async def scheduler(bot: Bot):
    while True:
        try:
            await sync_schedule(bot)
        except Exception:
            log.exception("Ошибка при обработке расписания")
        await asyncio.sleep(CHECK_INTERVAL_SEC)


# ---------- главное меню и регистрация ----------

async def main_menu(uid: int):
    admin = is_admin(uid)
    where = "" if admin else "WHERE is_open = 1"
    rows = await fetchall(
        "SELECT id, title, is_open, (SELECT COUNT(*) FROM entries WHERE queue_id = q.id) "
        f"FROM queues q {where} ORDER BY COALESCE(starts_at, '9999'), id"
    )

    kb = InlineKeyboardBuilder()
    for qid, title, is_open, cnt in rows:
        lock = "" if is_open else "🔒 "
        kb.button(text=f"{lock}{title} · {cnt} чел.", callback_data=f"q:{qid}")
    kb.button(text="🔄 Обновить", callback_data="menu")
    kb.adjust(1)

    text = ("<b>Открытые очереди</b>\nНажми на очередь, чтобы записаться." if rows
            else "Сейчас нет открытых очередей. Когда появится — пришлю уведомление.")
    return text, kb.as_markup()


HELP_TEXT = (
    "<b>Как пользоваться</b>\n\n"
    f"{BTN_QUEUES} — открытые очереди. Нажми на очередь → «✋ Встать в очередь».\n"
    f"{BTN_MINE} — в каких очередях ты стоишь и на каком месте.\n"
    f"{BTN_LABS} — все пары группы с сайта КНИТУ. Очередь на лабу (🧪) "
    "открывается сама в начале пары.\n"
    "Когда подойдёт твоя очередь, бот пришлёт уведомление. "
    "Сдал — нажми «✅ Сдал», и бот позовёт следующего. "
    "Не сдал — «🔁 Отправлен на пересдачу»: встанешь в конец очереди.\n"
    f"{BTN_PROFILE} — поменять имя или группу."
)
ADMIN_HELP = (
    "\n\n<b>Для админа</b>\n"
    f"{BTN_ADD} — очередь, которая будет появляться сама (каждую неделю, день и т.д.).\n"
    f"{BTN_RULES} — список таких очередей и удаление.\n"
    f"{BTN_ADMINS} — назначить или убрать админов (только у главного).\n"
    "Внутри очереди есть кнопки [админ]: вызвать следующего, закрыть запись, удалить."
)


async def is_registered(uid: int) -> bool:
    return bool(await fetchone("SELECT 1 FROM users WHERE tg_id = ?", uid))


async def ask_registration(m: Message, state: FSMContext, greeting: bool = True):
    await state.set_state(Reg.name)
    text = WELCOME_TEXT if greeting else ""
    await m.answer(text + "✍️ Напиши свои <b>фамилию и имя</b>:")


WELCOME_TEXT = (
    "👋 <b>ХАЙ!</b> Это бот-помощник для студентов.\n\n"
    "Здесь можно:\n"
    "📅 узнать актуальное расписание\n"
    "🧪 занять очередь на сдачу лабы\n\n"
    "👨‍💻 Создатели: @SA16kzn и @tnegilletnii\n"
    "🐞 Нашли баг или есть идея? Пишите нам ✅\n\n"
)


async def send_queues(m: Message, state: FSMContext):
    await state.clear()
    if not await is_registered(m.from_user.id):
        await ask_registration(m, state)
        return
    text, kb = await main_menu(m.from_user.id)
    await m.answer(text, reply_markup=kb)


@router.message.outer_middleware()
async def remember_username(handler, event: Message, data):
    u = event.from_user
    if u and u.username:
        try:
            await execute("UPDATE users SET username = ? WHERE tg_id = ? "
                          "AND COALESCE(username, '') != ?", u.username, u.id, u.username)
        except Exception:
            pass
    return await handler(event, data)


@router.message(F.text.in_(MENU_BUTTONS))
async def menu_button(m: Message, state: FSMContext):
    """Кнопки постоянной клавиатуры работают из любого состояния (прерывают ввод)."""
    await state.clear()
    t = m.text
    if t == BTN_QUEUES:
        await send_queues(m, state)
    elif not await is_registered(m.from_user.id):
        await ask_registration(m, state)
    elif t == BTN_MINE:
        await cmd_my(m, state)
    elif t == BTN_PROFILE:
        await cmd_profile(m, state)
    elif t == BTN_LABS:
        await cmd_schedule(m, state)
    elif t == BTN_HELP:
        await cmd_help(m, state)
    elif t == BTN_ADD and is_admin(m.from_user.id):
        await cmd_add(m, state)
    elif t == BTN_ADMINS and is_owner(m.from_user.id):
        await cmd_admins(m, state)
    elif t == BTN_RULES and is_admin(m.from_user.id):
        await cmd_rules(m)


@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    if not await is_registered(m.from_user.id):
        await ask_registration(m, state)
        return
    await m.answer("Меню внизу экрана 👇", reply_markup=reply_kb(m.from_user.id))
    await send_queues(m, state)


@router.message(Command("help"))
async def cmd_help(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(HELP_TEXT + (ADMIN_HELP if is_admin(m.from_user.id) else ""),
                   reply_markup=reply_kb(m.from_user.id))


@router.message(Command("my"))
async def cmd_my(m: Message, state: FSMContext):
    await state.clear()
    rows = await fetchall(
        """SELECT q.id, q.title,
                  (SELECT COUNT(*) FROM entries e2 WHERE e2.queue_id = q.id AND e2.id <= e.id)
           FROM entries e JOIN queues q ON q.id = e.queue_id
           WHERE e.user_id = ? ORDER BY q.id""",
        m.from_user.id,
    )
    if not rows:
        await m.answer(f"Ты пока ни в одной очереди не стоишь. Открой {BTN_QUEUES}")
        return
    kb = InlineKeyboardBuilder()
    lines = ["<b>Мои записи</b>", ""]
    for qid, title, pos in rows:
        mark = "🔔 твоя очередь!" if pos == 1 else "⏳ следующий" if pos == 2 else f"место {pos}"
        lines.append(f"• {esc(title)} — {mark}")
        kb.button(text=f"{title} · {pos}", callback_data=f"q:{qid}")
    kb.adjust(1)
    await m.answer("\n".join(lines), reply_markup=kb.as_markup())


SCHEDULE_MODES = {"today": "Сегодня", "tomorrow": "Завтра", "week0": "Эта неделя",
                  "week1": "След. неделя"}


async def schedule_view(uid: int, mode: str = "today"):
    if not KSTU_GROUP:
        return "Загрузка расписания с сайта выключена.", None
    today = now().date()
    monday = today - timedelta(days=today.weekday())
    if mode == "tomorrow":
        days = [today + timedelta(days=1)]
    elif mode in ("week0", "week1"):
        start = monday + timedelta(days=7 if mode == "week1" else 0)
        days = [start + timedelta(days=i) for i in range(7)]
    else:
        mode, days = "today", [today]

    open_q = dict(await fetchall("SELECT rule_key, id FROM queues WHERE rule_key LIKE 'kstu:%'"))
    kb = InlineKeyboardBuilder()
    lines = [f"<b>📅 Расписание · {SCHEDULE_MODES[mode]}</b>"]
    any_lessons = False
    for d in days:
        lessons = sorted((l for l in kstu_state["lessons"] if l.day == d),
                         key=lambda l: (l.start, l.note))
        if not lessons and len(days) > 1:
            continue
        lines.append("")
        lines.append(f"<b>{WEEKDAY_NAMES[d.weekday()].upper()} {d:%d.%m}</b>"
                     + ("  · сегодня" if d == today and len(days) > 1 else ""))
        if not lessons:
            lines.append("Пар нет 🎉")
        shown_q = set()
        for l in lessons:
            any_lessons = True
            key = f"kstu:{l.day.isoformat()}:{l.kind}:{l.subject}"
            mark = ""
            if l.kind in KSTU_KINDS:
                mark = "🟢 " if key in open_q else "🧪 "
            lines.append(f"{mark}{l.start:%H:%M}–{l.end:%H:%M}  {esc(l.subject)} <b>({esc(l.kind)})</b>")
            details = " · ".join(x for x in (l.room, l.teacher, l.note) if x)
            if details:
                lines.append(f"      <i>{esc(details)}</i>")
            if key in open_q and key not in shown_q:
                shown_q.add(key)
                kb.button(text=f"🟢 Очередь: {l.subject} · {l.day:%d.%m}",
                          callback_data=f"q:{open_q[key]}")
    if len(days) > 1 and not any_lessons:
        lines.append("")
        lines.append("Пар нет 🎉" if kstu_state["lessons"] else "Расписание пока не загружено.")
    lines.append("")
    lines.append("🧪 — лаба: очередь откроется в начале пары, 🟢 — очередь открыта")
    if kstu_state["updated"]:
        upd = datetime.fromisoformat(kstu_state["updated"])
        lines.append(f"<i>Обновлено с сайта {upd:%d.%m %H:%M}</i>")

    nav = InlineKeyboardBuilder()
    for m_key, label in SCHEDULE_MODES.items():
        nav.button(text=("• " + label + " •") if m_key == mode else label,
                   callback_data=f"sch:{m_key}")
    if is_admin(uid):
        nav.button(text="🔄 [админ] Обновить с сайта", callback_data=f"kstu_refresh:{mode}")
    nav.adjust(2, 2, 1)
    kb.adjust(1)
    kb.attach(nav)
    return "\n".join(lines), kb.as_markup()


@router.message(Command("schedule", "labs"))
async def cmd_schedule(m: Message, state: FSMContext):
    await state.clear()
    text, kb = await schedule_view(m.from_user.id)
    await m.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("sch:"))
async def cb_schedule(c: CallbackQuery):
    text, kb = await schedule_view(c.from_user.id, c.data.split(":")[1])
    await safe_edit(c, text, kb)
    await c.answer()


@router.callback_query(F.data.startswith("kstu_refresh"))
async def cb_kstu_refresh(c: CallbackQuery):
    if not await admin_guard(c):
        return
    mode = c.data.split(":")[1] if ":" in c.data else "today"
    ok = await kstu_refresh()
    if ok:
        await sync_schedule(c.bot)
    text, kb = await schedule_view(c.from_user.id, mode)
    await safe_edit(c, text, kb)
    await c.answer("Обновлено ✅" if ok else "Сайт не ответил, показываю сохранённое",
                   show_alert=not ok)


async def profile_view(uid: int):
    u = await fetchone("SELECT full_name, grp FROM users WHERE tg_id = ?", uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Изменить", callback_data="editprofile")
    role = "\nРоль: 👑 админ" if is_admin(uid) else ""
    return f"<b>Профиль</b>\nИмя: {esc(u[0])}\nГруппа: {esc(u[1])}{role}", kb.as_markup()


@router.message(Command("profile"))
async def cmd_profile(m: Message, state: FSMContext):
    await state.clear()
    if not await is_registered(m.from_user.id):
        await ask_registration(m, state)
        return
    text, kb = await profile_view(m.from_user.id)
    await m.answer(text, reply_markup=kb)


@router.callback_query(F.data == "editprofile")
async def cb_editprofile(c: CallbackQuery, state: FSMContext):
    await state.set_state(Reg.name)
    await c.message.answer("Напиши свои фамилию и имя:")
    await c.answer()


@router.message(Command("id"))
async def cmd_id(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")


@router.message(Command("admin"))
async def cmd_admin(m: Message):
    uid = m.from_user.id
    if is_admin(uid):
        await m.answer("Ты уже админ.", reply_markup=reply_kb(uid))
        return
    if admins:
        await m.answer("Админ у бота уже есть.")
        return
    await execute("INSERT OR REPLACE INTO admins (tg_id, role) VALUES (?, 'owner')", uid)
    admins.add(uid)
    owners.add(uid)
    await set_admin_commands(m.bot, uid)
    await m.answer("👑 Теперь ты <b>главный админ</b> этого бота. Внизу появились кнопки "
                   f"{BTN_ADD}, {BTN_RULES} и {BTN_ADMINS}." + ADMIN_HELP,
                   reply_markup=reply_kb(uid))


# ---------- управление админами (только главный админ) ----------

async def admins_view():
    rows = await fetchall(
        """SELECT a.tg_id, a.role, u.full_name, u.grp, u.username
           FROM admins a LEFT JOIN users u ON u.tg_id = a.tg_id
           ORDER BY a.role = 'owner' DESC, u.full_name""")
    lines = ["<b>👑 Админы бота</b>", ""]
    kb = InlineKeyboardBuilder()
    for uid, role, name, grp, uname in rows:
        who = esc(name) if name else f"ID {uid}"
        if grp:
            who += f" ({esc(grp)})"
        if uname:
            who += f" @{esc(uname)}"
        lines.append(("👑 " if role == "owner" else "🛠 ") + who
                     + (" — главный" if role == "owner" else ""))
        if role != "owner":
            kb.button(text=f"❌ Убрать {name or uid}", callback_data=f"adm_del:{uid}")
    lines += ["", "🛠 Админы могут создавать и закрывать очереди, вызывать следующего, "
              "отправлять на пересдачу.", "👑 Назначать админов может только главный."]
    kb.button(text="➕ Добавить админа", callback_data="adm_add")
    kb.adjust(1)
    return "\n".join(lines), kb.as_markup()


@router.message(Command("admins"))
async def cmd_admins(m: Message, state: FSMContext):
    await state.clear()
    if not is_owner(m.from_user.id):
        return
    text, kb = await admins_view()
    await m.answer(text, reply_markup=kb)


@router.callback_query(F.data == "adm_add")
async def cb_adm_add(c: CallbackQuery, state: FSMContext):
    if not is_owner(c.from_user.id):
        await c.answer("Только для главного админа", show_alert=True)
        return
    await state.set_state(AddAdmin.who)
    await c.message.answer(
        "Кого сделать админом? Пришли его <b>@username</b> или <b>Telegram ID</b>.\n\n"
        "Человек должен хотя бы раз написать боту /start. "
        "Свой ID он может узнать командой /id.")
    await c.answer()


async def find_user(text: str):
    t = text.strip()
    if t.lstrip("-").isdigit():
        return await fetchone("SELECT tg_id, full_name FROM users WHERE tg_id = ?", int(t))
    uname = t.lstrip("@").lower()
    return await fetchone("SELECT tg_id, full_name FROM users WHERE lower(username) = ?", uname)


@router.message(AddAdmin.who, F.text, ~F.text.startswith("/"))
async def adm_add_who(m: Message, state: FSMContext):
    if not is_owner(m.from_user.id):
        await state.clear()
        return
    found = await find_user(m.text)
    if not found:
        await m.answer("Не нашёл такого пользователя 🤔 Попроси его написать боту /start "
                       "и пройти регистрацию, потом пришли @username или ID ещё раз.")
        return
    uid, name = found
    await state.clear()
    if uid in admins:
        await m.answer(f"{esc(name)} уже админ.")
    else:
        await execute("INSERT OR REPLACE INTO admins (tg_id, role) VALUES (?, 'admin')", uid)
        admins.add(uid)
        await set_admin_commands(m.bot, uid)
        try:
            await m.bot.send_message(
                uid, "🛠 Тебя назначили <b>админом</b> бота. Внизу появились кнопки "
                     f"{BTN_ADD} и {BTN_RULES}." + ADMIN_HELP, reply_markup=reply_kb(uid))
        except Exception:
            pass
        await m.answer(f"✅ {esc(name)} теперь админ.")
    text, kb = await admins_view()
    await m.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("adm_del:"))
async def cb_adm_del(c: CallbackQuery):
    if not is_owner(c.from_user.id):
        await c.answer("Только для главного админа", show_alert=True)
        return
    uid = int(c.data.split(":")[1])
    if uid in owners:
        await c.answer("Главного админа убрать нельзя", show_alert=True)
        return
    await execute("DELETE FROM admins WHERE tg_id = ?", uid)
    admins.discard(uid)
    try:
        await c.bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=uid))
        await c.bot.send_message(uid, "Ты больше не админ бота.", reply_markup=reply_kb(uid))
    except Exception:
        pass
    text, kb = await admins_view()
    await safe_edit(c, text, kb)
    await c.answer("Админ убран")


@router.message(Reg.name, F.text, ~F.text.in_(MENU_BUTTONS), ~F.text.startswith("/"))
async def reg_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text.strip()[:64])
    await state.set_state(Reg.group)
    await m.answer("Теперь номер группы:")


@router.message(Reg.group, F.text, ~F.text.in_(MENU_BUTTONS), ~F.text.startswith("/"))
async def reg_group(m: Message, state: FSMContext):
    data = await state.get_data()
    await execute(
        "INSERT OR REPLACE INTO users (tg_id, full_name, grp, username) VALUES (?, ?, ?, ?)",
        m.from_user.id, data["name"], m.text.strip()[:32], m.from_user.username,
    )
    await state.clear()
    await m.answer("Готово ✅ Когда откроется новая очередь — пришлю уведомление.\n"
                   "Меню внизу экрана 👇", reply_markup=reply_kb(m.from_user.id))
    text, kb = await main_menu(m.from_user.id)
    await m.answer(text, reply_markup=kb)


@router.message(Reg.name)
@router.message(Reg.group)
async def reg_hint(m: Message):
    await m.answer("Сначала закончим регистрацию — напиши текстом, пожалуйста 🙂")


@router.callback_query(F.data == "menu")
async def cb_menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    text, kb = await main_menu(c.from_user.id)
    await safe_edit(c, text, kb)
    await c.answer()


# ---------- экран очереди ----------

async def queue_view(qid: int, uid: int):
    q = await fetchone("SELECT title, is_open, info FROM queues WHERE id = ?", qid)
    if not q:
        return None, None
    title, is_open, info = q
    members = await queue_members(qid)

    lines = [f"<b>{esc(title)}</b>"]
    if info:
        lines.append(f"<i>{esc(info)}</i>")
    if not is_open:
        lines.append("🔒 Запись закрыта")
    lines.append("")
    my_pos = None
    retakes = {r[0] for r in await fetchall(
        "SELECT user_id FROM entries WHERE queue_id = ? AND retake = 1", qid)}
    for i, (mid, name, grp, lab) in enumerate(members, 1):
        line = member_line(i, name, grp, lab)
        if mid in retakes:
            line += " 🔁"
        if i == 1:
            line += " — сдаёт"
        if mid == uid:
            my_pos = i
            line = f"👉 <b>{line}</b>"
        lines.append(line)
    if not members:
        lines.append("Очередь пуста.")
    lines.append("")
    if my_pos == 1:
        lines.append("🔔 <b>Сейчас твоя очередь</b> — иди сдавать. Сдал — жми «✅ Сдал».")
    elif my_pos == 2:
        lines.append("⏳ Ты следующий — готовься.")
    elif my_pos:
        lines.append(f"Твоё место: <b>{my_pos}</b>")
    else:
        lines.append("Ты не в очереди.")

    kb = InlineKeyboardBuilder()
    if my_pos == 1:
        kb.button(text="✅ Сдал", callback_data=f"done:{qid}")
        if len(members) > 1:
            kb.button(text="🔁 Отправлен на пересдачу", callback_data=f"retake:{qid}")
    if my_pos:
        kb.button(text="🚪 Выйти из очереди", callback_data=f"leave:{qid}")
    elif is_open:
        kb.button(text="✋ Встать в очередь", callback_data=f"join:{qid}")
    if is_admin(uid):
        if members:
            kb.button(text="▶️ [админ] Первый сдал, следующий", callback_data=f"next:{qid}")
        if len(members) > 1:
            kb.button(text="🔁 [админ] Первого — на пересдачу", callback_data=f"aretake:{qid}")
        kb.button(text="🔒 [админ] Закрыть запись" if is_open else "🔓 [админ] Открыть запись",
                  callback_data=f"toggle:{qid}")
        kb.button(text="🗑 [админ] Удалить очередь", callback_data=f"del:{qid}")
    kb.button(text="🔄 Обновить", callback_data=f"q:{qid}")
    kb.button(text="⬅️ Назад", callback_data="menu")
    kb.adjust(1)
    return "\n".join(lines), kb.as_markup()


async def show_queue(c: CallbackQuery, qid: int, toast: str | None = None):
    text, kb = await queue_view(qid, c.from_user.id)
    if text is None:
        await c.answer("Очередь уже закрыта", show_alert=True)
        return
    await safe_edit(c, text, kb)
    await c.answer(toast)


@router.callback_query(F.data.startswith("q:"))
async def cb_queue(c: CallbackQuery):
    await show_queue(c, int(c.data.split(":")[1]))


@router.callback_query(F.data.startswith("join:"))
async def cb_join(c: CallbackQuery, state: FSMContext):
    qid = int(c.data.split(":")[1])
    if not await fetchone("SELECT 1 FROM users WHERE tg_id = ?", c.from_user.id):
        await c.answer("Сначала зарегистрируйся: /start", show_alert=True)
        return
    q = await fetchone("SELECT is_open FROM queues WHERE id = ?", qid)
    if not q or not q[0]:
        await c.answer("Запись в эту очередь закрыта", show_alert=True)
        return
    await state.set_state(Join.lab)
    await state.update_data(qid=qid)
    await c.message.answer("Какую лабу сдаёшь? Напиши номер (или «-», если неважно):")
    await c.answer()


@router.message(Join.lab, F.text, ~F.text.startswith("/"))
async def join_lab(m: Message, state: FSMContext):
    qid = (await state.get_data())["qid"]
    await state.clear()
    q = await fetchone("SELECT is_open FROM queues WHERE id = ?", qid)
    if not q or not q[0]:
        await m.answer("Пока ты писал, запись закрыли 😕")
        return
    await execute(
        "INSERT OR IGNORE INTO entries (queue_id, user_id, lab) VALUES (?, ?, ?)",
        qid, m.from_user.id, m.text.strip()[:32],
    )
    text, kb = await queue_view(qid, m.from_user.id)
    await m.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("retake:"))
async def cb_retake(c: CallbackQuery):
    qid = int(c.data.split(":")[1])
    members = await queue_members(qid)
    if not members or members[0][0] != c.from_user.id:
        await show_queue(c, qid, "Сейчас не твоя очередь")
        return
    pos = await send_to_end(c.bot, qid, c.from_user.id)
    await show_queue(c, qid, f"Ты в конце очереди, место {pos}. Удачи на пересдаче 💪")


@router.callback_query(F.data.startswith("aretake:"))
async def cb_admin_retake(c: CallbackQuery):
    if not await admin_guard(c):
        return
    qid = int(c.data.split(":")[1])
    members = await queue_members(qid)
    if not members:
        await show_queue(c, qid, "Очередь пуста")
        return
    uid, name = members[0][0], members[0][1]
    pos = await send_to_end(c.bot, qid, uid)
    q = await fetchone("SELECT title FROM queues WHERE id = ?", qid)
    await notify(c.bot, uid, f"🔁 Тебя отправили на пересдачу в очереди «{esc(q[0]) if q else ''}». "
                             f"Ты в конце, место {pos}.")
    await show_queue(c, qid, f"{name} — на пересдачу, место {pos}")


@router.callback_query(F.data.startswith(("leave:", "done:")))
async def cb_leave(c: CallbackQuery):
    action, qid = c.data.split(":")
    removed = await remove_from_queue(c.bot, int(qid), c.from_user.id)
    toast = None
    if removed:
        toast = "Отлично, следующий уже вызван 👍" if action == "done" else "Ты вышел из очереди"
    await show_queue(c, int(qid), toast)


# ---------- админ: управление очередью ----------

async def admin_guard(c: CallbackQuery) -> bool:
    if not is_admin(c.from_user.id):
        await c.answer("Только для админа", show_alert=True)
        return False
    return True


@router.callback_query(F.data.startswith("next:"))
async def cb_next(c: CallbackQuery):
    if not await admin_guard(c):
        return
    qid = int(c.data.split(":")[1])
    members = await queue_members(qid)
    if members:
        await remove_from_queue(c.bot, qid, members[0][0])
    await show_queue(c, qid, f"Сдал: {members[0][1]}" if members else "Очередь пуста")


@router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(c: CallbackQuery):
    if not await admin_guard(c):
        return
    qid = int(c.data.split(":")[1])
    await execute("UPDATE queues SET is_open = 1 - is_open WHERE id = ?", qid)
    await show_queue(c, qid)


@router.callback_query(F.data.startswith("del:"))
async def cb_del(c: CallbackQuery):
    if not await admin_guard(c):
        return
    qid = int(c.data.split(":")[1])
    kb = InlineKeyboardBuilder()
    kb.button(text="Да, удалить", callback_data=f"delok:{qid}")
    kb.button(text="Отмена", callback_data=f"q:{qid}")
    await safe_edit(c, "Удалить эту очередь? Записанные получат уведомление.\n"
                       "(Повторяющееся правило останется — удалить его можно в /rules.)",
                    kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("delok:"))
async def cb_delok(c: CallbackQuery):
    if not await admin_guard(c):
        return
    qid = int(c.data.split(":")[1])
    q = await fetchone("SELECT title FROM queues WHERE id = ?", qid)
    members = await queue_members(qid)
    await execute("DELETE FROM entries WHERE queue_id = ?", qid)
    await execute("DELETE FROM queues WHERE id = ?", qid)
    if q:
        for uid, *_ in members:
            await notify(c.bot, uid, f"❌ Очередь «{esc(q[0])}» отменена.")
    text, kb = await main_menu(c.from_user.id)
    await safe_edit(c, text, kb)
    await c.answer("Удалено")


# ---------- админ: повторяющиеся очереди ----------

def parse_when(s: str) -> datetime | None:
    s = " ".join(s.replace(",", " ").split())
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M", "%d.%m %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
        except ValueError:
            continue
        if fmt == "%d.%m %H:%M":
            dt = dt.replace(year=now().year)
        return dt.replace(tzinfo=TZ)
    return None


@router.message(Command("add"))
async def cmd_add(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return
    await state.clear()
    await state.set_state(AddRule.title)
    await m.answer("Название очереди? Например: <i>ОАиП, лабы, ауд. 305</i>")


@router.message(AddRule.title, F.text, ~F.text.startswith("/"))
async def add_title(m: Message, state: FSMContext):
    await state.update_data(title=m.text.strip()[:80])
    await state.set_state(AddRule.when)
    await m.answer(f"Когда очередь появится в первый раз?\n"
                   f"Формат: <code>ДД.ММ ЧЧ:ММ</code>, например <code>{now():%d.%m} 18:00</code>")


@router.message(AddRule.when, F.text, ~F.text.startswith("/"))
async def add_when(m: Message, state: FSMContext):
    when = parse_when(m.text)
    if not when:
        await m.answer("Не понял дату 🤔 Напиши так: <code>03.11 18:00</code>")
        return
    await state.update_data(when=when.isoformat())
    await state.set_state(None)
    kb = InlineKeyboardBuilder()
    for days, label in PERIODS.items():
        kb.button(text=label, callback_data=f"addp:{days}")
    kb.adjust(2)
    await m.answer(f"Первое появление: <b>{fmt_dt(when)}</b>\nКак часто повторять?",
                   reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("addp:"))
async def add_period(c: CallbackQuery, state: FSMContext):
    if not await admin_guard(c):
        return
    days = int(c.data.split(":")[1])
    await state.update_data(period=days)
    kb = InlineKeyboardBuilder()
    for hours, label in DURATIONS.items():
        if hours == -1 and days == 0:
            label = "пока не удалю"
        kb.button(text=label, callback_data=f"addd:{hours}")
    kb.adjust(2)
    await safe_edit(c, f"Повтор: <b>{PERIODS[days]}</b>\nСколько очередь держать открытой?",
                    kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("addd:"))
async def add_duration(c: CallbackQuery, state: FSMContext):
    if not await admin_guard(c):
        return
    data = await state.get_data()
    if not {"title", "when", "period"} <= data.keys():
        await c.answer("Начни заново: /add", show_alert=True)
        return
    hours = int(c.data.split(":")[1])
    await state.clear()
    rid = await execute(
        "INSERT INTO rules (title, first_at, period_days, duration_h) VALUES (?, ?, ?, ?)",
        data["title"], data["when"], data["period"], hours,
    )
    first = datetime.fromisoformat(data["when"])
    nxt = next_open(first, data["period"], now())
    status = ("уже открыта — смотри /start" if rule_occurrence(first, data["period"], hours, now())
              else f"появится {fmt_dt(nxt)}" if nxt else "время уже прошло")
    await safe_edit(c, f"✅ Очередь добавлена (№{rid})\n\n<b>{esc(data['title'])}</b>\n"
                       f"Повтор: {PERIODS[data['period']]}\n"
                       f"Открыта: {DURATIONS[hours] if hours != -1 or data['period'] else 'пока не удалю'}\n"
                       f"Статус: {status}\n\nВсе правила: /rules",
                    InlineKeyboardBuilder().as_markup())
    await c.answer()
    await sync_schedule(c.bot)


async def rules_view():
    rows = await fetchall("SELECT id, title, first_at, period_days, duration_h FROM rules ORDER BY id")
    if not rows:
        return "Повторяющихся очередей нет. Добавить: /add", InlineKeyboardBuilder().as_markup()
    lines = ["<b>Повторяющиеся очереди</b>", ""]
    kb = InlineKeyboardBuilder()
    for rid, title, first_at, period, dur in rows:
        first = datetime.fromisoformat(first_at)
        nxt = next_open(first, period, now())
        when = f"следующая: {fmt_dt(nxt)}" if nxt else "больше не повторится"
        if rule_occurrence(first, period, dur, now()):
            when = "🟢 открыта сейчас, " + when
        lines.append(f"№{rid} <b>{esc(title)}</b>\n   {PERIODS[period]}, {when}")
        kb.button(text=f"🗑 Удалить №{rid}", callback_data=f"rdel:{rid}")
    kb.adjust(2)
    return "\n".join(lines), kb.as_markup()


@router.message(Command("rules"))
async def cmd_rules(m: Message):
    if not is_admin(m.from_user.id):
        return
    text, kb = await rules_view()
    await m.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("rdel:"))
async def cb_rdel(c: CallbackQuery):
    if not await admin_guard(c):
        return
    rid = int(c.data.split(":")[1])
    await execute("DELETE FROM rules WHERE id = ?", rid)
    text, kb = await rules_view()
    await safe_edit(c, text, kb)
    await c.answer(f"Правило №{rid} удалено. Уже открытая очередь осталась — её можно удалить вручную.",
                   show_alert=True)


# ---------- запуск ----------

async def set_admin_commands(bot: Bot, uid: int):
    try:
        await bot.set_my_commands(OWNER_COMMANDS if uid in owners else ADMIN_COMMANDS,
                                  scope=BotCommandScopeChat(chat_id=uid))
    except Exception as e:  # админ ещё не писал боту или нет связи
        log.warning("Не удалось задать команды админа: %s", e)


async def main():
    logging.basicConfig(level=logging.INFO)
    await init_db()
    session = AiohttpSession(proxy=TG_PROXY) if TG_PROXY else None
    if TG_PROXY:
        log.info("Telegram через прокси: %s", TG_PROXY.split("@")[-1])
    bot = Bot(TOKEN, session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    try:
        await bot.set_my_commands(STUDENT_COMMANDS, scope=BotCommandScopeDefault())
        for uid in admins:
            await set_admin_commands(bot, uid)
    except Exception as e:
        log.warning("Не удалось задать меню команд (нет связи с Telegram?): %s", e)
    dp = Dispatcher()
    dp.include_router(router)
    if KSTU_GROUP:
        asyncio.create_task(kstu_loop())
    asyncio.create_task(scheduler(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
