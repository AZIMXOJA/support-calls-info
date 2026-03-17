"""
Support Bot — Отчёт по звонкам
1. Вставь BOT_TOKEN
2. Сделай Google Sheets публичным (Все пользователи → Читатель)
3. pip install aiogram requests
4. python support_bot.py
"""

# ══════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════

BOT_TOKEN      = "8534679316:AAGIYMXcrJsKfXDSRY8caL84B07ATtrDJZQ"
SPREADSHEET_ID = "1puzwfXofW7uNRRSqOaINBh9WK8YGZCOe7S_bwzy5e3A"
SHEET_GID      = "1364228853"

AGENT_NAMES = {
    "316": "Nodir Baxtiyarov",
    "311": "Umida Alimdjanova",
    "312": "Salohiddin Jamoliddinov",
    "317": "Xalilov Qaxramon",
    "318": "Azimjon Ungarov",
    "319": "Bobomurod Xursandov",
    "321": "Mekhrali Zabitov",
    "322": "Ruslan Agabekov",
    "323": "Kozim Ergashev",
    "5001": "Вне раб. времени",
}

# Номер переадресации пропущенных
REDIRECT_NUMBER = "5203"

# Разрешённые пользователи (username без @)
ALLOWED_USERS = {"azim_gws", "Svetlana_Tsoy_Smartup", "bts_lily"}

# URL Mini App (ngrok)
MINI_APP_URL = "https://support-calls-info.onrender.com"

# ══════════════════════════════════════════
#  КОД
# ══════════════════════════════════════════

import asyncio, csv, io, logging, requests
from collections import Counter
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton,
    InlineKeyboardMarkup, Message,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# Индексы колонок
C_UUID=0; C_DIR=1; C_CALLER=2; C_CALLEE=3; C_START=4
C_END=5; C_TOTAL_DUR=6; C_TALK_DUR=7; C_WAIT_DUR=8
C_RESULT=9; C_DEPT=10; C_QUALITY=11; C_CALLBACK=12

ANSWERED = {"NORMAL_CLEARING", "NORMAL_UNSPECIFIED"}


# ── FSM ──────────────────────────────────

class Range(StatesGroup):
    start = State()
    end   = State()


# ── Keyboards ────────────────────────────

def kb_main():
    from aiogram.types import WebAppInfo
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Отчёт по звонкам", callback_data="menu")],
        [InlineKeyboardButton(text="🖥 Открыть дашборд", web_app=WebAppInfo(url=MINI_APP_URL))],
    ])

def kb_period():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Сегодня",       callback_data="p:today"),
            InlineKeyboardButton(text="📅 Вчера",         callback_data="p:yesterday"),
        ],
        [
            InlineKeyboardButton(text="📅 Эта неделя",    callback_data="p:week"),
            InlineKeyboardButton(text="📅 Этот месяц",    callback_data="p:month"),
        ],
        [
            InlineKeyboardButton(text="🗓 Свой диапазон", callback_data="p:custom"),
        ],
        [
            InlineKeyboardButton(text="◀️ Назад",         callback_data="back"),
        ],
    ])

def kb_back():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="◀️ Назад к выбору периода", callback_data="menu")
    ]])


# ── Date helpers ─────────────────────────

def period_dates(key: str):
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end   = today.replace(hour=23, minute=59, second=59)
    if key == "today":
        return today, end, "Сегодня", fmt_date_range(today, end)
    if key == "yesterday":
        d = today - timedelta(days=1)
        e = d.replace(hour=23, minute=59, second=59)
        return d, e, "Вчера", fmt_date_range(d, e)
    if key == "week":
        s = today - timedelta(days=today.weekday())
        return s, end, "Эта неделя", fmt_date_range(s, end)
    if key == "month":
        s = today.replace(day=1)
        return s, end, "Этот месяц", fmt_date_range(s, end)
    return None, None, "", ""

def fmt_date_range(start: datetime, end: datetime) -> str:
    if start.date() == end.date():
        return start.strftime("%d.%m.%Y")
    return f"{start.strftime('%d.%m.%Y')} — {end.strftime('%d.%m.%Y')}"

def parse_dt(val: str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None

def fmt_sec(sec: float) -> str:
    s = int(sec)
    if s < 60:
        return f"{s} сек"
    return f"{s // 60} мин {s % 60} сек"

def agent_name(num: str) -> str:
    n = str(num).split(".")[0].strip()
    return AGENT_NAMES.get(n, f"#{n}")

def safe_float(val) -> float:
    try:
        return float(str(val).replace(",", ".").strip())
    except:
        return 0.0


# ── Google Sheets reader ─────────────────

def fetch_rows() -> list[list]:
    url = (
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        f"/export?format=csv&gid={SHEET_GID}"
    )
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()
    for enc in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            text = resp.content.decode(enc)
            if "UUID" in text or "uuid" in text.lower():
                break
        except:
            continue
    rows = list(csv.reader(io.StringIO(text)))
    return rows[1:]


# ── Report builder ───────────────────────

def progress_bar(value: int, total: int, width: int = 10) -> str:
    if total == 0:
        return "░" * width
    filled = round(value / total * width)
    return "█" * filled + "░" * (width - filled)

def build_report(rows: list[list], start: datetime, end: datetime,
                 label: str, date_range: str) -> str:
    total = inc = out = 0
    missed = missed_ok = missed_fail = 0
    waits, talks = [], []
    quality: Counter = Counter()
    inc_agents: Counter = Counter()
    out_agents: Counter = Counter()

    support_numbers = set(AGENT_NAMES.keys())

    for row in rows:
        if len(row) <= C_DEPT:
            continue
        dt = parse_dt(row[C_START])
        if not dt or not (start <= dt <= end):
            continue

        direction = row[C_DIR].strip()
        dept      = row[C_DEPT].strip()
        caller    = str(row[C_CALLER]).split(".")[0].strip() if len(row) > C_CALLER else ""
        callee    = str(row[C_CALLEE]).split(".")[0].strip() if len(row) > C_CALLEE else ""

        # Входящий на Support — отдел Support, не исходящий
        is_incoming = dept == "Support" and "сход" not in direction
        # Исходящий от агента Support — кто звонил есть в списке агентов
        is_outgoing = "сход" in direction and caller in support_numbers

        if not (is_incoming or is_outgoing):
            continue

        result = row[C_RESULT].strip() if len(row) > C_RESULT else ""
        talk   = safe_float(row[C_TALK_DUR]) if len(row) > C_TALK_DUR else 0
        wait   = safe_float(row[C_WAIT_DUR]) if len(row) > C_WAIT_DUR else 0
        q      = int(safe_float(row[C_QUALITY])) if len(row) > C_QUALITY else 0
        cb_raw = row[C_CALLBACK].strip().upper() if len(row) > C_CALLBACK else ""

        total += 1
        quality[q] += 1

        if is_incoming:
            inc += 1
            # Пропущенный = Кому звонили = 5203 (переадресация на очередь)
            # Отвеченный = Кому звонили = номер агента
            if callee == REDIRECT_NUMBER:
                missed += 1
                if cb_raw == "TRUE":
                    missed_ok += 1
                elif cb_raw == "FALSE":
                    missed_fail += 1
            else:
                if callee in support_numbers:
                    inc_agents[callee] += 1
                if result in ANSWERED and talk > 0:
                    waits.append(wait)
        elif is_outgoing:
            out += 1
            out_agents[caller] += 1

        if talk > 0:
            talks.append(talk)

    avg_wait = sum(waits) / len(waits) if waits else 0
    avg_talk = sum(talks) / len(talks) if talks else 0
    cb_pct   = round(missed_ok / missed * 100) if missed else 0
    rated    = sum(v for k, v in quality.items() if k > 0)
    sep      = "─" * 26

    lines = [
        f"📊 <b>Отчёт — Support</b>",
        f"🗓 <b>{label}</b>  {date_range}",
        sep,
        "",
        "📞 <b>Звонки</b>",
        f"  Всего:         <b>{total}</b>",
        f"  📥 Входящих:   <b>{inc}</b>",
        f"  📤 Исходящих:  <b>{out}</b>",
        "",
        sep,
        "",
        "🚨 <b>Пропущенные</b>",
        f"  Всего:             <b>{missed}</b>",
        f"  ✅ Перезвонили:    <b>{missed_ok}</b>",
        f"  ❌ Не перезвонили: <b>{missed_fail}</b>",
    ]
    if missed:
        lines.append(f"  📈 Обработано:     <b>{cb_pct}%</b>")

    lines += [
        "",
        sep,
        "",
        "⏱ <b>Время</b>",
        f"  Среднее ожидание:  <b>{fmt_sec(avg_wait)}</b>",
        f"  Средний разговор:  <b>{fmt_sec(avg_talk)}</b>",
        "",
        sep,
        "",
    ]

    total_q = rated + quality.get(0, 0)
    lines.append(f"⭐ <b>Оценки</b>  — оценили <b>{rated}</b> из {total_q}")
    score_map = {5: "5★ Отлично", 4: "4★ Хорошо", 3: "3★ Средне", 2: "2★ Плохо", 1: "1★ Ужасно"}
    for score in [5, 4, 3, 2, 1]:
        cnt = quality.get(score, 0)
        if cnt:
            pct = round(cnt / rated * 100) if rated else 0
            lines.append(f"  {score_map[score]}   <b>{cnt}</b>  ({pct}%)")

    lines += ["", sep, "", "👥 <b>Агенты — входящие</b>"]
    top_in = inc_agents.most_common(15)
    if top_in:
        for i, (a, c) in enumerate(top_in, 1):
            lines.append(f"  {i}. {agent_name(a)}  —  <b>{c}</b>")
    else:
        lines.append("  Нет данных")

    lines += ["", "👥 <b>Агенты — исходящие</b>"]
    top_out = out_agents.most_common(15)
    if top_out:
        for i, (a, c) in enumerate(top_out, 1):
            lines.append(f"  {i}. {agent_name(a)}  —  <b>{c}</b>")
    else:
        lines.append("  Нет данных")

    lines += ["", sep, f"<i>🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}</i>"]
    return "\n".join(lines)


def get_report(start: datetime, end: datetime, label: str, date_range: str) -> str:
    rows = fetch_rows()
    return build_report(rows, start, end, label, date_range)


# ── Handlers ─────────────────────────────

def is_allowed(msg) -> bool:
    username = (msg.from_user.username or "").lower()
    return username in {u.lower() for u in ALLOWED_USERS}

def access_denied_text():
    return "⛔ У вас нет доступа к этому боту."

@dp.message(CommandStart())
async def h_start(msg: Message, state: FSMContext):
    if not is_allowed(msg):
        await msg.answer(access_denied_text())
        return
    await state.clear()
    await msg.answer(
        f"👋 Привет, <b>{msg.from_user.first_name}</b>!\n\n"
        "Бот отчётов отдела <b>Support</b>.\nВыбери действие:",
        parse_mode="HTML", reply_markup=kb_main()
    )

@dp.callback_query(F.data == "back")
async def h_back(call: CallbackQuery, state: FSMContext):
    if not is_allowed(call):
        await call.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await call.message.edit_text("Выбери действие:", reply_markup=kb_main())
    await call.answer()

@dp.callback_query(F.data == "menu")
async def h_menu(call: CallbackQuery, state: FSMContext):
    if not is_allowed(call):
        await call.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await call.message.edit_text(
        "📊 <b>Отчёт по звонкам — Support</b>\n\nВыбери период:",
        parse_mode="HTML", reply_markup=kb_period()
    )
    await call.answer()

@dp.callback_query(F.data.startswith("p:") & ~F.data.in_({"p:custom"}))
async def h_period(call: CallbackQuery):
    if not is_allowed(call):
        await call.answer("⛔ Нет доступа", show_alert=True)
        return
    key = call.data[2:]
    start, end, label, date_range = period_dates(key)
    await call.message.edit_text(
        f"⏳ Загружаю данные...\n<i>{label}  {date_range}</i>",
        parse_mode="HTML"
    )
    await call.answer()
    try:
        loop = asyncio.get_event_loop()
        text = await loop.run_in_executor(None, get_report, start, end, label, date_range)
    except Exception as e:
        log.error(e, exc_info=True)
        text = f"❌ Ошибка:\n<code>{e}</code>"
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb_back())

@dp.callback_query(F.data == "p:custom")
async def h_custom(call: CallbackQuery, state: FSMContext):
    if not is_allowed(call):
        await call.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(Range.start)
    await call.message.edit_text(
        "🗓 <b>Свой диапазон</b>\n\n"
        "Введи дату начала:\n<code>ДД.ММ.ГГГГ</code>\n\n"
        "Пример: <code>01.01.2026</code>\n/cancel — отмена",
        parse_mode="HTML"
    )
    await call.answer()

@dp.message(Range.start)
async def h_range_start(msg: Message, state: FSMContext):
    try:
        d = datetime.strptime(msg.text.strip(), "%d.%m.%Y")
        await state.update_data(start=d)
        await state.set_state(Range.end)
        await msg.answer(
            f"✅ Начало: <b>{d.strftime('%d.%m.%Y')}</b>\n\n"
            "Теперь введи дату конца:\n<code>ДД.ММ.ГГГГ</code>",
            parse_mode="HTML"
        )
    except ValueError:
        await msg.answer("❌ Неверный формат. Пример: <code>01.01.2026</code>", parse_mode="HTML")

@dp.message(Range.end)
async def h_range_end(msg: Message, state: FSMContext):
    try:
        end = datetime.strptime(msg.text.strip(), "%d.%m.%Y").replace(hour=23, minute=59, second=59)
        data  = await state.get_data()
        start = data["start"]
        if end < start:
            await msg.answer("❌ Конец не может быть раньше начала. Введи снова:")
            return
        await state.clear()
        date_range = fmt_date_range(start, end)
        label = date_range
        wait  = await msg.answer(
            f"⏳ Загружаю данные...\n<i>{date_range}</i>",
            parse_mode="HTML"
        )
        try:
            loop = asyncio.get_event_loop()
            text = await loop.run_in_executor(None, get_report, start, end, label, date_range)
        except Exception as e:
            log.error(e, exc_info=True)
            text = f"❌ Ошибка:\n<code>{e}</code>"
        await wait.edit_text(text, parse_mode="HTML", reply_markup=kb_back())
    except ValueError:
        await msg.answer("❌ Неверный формат. Пример: <code>01.01.2026</code>", parse_mode="HTML")

@dp.message(Command("cancel"))
async def h_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("❌ Отменено.", reply_markup=kb_main())


# ── Run ──────────────────────────────────

async def main():
    log.info("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
