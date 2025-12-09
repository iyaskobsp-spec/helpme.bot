# -*- coding: utf-8 -*-
import os
import json
import time
import re
import uuid
from datetime import datetime, timedelta, date
from typing import Optional, Tuple, List

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, ContextTypes, CallbackQueryHandler,
    MessageHandler, filters
)
from telegram.error import Forbidden, BadRequest, TelegramError

# ===================== ENV & CONFIG =====================
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Токен Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Назва таблиці в Google Sheets
SPREADSHEET_NAME = os.getenv("GOOGLE_SHEETS_SPREADSHEET_NAME", "BusinessTrip_forBot")

# Сервіс-аккаунт (або JSON-файл, або JSON-рядок)
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")

# --------------------- WEBHOOK --------------------------
# БАЗОВИЙ URL Railway-проєкту (без "/" наприкінці)
# Приклад: https://botname-production.up.railway.app
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "").rstrip("/")

# --------------------- SETTINGS --------------------------
# НЕ впливає на календар бронювання — лише на стару логіку
DEFAULT_DAYS_AHEAD = int(os.getenv("DEFAULT_DAYS_AHEAD", "10"))

TIME_STEP_MIN = 30              # Крок зміни часу (кнопки + / -)
REMIND_HOUR_BEFORE = 18         # Нагадування за день о 18:00
MORNING_REMIND_HOUR = 8         # Нагадування в день зміни

# --------------------- VALIDATION -------------------------
if not TELEGRAM_TOKEN:
    raise RuntimeError("❌ Missing TELEGRAM_TOKEN in .env")

if not WEBHOOK_HOST:
    raise RuntimeError("❌ Missing WEBHOOK_HOST in .env (наприклад https://myapp.up.railway.app)")

# ===================== GOOGLE SHEETS =====================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Підтримка як JSON-рядка, так і файлу
if SERVICE_ACCOUNT_JSON and SERVICE_ACCOUNT_JSON.strip().startswith("{"):
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
else:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=SCOPES)

gc = gspread.authorize(creds)
ss = gc.open(SPREADSHEET_NAME)

requests_ws = ss.worksheet("Requests")
try:
    stores_ws = ss.worksheet("Stores")
except gspread.WorksheetNotFound:
    stores_ws = ss.worksheet("Stores")  # must exist

# >>>>>>>>>>>>>>> ДОДАТИ СЮДИ <<<<<<<<<<<<<<<<

# JobQueue sheet
try:
    jobqueue_ws = ss.worksheet("JobQueue")
except gspread.WorksheetNotFound:
    jobqueue_ws = ss.add_worksheet("JobQueue", rows=500, cols=7)

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# -------------------- Колонки Requests (1-based) --------------------
# A:ID(формула)
COL_STORE       = 2   # B №_магазину
COL_CITY        = 3   # C Місто (формула в таблиці)
COL_DATE        = 4   # D
COL_TIME_FROM   = 5   # E
COL_TIME_TO     = 6   # F
COL_NEED        = 7   # G
COL_BOOKED      = 8   # H (TG_ID через кому)
COL_STATUS      = 9   # I
COL_NOTE        = 10  # J
COL_CREATED_TG  = 11  # K
COL_CREATED_PH  = 12  # L
COL_BOOKED_PH   = 13  # M (номери працівників через кому)
COL_BOOKED_NAME = 14  # N (ПІБ працівників через кому)
COL_ARRIVED     = 15  # O ("Так" якщо підтвердили прибуття)

STATUS_PENDING   = "Pending"
STATUS_WAIT      = "Очікує підтвердження"
STATUS_CONFIRMED = "Підтверджено"

# ===================== Кеші =====================
_REQ_CACHE = {"ts": 0.0, "rows": []}
_STORE_CACHE = {"ts": 0.0, "rows": []}

def get_requests_records(ttl_sec: int = 20):
    now = time.time()
    if (now - _REQ_CACHE["ts"]) < ttl_sec and _REQ_CACHE["rows"]:
        return _REQ_CACHE["rows"], True
    rows = requests_ws.get_all_records()
    _REQ_CACHE["rows"] = rows
    _REQ_CACHE["ts"] = now
    return rows, False

def get_stores_records(ttl_sec: int = 60):
    now = time.time()
    if (now - _STORE_CACHE["ts"]) < ttl_sec and _STORE_CACHE["rows"]:
        return _STORE_CACHE["rows"], True
    rows = stores_ws.get_all_records()
    _STORE_CACHE["rows"] = rows
    _STORE_CACHE["ts"] = now
    return rows, False

def safe_stores_records():
    try:
        rows, _ = get_stores_records(ttl_sec=60)
        return rows, ""
    except Exception as e:
        return [], str(e)

# ===================== Утиліти =====================
def get_store_meta(store_num: str) -> Tuple[str, str, str, str, str]:
    """Повертає (місто, область, адреса, ПІБ_ТМ, Телефон_ТМ) по №_магазину."""
    rows, _ = safe_stores_records()
    s = str(store_num).strip()
    for r in rows:
        if str(r.get("№_магазину","")).strip() == s:
            return (
                str(r.get("Місто","")).strip(),
                str(r.get("Область","")).strip(),
                str(r.get("Адреса","")).strip(),
                str(r.get("ПІБ_ТМ","")).strip(),
                str(r.get("Телефон_ТМ","")).strip(),
            )
    return "", "", "", "", ""

def parse_date_flexible(s: str) -> Optional[date]:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def jobqueue_add(job_type: str, chat_id: int, row_idx: int, when_dt: datetime, text: str):
    """Додає задачу в Google Sheets JobQueue"""
    new_id = str(uuid.uuid4())
    jobqueue_ws.append_row([
        new_id,
        job_type,
        str(chat_id),
        str(row_idx),
        when_dt.isoformat(),
        text,
        "no"
    ])
    return new_id

def jobqueue_mark_done(job_id: str):
    """Позначає задачу виконаною"""
    rows = jobqueue_ws.get_all_values()
    for idx, r in enumerate(rows, start=1):
        if r and r[0] == job_id:
            jobqueue_ws.update_cell(idx, 7, "yes")
            return

async def jobqueue_runner(context: ContextTypes.DEFAULT_TYPE):
    """Виконується при настанні події run_once"""
    data = context.job.data
    job_id = data.get("job_id")
    job_type = data.get("type")
    chat_id = data.get("chat_id")
    row_idx = data.get("row_idx")
    text = data.get("text")

    # 1 — шлемо
    if chat_id:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            pass

    # 2 — спец. випадок arrival
    if job_type == "arrival":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Я прибув(ла)", callback_data=f"arrived:{row_idx}")]
        ])
        try:
            await context.bot.send_message(chat_id=chat_id, text="Будь ласка, підтвердьте прибуття:", reply_markup=kb)
        except Exception:
            pass

    # 3 — позначаємо виконаною
    jobqueue_mark_done(job_id)

def jobqueue_load_all(app):
    """Перечитує всі задачі з таблиці при запуску бота
       і запускає їх у job_queue повторно."""
    rows = jobqueue_ws.get_all_records()
    now = datetime.now()

    for r in rows:
        if r.get("done", "no") == "yes":
            continue

        try:
            job_id = r["id"]
            job_type = r["type"]
            chat_id = int(r["chat_id"])
            row_idx = int(r["row_idx"])
            when_dt = datetime.fromisoformat(r["when"])
            text = r["text"]
        except Exception:
            continue

        delay = (when_dt - now).total_seconds()
        if delay < 0:
            delay = 2

        app.job_queue.run_once(
            jobqueue_runner,
            when=delay,
            data={
                "job_id": job_id,
                "type": job_type,
                "chat_id": chat_id,
                "row_idx": row_idx,
                "text": text
            },
            name=f"job_{job_id}"
        )

# ===================== Клавіатури: регіон/місто/магазини =====================
def build_region_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Київ і область", callback_data="region:kyiv")],
        [InlineKeyboardButton("Інші міста", callback_data="region:other")]
    ])

def build_cities_keyboard_region(region: str):
    rows, _ = safe_stores_records()
    if not rows:
        return None

    cities_all = sorted({str(r.get("Місто", "")).strip() for r in rows if str(r.get("Місто", "")).strip()})
    oblast_map = {
        str(r.get("Місто", "")).strip(): str(r.get("Область", "")).strip().lower()
        for r in rows
    }

    def is_kyiv_area(city: str) -> bool:
        c = city.lower()
        return ("київ" in c) or (oblast_map.get(city, "") == "київська")

    if region == "kyiv":
        cities = [c for c in cities_all if is_kyiv_area(c)]
    else:
        cities = [c for c in cities_all if not is_kyiv_area(c)]

    if not cities:
        return None

    # 2 колонки
    buttons, row = [], []
    for i, city in enumerate(cities, 1):
        row.append(InlineKeyboardButton(city, callback_data=f"pickcity:{city}"))
        if i % 2 == 0:
            buttons.append(row); row = []
    if row: buttons.append(row)
    return InlineKeyboardMarkup(buttons)

def build_stores_keyboard(city: str):
    rows, _ = safe_stores_records()

    stores = [
        (str(r.get("№_магазину","")).strip(), str(r.get("Адреса","")).strip())
        for r in rows if str(r.get("Місто","")).strip() == city
    ]
    if not stores:
        return None

    buttons = []
    row = []

    for i, (num, addr) in enumerate(stores, start=1):
        label = f"{num} • {addr.split(',')[0]}"
        row.append(InlineKeyboardButton(label, callback_data=f"pickstore:{num}"))
        if i % 2 == 0:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    return InlineKeyboardMarkup(buttons)

# ===================== Список змін по місту (без сьогодні, сортовані) =====================
def build_shifts_keyboard_by_city(city: str, days_ahead: Optional[int] = None):
    try:
        limit = int(os.getenv("DEFAULT_DAYS_AHEAD", "10")) if days_ahead is None else int(days_ahead)
    except Exception:
        limit = 10

    today = datetime.now().date()
    start_day = today + timedelta(days=1)   # показуємо з завтрашнього дня
    last_day  = today + timedelta(days=limit)

    stores_rows, _ = safe_stores_records()
    addr_map = {str(s.get("№_магазину","")).strip(): str(s.get("Адреса","")).strip() for s in stores_rows}
    city_map = {str(s.get("№_магазину","")).strip(): str(s.get("Місто","")).strip() for s in stores_rows}

    rows, _ = get_requests_records(ttl_sec=15)
    items: List[Tuple[int, date, str]] = []  # (row_idx, date, label)

    for idx, r in enumerate(rows, start=2):
        store = str(r.get("№_магазину","")).strip()
        if not store:
            continue

        r_city = (str(r.get("Місто","")).strip() or city_map.get(store, ""))
        if r_city != city:
            continue

        status_raw = (str(r.get("Статус","")).strip() or "").lower()
        status_ok = (
            status_raw == "" or
            "pending" in status_raw or
            "очіку" in status_raw or
            "підтвер" in status_raw or
            "confirm" in status_raw
        )
        if not status_ok:
            continue

        needed_s = str(r.get("Потрібно","")).strip().replace(",", ".")
        if needed_s.isdigit():
            needed = int(needed_s)
        elif needed_s.replace(".", "", 1).isdigit():
            needed = max(1, int(float(needed_s)))
        else:
            needed = 1
        if needed < 1:
            needed = 1

        booked_raw = str(r.get("Заброньовано","")).strip()
        booked_ids = [x.strip() for x in booked_raw.split(",") if x.strip().isdigit()]
        free = max(0, needed - len(booked_ids))
        if free <= 0:
            continue

        date_s = str(r.get("Дата","")).strip()
        d = parse_date_flexible(date_s)
        if not d or not (start_day <= d <= last_day):
            continue

        t_start = str(r.get("Час_початку","")).strip()
        t_end   = str(r.get("Час_закінчення","")).strip()

        full_addr = addr_map.get(store, "").strip()
        short_addr = (full_addr.split(",")[0] if full_addr else "")
        if len(short_addr) > 22:
            short_addr = short_addr[:22]

        label = f"{d.strftime('%d.%m')} {t_start}-{t_end} • ТТ {store}"
        if short_addr:
            label += f" • {short_addr}"
        label += f" • {len(booked_ids)}/{needed}"

        items.append((idx, d, label))

    if not items:
        return None

    # сортуємо за датою зростання
    items.sort(key=lambda x: x[1])
    buttons = [[InlineKeyboardButton(text[:64], callback_data=f"book:{row_idx}")]
               for (row_idx, _, text) in items[:50]]
    return InlineKeyboardMarkup(buttons)

# ===================== Календар / час =====================
def _month_days(year: int, month: int):
    import calendar
    first_weekday, days_count = calendar.monthrange(year, month)
    return first_weekday, days_count  # Пн=0 ... Нд=6

def build_calendar(year: int = None, month: int = None):
    today = date.today()
    if year is None: year = today.year
    if month is None: month = today.month

    first_wd, days = _month_days(year, month)

    row1 = [
        InlineKeyboardButton("«", callback_data=f"calnav:{year}:{month}:prev"),
        InlineKeyboardButton(f"{year}-{month:02d}", callback_data="noop"),
        InlineKeyboardButton("»", callback_data=f"calnav:{year}:{month}:next"),
    ]
    wk = ["Пн","Вт","Ср","Чт","Пт","Сб","Нд"]
    row2 = [InlineKeyboardButton(x, callback_data="noop") for x in wk]

    buttons = [row1, row2]
    row = []
    pad = (first_wd - 0) % 7
    for _ in range(pad):
        row.append(InlineKeyboardButton(" ", callback_data="noop"))

    for d in range(1, days+1):
        row.append(InlineKeyboardButton(str(d), callback_data=f"calpick:{year}-{month:02d}-{d:02d}"))
        if len(row) == 7:
            buttons.append(row); row = []
    if row: buttons.append(row)
    return InlineKeyboardMarkup(buttons)

# ===================== Календар для бронювання з виділенням змін =====================

def build_booking_calendar(city: str, year: int = None, month: int = None):
    today = date.today()
    if year is None: year = today.year
    if month is None: month = today.month

    # --- отримуємо список доступних дат у цьому місті ---
    rows, _ = get_requests_records(ttl_sec=20)
    stores, _ = safe_stores_records()

    city_map = {str(s.get("№_магазину","")).strip(): str(s.get("Місто","")).strip() for s in stores}

    available_dates = set()
    for r in rows:
        store = str(r.get("№_магазину","")).strip()
        r_city = (str(r.get("Місто","")).strip() or city_map.get(store, ""))

        if r_city != city:
            continue

        status_raw = (str(r.get("Статус","")).strip() or "").lower()
        if not ("pending" in status_raw or "очіку" in status_raw or "confirm" in status_raw):
            continue

        date_s = str(r.get("Дата","")).strip()
        d = parse_date_flexible(date_s)
        if d:
            available_dates.add(d)

    first_wd, days = _month_days(year, month)

    row1 = [
        InlineKeyboardButton("«", callback_data=f"calnav2:{year}:{month}:prev"),
        InlineKeyboardButton(f"{year}-{month:02d}", callback_data="noop"),
        InlineKeyboardButton("»", callback_data=f"calnav2:{year}:{month}:next"),
    ]
    wk = ["Пн","Вт","Ср","Чт","Пт","Сб","Нд"]
    row2 = [InlineKeyboardButton(x, callback_data="noop") for x in wk]

    buttons = [row1, row2]
    row = []

    pad = (first_wd - 0) % 7
    for _ in range(pad):
        row.append(InlineKeyboardButton(" ", callback_data="noop"))

    for d in range(1, days+1):
        cur = date(year, month, d)

        # минулі дні → неактивні
        if cur < today:
            row.append(InlineKeyboardButton(" ", callback_data="noop"))

        # дні зі змінами → ⭐
        elif cur in available_dates:
            row.append(InlineKeyboardButton(f"{d}⭐", callback_data=f"bookdate:{cur}"))

        # інші майбутні → активні, але потім перевіримо (варіант B)
        else:
            row.append(InlineKeyboardButton(str(d), callback_data=f"bookdate:{cur}"))

        if len(row) == 7:
            buttons.append(row); row = []

    if row:
        buttons.append(row)

    return InlineKeyboardMarkup(buttons)

def _time_to_str(h: int, m: int) -> str:
    return f"{h:02d}:{m:02d}"

def _inc_time(h: int, m: int, step: int = TIME_STEP_MIN):
    total = h*60 + m + step
    total %= (24*60)
    return total//60, total%60

def _dec_time(h: int, m: int, step: int = TIME_STEP_MIN):
    total = h*60 + m - step
    total %= (24*60)
    return total//60, total%60

def build_time_picker(prefix, h, m, label="Час"):
    t = f"{h:02d}:{m:02d}"

    # 0.5 частини
    btn_minus = InlineKeyboardButton(" – ", callback_data=f"{prefix}:dec:{h}:{m}")
    # 2 частини
    btn_center = InlineKeyboardButton("        " + t + "        ", callback_data="noop")
    # 0.5 частини
    btn_plus = InlineKeyboardButton(" + ", callback_data=f"{prefix}:inc:{h}:{m}")

    row1 = [btn_minus, btn_center, btn_plus]
    row2 = [InlineKeyboardButton("OK", callback_data=f"{prefix}:ok:{h}:{m}")]

    return InlineKeyboardMarkup([row1, row2])


def _parse_hm(hh: str, mm: str) -> Tuple[int,int]:
    return int(hh), int(mm)

# ===================== Команди =====================
# ===================== START =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    context.user_data["creator_tg"] = tg_id

    inline_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 Створити зміну", callback_data="menu:create")],
        [InlineKeyboardButton("📅 Забронювати зміни", callback_data="menu:book")],
        [InlineKeyboardButton("🗂 Мої відпрацьовані зміни", callback_data="menu:mydone")]
    ])

    # Головне меню в повідомленні
    await update.message.reply_text(
        "Оберіть дію:",
        reply_markup=inline_kb
    )

    # Стабільна кнопка під клавіатурою
    await update.message.reply_text(
        "Меню доступне внизу 👇",
        reply_markup=stable_menu_keyboard()
    )

# ===================== On start button =====================
async def on_start_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def on_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong 🏓")

async def shifts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Використовуй кнопки меню вище.")

# ===================== Контакт / текст =====================
async def on_contact_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    contact = update.message.contact

    # збереження телефону і TG ID
    phone = contact.phone_number if contact else ""
    context.user_data["creator_phone"] = phone
    context.user_data["creator_tg"] = update.effective_user.id

    print(f"[debug] on_contact_create: phone={phone}  user_id={update.effective_user.id}")

    await update.message.reply_text("Дякую! ✅ Телефон збережено.", reply_markup=ReplyKeyboardRemove())

    # --- Якщо чекали телефон для сторінки "Мої відпрацьовані" ---
    if context.user_data.pop("await_mydone_phone", False):
        await show_my_attendance(update, context)
        return

    # --- Якщо чекали телефон для бронювання ---
    pending_row = context.user_data.pop("pending_book_row", None)
    if pending_row:
        await complete_booking_after_data(update, context, int(pending_row))
        return

    # --- Якщо чекали телефон для створення зміни ---
    if context.user_data.pop("await_create_phone", False):
        await update.message.reply_text(
            "📍 Телефон збережено. Тепер обери регіон:",
            reply_markup=build_region_keyboard()
        )
        context.user_data["mode"] = "create"
        return

    # --- Якщо нічого не чекали ---
    await update.message.reply_text(
        "Оберіть регіон:",
        reply_markup=build_region_keyboard()
    )

async def handle_create_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    step = context.user_data.get("await")
    txt = (update.message.text or "").strip()


    if step == "emp_name":
        parts = txt.split()

        # Мінімум 2 слова
        if len(parts) < 2:
            await update.message.reply_text("Вкажіть ПІБ у форматі: Прізвище Ім’я")
            return

        surname = parts[0].capitalize()
        name = " ".join([p.capitalize() for p in parts[1:]])

        full_name = f"{surname} {name}"

        context.user_data["emp_name"] = full_name

        pending_row = context.user_data.pop("pending_book_row", None)
        if pending_row:
            await complete_booking_after_data(update, context, int(pending_row))
            return

        await update.message.reply_text("Дякую! Тепер оберіть дію в меню.")
        return

    if step == "needed":
        try:
            needed = int(txt)
            if needed < 1:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❗ Введи додатне ціле число (наприклад, 1 або 2).")
            return

        store = context.user_data.get("store_num") or ""
        d     = context.user_data.get("date") or ""          # YYYY-MM-DD
        ts    = context.user_data.get("time_start") or ""
        te    = context.user_data.get("time_end") or ""
        creator_tg    = context.user_data.get("creator_tg") or update.effective_user.id
        creator_phone = context.user_data.get("creator_phone") or ""

        colB = requests_ws.col_values(COL_STORE)
        next_row = len(colB) + 1

        try:
            d_obj = datetime.strptime(d, "%Y-%m-%d")
            d_str = d_obj.strftime("%d.%m.%Y")
        except Exception:
            d_str = str(d)

        payload = [
            {'range': f'B{next_row}:B{next_row}', 'values': [[store]]},
            {'range': f'D{next_row}:G{next_row}', 'values': [[d_str, ts, te, needed]]},
            {'range': f'I{next_row}:I{next_row}', 'values': [[STATUS_PENDING]]},
            {'range': f'K{next_row}:L{next_row}', 'values': [[str(creator_tg), str(creator_phone)]]},
        ]
        requests_ws.batch_update(payload)

        for k in ("await","date","time_start","time_end","store_num"):
            context.user_data.pop(k, None)

        await update.message.reply_text("✅ Зміну створено. Вона з’явиться у списку доступних для бронювання.")

        return

# ===================== Допоміжні дії =====================
async def show_my_attendance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = context.user_data.get("creator_phone","")
    phone_digits = re.sub(r"\D","", phone)
    try:
        ws = ss.worksheet("Attendance")
        rows = ws.get_all_records()
    except Exception:
        rows = []

    mine = [r for r in rows if re.sub(r"\D","", str(r.get("Телефон_працівника",""))) == phone_digits]

    def _dt_of(r):
        s = str(r.get("Дата","")).strip()
        for fmt in ("%d.%m.%Y","%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
        return datetime.min

    mine.sort(key=_dt_of, reverse=True)
    if not mine:
        await update.effective_message.edit_text("Наразі немає відмічених як відпрацьовані.")
        return

    text = "🗂 Твої відпрацьовані зміни:\n\n"
    for r in mine[:10]:
        text += (f"{r.get('Дата','?')} • {r.get('Місто','?')} • ТТ {r.get('№_магазину','?')}\n"
                 f"Підтвердження прибуття: {r.get('Прибуття_підтверджено','') or '—'}\n\n")
    await update.effective_message.edit_text(text)

async def complete_booking_after_data(update: Update, context: ContextTypes.DEFAULT_TYPE, row_idx: int):
    row = requests_ws.row_values(row_idx)
    while len(row) < COL_ARRIVED:
        row.append("")

    store       = (row[COL_STORE-1] or "").strip()
    city_cell   = (row[COL_CITY-1] or "").strip()
    date_s      = (row[COL_DATE-1] or "").strip()
    t_start     = (row[COL_TIME_FROM-1] or "").strip()
    t_end       = (row[COL_TIME_TO-1] or "").strip()
    needed_s    = (row[COL_NEED-1] or "").strip()
    booked_raw  = (row[COL_BOOKED-1] or "").strip()
    manager_raw = (row[COL_CREATED_TG-1] or "").strip()

    # кількість
    s = (needed_s or "").replace(",", ".")
    needed = int(float(s)) if s.replace(".", "", 1).isdigit() else 1
    booked_ids = [x.strip() for x in booked_raw.split(",") if x.strip().isdigit()]

    tg_id = str(update.effective_user.id)

    if tg_id in booked_ids:
        await update.message.reply_text("ℹ️ Ти вже бронював(ла) цю зміну.")
        return

    if len(booked_ids) >= needed:
        await update.message.reply_text("❗ На жаль, усі місця на цю зміну вже заброньовані.")
        return

    emp_name = context.user_data.get("emp_name")
    if not emp_name:
        context.user_data["pending_book_row"] = row_idx
        context.user_data["await"] = "emp_name"
        await update.message.reply_text("Вкажіть ПІБ у форматі: Прізвище Ім’я")
        return

    # запис у таблицю
    booked_ids.append(tg_id)
    requests_ws.update_cell(row_idx, COL_BOOKED, ", ".join(booked_ids))
    new_status = f"{STATUS_WAIT} ({len(booked_ids)}/{needed})"
    requests_ws.update_cell(row_idx, COL_STATUS, new_status)

    worker_phone = re.sub(r"\D", "", context.user_data.get("creator_phone", ""))
    phones_raw = (row[COL_BOOKED_PH-1] or "")
    phone_list = [x.strip() for x in phones_raw.split(",") if x.strip()]
    if worker_phone:
        phone_list.append(worker_phone)
    requests_ws.update_cell(row_idx, COL_BOOKED_PH, ", ".join(phone_list))

    names_raw = (row[COL_BOOKED_NAME-1] or "")
    name_list = [x.strip() for x in names_raw.split(",") if x.strip()]
    name_list.append(emp_name)
    requests_ws.update_cell(row_idx, COL_BOOKED_NAME, ", ".join(name_list))

    # повідомлення працівнику
    meta_city, meta_obl, meta_addr, _, _ = get_store_meta(store)

    # Якщо місто в Requests пусте – беремо з Stores
    city = city_cell if city_cell else meta_city

    # Адреса завжди повністю з таблиці Stores!
    address = meta_addr

    await update.message.reply_text(
        "✅ Твоє бронювання збережено.\n"
        f"Місто: {city}\n"
        f"Адреса: {address}\n"
        f"ТТ: {store}\n"
        f"Дата: {date_s}\n"
        f"Час: {t_start}–{t_end}\n"
        f"Статус: {new_status}"
    )

    # надсилання керівнику
    manager_id = re.sub(r"\D", "", manager_raw)

    print(f"[debug] sending to manager: {manager_id}")

    if manager_id:
        cb = f"mgrconfirm:{row_idx}:{tg_id}:{worker_phone}"
        kb_mgr = InlineKeyboardMarkup(
            [[InlineKeyboardButton("✅ Підтвердити бронювання", callback_data=cb)]]
        )

        text_mgr = (
            "🔔 Запит на бронювання зміни\n"
            f"Місто: {city}\n"
            f"Адреса: {address}\n"
            f"ТТ: {store}\n"
            f"Дата: {date_s}\n"
            f"Час: {t_start}–{t_end}\n"
            f"Працівник: {emp_name} • +{worker_phone}\n"
            f"Поточний статус: {new_status}"
        )

        await update.effective_chat.send_message("Запит надіслано керівнику на підтвердження.")

        await update.get_bot().send_message(
            chat_id=int(manager_id),
            text=text_mgr,
            reply_markup=kb_mgr
        )

def _write_creator_fields(row_idx, update, context):
    """Записує TG_ID і телефон керівника (того, хто створив зміну)"""
    try:
        tg_id = str(context.user_data.get("creator_tg", "")).strip()
        phone = str(context.user_data.get("creator_phone", "")).strip()

        print(f"[debug] _write_creator_fields(): row={row_idx}, tg_id={tg_id}, phone={phone}")

        # K = Created_By_TG, L = Created_By_Phone
        if tg_id:
            requests_ws.update_cell(row_idx, 11, tg_id)
        if phone:
            requests_ws.update_cell(row_idx, 12, phone)
    except Exception as e:
        print(f"[debug] _write_creator_fields error: {e}")

from telegram import ReplyKeyboardMarkup, KeyboardButton

def stable_menu_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🏠 Меню")]],
        resize_keyboard=True,
        one_time_keyboard=False
    )

# =====================================================================
# Автоматичне повернення кнопки "Меню" після будь-якої дії
# =====================================================================
async def auto_show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показує стабільну кнопку "Меню", щоб вона не зникала після пауз,
    переходів у інші боти, очистки клавіатури Telegram та інших глюків.
    """
    try:
        await update.effective_chat.send_message(
            " ",
            reply_markup=stable_menu_keyboard()
        )
    except:
        pass

def persistent_menu():
    return ReplyKeyboardMarkup(
        [["🏠 Меню"]],
        resize_keyboard=True
    )

# ===================== Callback =====================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    # --- Меню створення зміни ---
    if data == "menu:create":
        keep_phone = context.user_data.get("creator_phone")
        keep_name  = context.user_data.get("emp_name")
        keep_tg    = context.user_data.get("creator_tg") or update.effective_user.id

        # Якщо немає телефону керівника — просимо його одразу
        if not keep_phone:
            kb = ReplyKeyboardMarkup(
                [[KeyboardButton("📞 Поділитися номером", request_contact=True)]],
                resize_keyboard=True, one_time_keyboard=True
            )
            await update.effective_message.reply_text(
                "📲 Щоб створити зміну, спочатку поділися своїм номером телефону:",
                reply_markup=kb
            )
            context.user_data["await_create_phone"] = True
            return

        # якщо телефон уже є — продовжуємо
        context.user_data.clear()
        if keep_phone: context.user_data["creator_phone"] = keep_phone
        if keep_name:  context.user_data["emp_name"] = keep_name
        if keep_tg:    context.user_data["creator_tg"] = keep_tg

        context.user_data["mode"] = "create"
        await update.effective_message.edit_text(
            "Оберіть регіон:",
            reply_markup=build_region_keyboard()
        )
        return

    # --- Меню бронювання зміни ---
    if data == "menu:book":
        keep_phone = context.user_data.get("creator_phone")
        keep_name  = context.user_data.get("emp_name")
        keep_tg    = context.user_data.get("creator_tg") or update.effective_user.id

        context.user_data.clear()
        if keep_phone: context.user_data["creator_phone"] = keep_phone
        if keep_name:  context.user_data["emp_name"] = keep_name
        if keep_tg:    context.user_data["creator_tg"] = keep_tg

        context.user_data["mode"] = "book"
        await update.effective_message.edit_text("Оберіть регіон:", reply_markup=build_region_keyboard())
        return

    if data == "menu:mydone":
        if not context.user_data.get("creator_phone"):
            kb = ReplyKeyboardMarkup([[KeyboardButton("📞 Поділитися номером", request_contact=True)]],
                                     resize_keyboard=True, one_time_keyboard=True)
            await update.effective_message.reply_text("Щоб знайти твої відпрацьовані, надішли номер:", reply_markup=kb)
            context.user_data["await_mydone_phone"] = True
            return
        await show_my_attendance(update, context)
        return

    # Регіон → міста
    if data.startswith("region:"):
        region = data.split(":",1)[1]
        context.user_data["region"] = region
        kb = build_cities_keyboard_region(region)
        if kb:
            mode = context.user_data.get("mode")
            prompt = "Оберіть місто для створення:" if mode == "create" else "Оберіть місто для бронювання:"
            await update.effective_message.edit_text(prompt, reply_markup=kb)
        else:
            await update.effective_message.edit_text("Не знайшла довідник міст у вибраному регіоні.")
        return

    # Місто → або списки змін, або магазини
    if data.startswith("pickcity:"):
        city = data.split(":", 1)[1]
        context.user_data["city"] = city
        mode = context.user_data.get("mode") or "book"

        if mode == "book":
            await update.effective_message.edit_text(
                f"Місто: {city}\nОберіть дату:",
                reply_markup=build_booking_calendar(city)
            )
            return

        # create
        kb = build_stores_keyboard(city)
        if kb:
            await update.effective_message.edit_text(f"Місто: {city}\nОберіть №_магазину:", reply_markup=kb)
        else:
            await update.effective_message.edit_text(f"У місті {city} немає магазинів у довіднику.")
        return

    # Магазин → календар дати
    if data.startswith("pickstore:"):
        store_num = data.split(":", 1)[1]
        context.user_data["store_num"] = store_num
        await update.effective_message.edit_text(
            f"✅ Магазин обрано: {store_num}\n\nОберіть дату зміни:",
            reply_markup=build_calendar()
        )
        return

    # Календар навігація
    if data.startswith("calnav:"):
        _, y, m, dirn = data.split(":")
        y, m = int(y), int(m)
        if dirn == "prev":
            m -= 1
            if m == 0: m, y = 12, y-1
        else:
            m += 1
            if m == 13: m, y = 1, y+1
        await update.effective_message.edit_text("Оберіть дату зміни:", reply_markup=build_calendar(y,m))
        return

    # --- Навігація календаря для бронювання ---
    if data.startswith("calnav2:"):
        _, y, m, direction = data.split(":")
        y, m = int(y), int(m)

        if direction == "prev":
            m -= 1
            if m == 0:
                m, y = 12, y - 1
        else:
            m += 1
            if m == 13:
                m, y = 1, y + 1

        city = context.user_data.get("city")
        await update.effective_message.edit_text(
            "Оберіть дату:",
            reply_markup=build_booking_calendar(city, y, m)
        )
        return

    # Обрана дата → вибір часу початку
    if data.startswith("calpick:"):
        d = data.split(":",1)[1]
        context.user_data["date"] = d
        kb = build_time_picker("tstart", 9, 0, label="Початок")
        dd = datetime.strptime(d, '%Y-%m-%d').strftime('%d.%m.%Y')
        await update.effective_message.edit_text(f"Дата: {dd}\nОберіть час початку:", reply_markup=kb)
        return

    # Час початку
    if data.startswith("tstart:"):
        _, action, hh, mm = data.split(":")
        h, m = _parse_hm(hh, mm)
        if action == "inc":
            h,m = _inc_time(h,m)
            kb = build_time_picker("tstart", h, m, label="Початок")
            await update.effective_message.edit_text("Оберіть час початку:", reply_markup=kb)
        elif action == "dec":
            h,m = _dec_time(h,m)
            kb = build_time_picker("tstart", h, m, label="Початок")
            await update.effective_message.edit_text("Оберіть час початку:", reply_markup=kb)
        elif action == "ok":
            context.user_data["time_start"] = _time_to_str(h,m)
            kb = build_time_picker("tend", 18, 0, label="Кінець")
            await update.effective_message.edit_text("Оберіть час закінчення:", reply_markup=kb)
        return

    # Час кінця
    if data.startswith("tend:"):
        _, action, hh, mm = data.split(":")
        h, m = _parse_hm(hh, mm)
        if action in ("inc","dec"):
            if action=="inc": h,m=_inc_time(h,m)
            else: h,m=_dec_time(h,m)
            kb = build_time_picker("tend", h, m, label="Кінець")
            await update.effective_message.edit_text("Оберіть час закінчення:", reply_markup=kb)
            return
        if action == "ok":
            context.user_data["time_end"] = _time_to_str(h,m)
            context.user_data["await"] = "needed"
            await update.effective_message.edit_text("Скільки працівників потрібно? (введи ціле число, наприклад 2)")
            return

    # --- Підтвердження створення зміни (кнопкою) ---
    if data.startswith("confirm_create:"):
        parts = data.split(":")
        if len(parts) < 6:
            await update.effective_message.edit_text("❌ Дані створення неповні.")
            return

        city = parts[1]
        store = parts[2]
        date_s = parts[3]
        t_start = parts[4]
        t_end = parts[5]
        needed = int(parts[6]) if len(parts) > 6 and parts[6].isdigit() else 1

        # --- визначаємо новий рядок ---
        colB = requests_ws.col_values(COL_STORE)
        next_row = len(colB) + 1

        # --- запис усіх основних даних ---
        payload = [
            {'range': f'B{next_row}:B{next_row}', 'values': [[store]]},
            {'range': f'C{next_row}:C{next_row}', 'values': [[city]]},  # <– МІСТО
            {'range': f'D{next_row}:G{next_row}', 'values': [[date_s, t_start, t_end, needed]]},
            {'range': f'I{next_row}:I{next_row}', 'values': [[STATUS_PENDING]]},
            {'range': f'K{next_row}:L{next_row}', 'values': [[
                str(context.user_data.get("creator_tg") or update.effective_user.id),
                str(context.user_data.get("creator_phone") or "")
            ]]}
        ]
        requests_ws.batch_update(payload)

        # --- контрольний виклик для надійності ---
        try:
            _write_creator_fields(next_row, update, context)
        except Exception as e:
            print(f"[debug] дублюючий запис керівника не вдався: {e}")

        await update.effective_message.edit_text("✅ Зміну створено успішно.")

        return

    # --- Обрана дата ---
    if data.startswith("bookdate:"):
        _, d = data.split(":")
        d_obj = parse_date_flexible(d)

        if not d_obj:
            await update.effective_message.edit_text("Помилка читання дати.")
            return

        context.user_data["book_date"] = d

        # Завантажуємо зміни на цю дату та це місто
        city = context.user_data.get("city")

        rows, _ = get_requests_records()
        stores, _ = safe_stores_records()
        city_map = {str(s.get("№_магазину","")).strip(): str(s.get("Місто","")).strip() for s in stores}

        # шукаємо всі зміни на обрану дату
        avail = []
        for idx, r in enumerate(rows, start=2):
            store = str(r.get("№_магазину","")).strip()
            r_city = (str(r.get("Місто","")).strip() or city_map.get(store, ""))

            if r_city != city:
                continue

            date_s = str(r.get("Дата","")).strip()
            r_date = parse_date_flexible(date_s)
            if not r_date or r_date != d_obj:
                continue

            needed_s = str(r.get("Потрібно","")).strip()
            needed = int(float(needed_s)) if needed_s.replace(",",".").replace(".","",1).isdigit() else 1

            booked_raw = str(r.get("Заброньовано","")).strip()
            booked_ids = [x.strip() for x in booked_raw.split(",") if x.strip()]
            free = needed - len(booked_ids)

            if free <= 0:
                continue

            t_start = str(r.get("Час_початку","")).strip()
            t_end   = str(r.get("Час_закінчення","")).strip()

            label = f"{t_start}-{t_end} • ТТ {store}"
            avail.append((idx, label))

        if not avail:
            await update.effective_message.edit_text(
                "На цю дату немає доступних змін.\nОберіть іншу дату:",
                reply_markup=build_booking_calendar(city)
            )
            return

        kb = [[InlineKeyboardButton(text, callback_data=f"book:{row_idx}")]
              for row_idx, text in avail]

        await update.effective_message.edit_text(
            f"Дата: {d_obj.strftime('%d.%m.%Y')}\nОберіть зміну:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    # --- Бронювання зміни (натискання на зміну) ---
    if data.startswith("book:"):
        row_idx = int(data.split(":", 1)[1])

        if not context.user_data.get("creator_phone"):
            context.user_data["pending_book_row"] = row_idx
            kb = ReplyKeyboardMarkup(
                [[KeyboardButton("📞 Поділитися номером", request_contact=True)]],
                resize_keyboard=True, one_time_keyboard=True
            )
            await update.effective_chat.send_message("Щоб завершити бронювання, надішли свій номер:", reply_markup=kb)
            return

        if not context.user_data.get("emp_name"):
            context.user_data["pending_book_row"] = row_idx
            context.user_data["await"] = "emp_name"
            await update.effective_message.edit_text("Вкажіть ПІБ у форматі: Прізвище Ім’я")
            return

        await complete_booking_after_data(update, context, row_idx)
        return

    # --- Підтвердження керівником ---
    if data.startswith("mgrconfirm:"):
        parts = data.split(":")
        row_idx = int(parts[1])
        worker_tg = parts[2]
        worker_phone = parts[3]

        row = requests_ws.row_values(row_idx)
        while len(row) < COL_ARRIVED:
            row.append("")

        manager_raw = (row[COL_CREATED_TG-1] or "").strip()
        manager_phone_raw = (row[COL_CREATED_PH-1] or "").strip()

        manager_id = int(manager_raw) if manager_raw.isdigit() else None
        manager_phone_digits = re.sub(r"\D", "", manager_phone_raw)
        user_phone_digits = re.sub(r"\D", "", context.user_data.get("creator_phone", ""))

        if (manager_id and manager_id != update.effective_user.id) and \
           (manager_phone_digits != user_phone_digits):
            await update.effective_message.edit_text(
                "❗ Підтвердження доступне лише керівнику, який створив зміну."
            )
            return

        store      = (row[COL_STORE-1] or "").strip()
        city       = (row[COL_CITY-1] or "").strip()
        date_s     = (row[COL_DATE-1] or "").strip()
        t_start    = (row[COL_TIME_FROM-1] or "").strip()
        t_end      = (row[COL_TIME_TO-1] or "").strip()
        needed_s   = (row[COL_NEED-1] or "").strip()
        booked_raw = (row[COL_BOOKED-1] or "").strip()

        needed = int(float(needed_s.replace(",", "."))) if needed_s else 1
        booked_ids = [x.strip() for x in booked_raw.split(",") if x.strip()]

        new_status = f"{STATUS_CONFIRMED} ({len(booked_ids)}/{needed})"
        requests_ws.update_cell(row_idx, COL_STATUS, new_status)

        meta_city, meta_obl, meta_addr, _, _ = get_store_meta(store)
        address = meta_addr
        city = city or meta_city

        phone_view = "+" + worker_phone if worker_phone else "—"

        await update.effective_message.edit_text(
            "✅ Ви підтвердили бронювання\n"
            f"Місто: {city}\n"
            f"Адреса: {address}\n"
            f"ТТ: {store}\n"
            f"Дата: {date_s}\n"
            f"Час: {t_start}–{t_end}\n"
            f"Працівник: {phone_view}\n"
            f"Статус: {new_status}"
        )

        # Повідомлення працівнику
        await update.get_bot().send_message(
            chat_id=int(worker_tg),
            text=(
                "✅ Ваше бронювання підтверджено керівником.\n"
                f"Місто: {city}\n"
                f"Адреса: {address}\n"
                f"ТТ: {store}\n"
                f"Дата: {date_s}\n"
                f"Час: {t_start}–{t_end}\n"
                f"Телефон керівника: +{manager_phone_digits}"
            )
        )
        
        # --- PERSISTENT JobQueue ---
        try:
            # --- Парсимо дату ---
            d = None
            for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                try:
                    d = datetime.strptime(date_s, fmt).date()
                    break
                except:
                    pass

            if d:
                now = datetime.now()

                # ---------- 1) Нагадування за день (18:00) ----------
                day_before_dt = datetime(
                    d.year, d.month, d.day, REMIND_HOUR_BEFORE, 0
                ) - timedelta(days=1)

                if day_before_dt > now:
                    job_id = jobqueue_add(
                        job_type="remind",
                        chat_id=int(worker_tg),
                        row_idx=row_idx,
                        when_dt=day_before_dt,
                        text=(
                            f"🔔 Нагадування: завтра зміна\n"
                            f"{city}, ТТ {store}\n"
                            f"{date_s} {t_start}–{t_end}\n"
                            f"Адреса: {address}"
                        )
                    )

                    delay = (day_before_dt - now).total_seconds()

                    context.application.job_queue.run_once(
                        jobqueue_runner,
                        when=delay,
                        data={
                            "job_id": job_id,
                            "type": "remind",
                            "chat_id": int(worker_tg),
                            "row_idx": row_idx,
                            "text": (
                                f"🔔 Нагадування: завтра зміна\n"
                                f"{city}, ТТ {store}\n"
                                f"{date_s} {t_start}–{t_end}\n"
                                f"Адреса: {address}"
                            ),
                        }
                    )

                # ---------- 2) Підтвердження прибуття ----------
                try:
                    sh, sm = map(int, t_start.split(":"))
                except:
                    sh, sm = 9, 0

                start_dt = datetime(d.year, d.month, d.day, sh, sm)

                if start_dt > now:
                    job_id = jobqueue_add(
                        job_type="arrival",
                        chat_id=int(worker_tg),
                        row_idx=row_idx,
                        when_dt=start_dt,
                        text=""
                    )

                    delay = (start_dt - now).total_seconds()

                    context.application.job_queue.run_once(
                        jobqueue_runner,
                        when=delay,
                        data={
                            "job_id": job_id,
                            "type": "arrival",
                            "chat_id": int(worker_tg),
                            "row_idx": row_idx,
                            "text": ""
                        }
                    )

        except Exception as e:
            print("[debug] error persistent scheduling:", e)

        return

    # --- Підтвердження прибуття користувачем ---
    if data.startswith("arrived:"):
        row_idx = int(data.split(":",1)[1])
        try:
            row = requests_ws.row_values(row_idx)
            while len(row) < COL_ARRIVED:
                row.append("")
        except Exception:
            await update.effective_message.edit_text("❌ Не вдалося прочитати рядок для відмітки.")
            return

        store     = (row[COL_STORE-1] or "").strip()
        city_cell = (row[COL_CITY-1] or "").strip()
        date_s    = (row[COL_DATE-1] or "").strip()

        meta_city, meta_obl, meta_addr, _, _ = get_store_meta(store)
        city = city_cell or meta_city

        try:
            att = ss.worksheet("Attendance")
        except gspread.WorksheetNotFound:
            att = ss.add_worksheet("Attendance", rows=1000, cols=10)

        phone = context.user_data.get("creator_phone","")
        phone_digits = re.sub(r"\D","", phone)
        emp_name = context.user_data.get("emp_name","")

        next_row = len(att.col_values(1)) + 1
        payload = [
            {'range': f'A{next_row}:A{next_row}', 'values': [[city]]},
            {'range': f'B{next_row}:B{next_row}', 'values': [[store]]},
            {'range': f'C{next_row}:C{next_row}', 'values': [[""]]},
            {'range': f'D{next_row}:D{next_row}', 'values': [[date_s]]},
            {'range': f'E{next_row}:E{next_row}', 'values': [[emp_name]]},
            {'range': f'F{next_row}:F{next_row}', 'values': [[phone_digits]]},
            {'range': f'G{next_row}:G{next_row}', 'values': [["Так"]]},
        ]
        att.batch_update(payload)

        try:
            requests_ws.update_cell(row_idx, COL_ARRIVED, "Так")
        except Exception:
            pass

        await update.effective_message.edit_text("✅ Дякуємо! Прибуття відмічено.")

        await update.effective_chat.send_message(
            "Оберіть дію:",
            reply_markup=persistent_menu()
        )
        return

    # В самому кінці on_callback
    try:
        await auto_show_menu(update, context)
    except:
        pass

# ===================== Джоби =====================
async def job_remind_tomorrow(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    txt = data.get("text","Нагадування про зміну завтра.")
    if chat_id:
        try:
            await context.bot.send_message(chat_id=chat_id, text=txt)
        except Exception:
            pass

async def job_ask_arrival(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    row_idx = data.get("row_idx")
    if chat_id and row_idx:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Я прибув(ла)", callback_data=f"arrived:{row_idx}")]
        ])
        try:
            await context.bot.send_message(chat_id=chat_id, text="Будь ласка, підтвердіть прибуття на зміну:", reply_markup=kb)
        except Exception:
            pass


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Regex("^🟢 Почати$"), on_start_button))
    app.add_handler(MessageHandler(filters.Regex("^🏠 Меню$"), on_menu_button))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("shifts", shifts))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact_create))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_create_text))

    # Persistent JobQueue
    jobqueue_load_all(app)
    print(">>> Persistent JobQueue loaded")

    # ---------- WEBHOOK ----------
    port = int(os.getenv("PORT", "8000"))

    webhook_path = TELEGRAM_TOKEN
    webhook_url = f"{WEBHOOK_HOST}/{webhook_path}"

    print("Bot is running (webhook mode)...")
    print(f">>> WEBHOOK_URL = {webhook_url}")
    print(f">>> PORT = {port}")

    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=webhook_path,
        webhook_url=webhook_url
    )


# <<< ЦЕЙ БЛОК ОБОВʼЯЗКОВИЙ — залишаємо в самому низу >>>
if __name__ == "__main__":
    main()













