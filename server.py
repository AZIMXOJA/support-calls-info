"""
FastAPI backend для Telegram Mini App
pip install fastapi uvicorn requests
python server.py
"""

import csv
import hashlib
import hmac
import io
import json
import os
import requests
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# ══════════════════════════════════════════
#  НАСТРОЙКИ (те же что в боте)
# ══════════════════════════════════════════

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
    "5001": "🌙 Вне раб. времени",
}

REDIRECT_NUMBER = "5203"

ALLOWED_USERS = {"azim_gws", "Svetlana_Tsoy_Smartup", "bts_lily"}
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SUPPORT_NUMBERS = set(AGENT_NAMES.keys())
ANSWERED        = {"NORMAL_CLEARING", "NORMAL_UNSPECIFIED"}

# ── Auth ─────────────────────────────────

def verify_init_data(init_data: str):
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", "")
        data_check = chr(10).join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed   = hmac.new(secret_key, data_check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(computed, received_hash):
            return None
        user_data = json.loads(parsed.get("user", "{}"))
        return user_data.get("username", "")
    except Exception:
        return None

def check_access(init_data: str):
    username = verify_init_data(init_data)
    if not username:
        raise HTTPException(status_code=403, detail="Unauthorized")
    if username.lower() not in {u.lower() for u in ALLOWED_USERS}:
        raise HTTPException(status_code=403, detail="Access denied")


# Индексы колонок
C_DIR=1; C_CALLER=2; C_CALLEE=3; C_START=4
C_TALK_DUR=7; C_WAIT_DUR=8; C_RESULT=9
C_DEPT=10; C_QUALITY=11; C_CALLBACK=12

# ══════════════════════════════════════════
#  APP
# ══════════════════════════════════════════

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Helpers ──────────────────────────────

def fetch_rows():
    url = (f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
           f"/export?format=csv&gid={SHEET_GID}")
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()
    for enc in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            text = resp.content.decode(enc)
            if "UUID" in text or "uuid" in text.lower():
                break
        except:
            continue
    return list(csv.reader(io.StringIO(text)))[1:]

def parse_dt(val):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(str(val).strip(), fmt)
        except:
            continue
    return None

def safe_float(val):
    try:
        return float(str(val).replace(",", ".").strip())
    except:
        return 0.0

def agent_name(num):
    n = str(num).split(".")[0].strip()
    return AGENT_NAMES.get(n, f"#{n}")

def period_range(period: str):
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end   = today.replace(hour=23, minute=59, second=59)
    if period == "today":
        return today, end
    if period == "yesterday":
        d = today - timedelta(days=1)
        return d, d.replace(hour=23, minute=59, second=59)
    if period == "week":
        return today - timedelta(days=today.weekday()), end
    if period == "month":
        return today.replace(day=1), end
    return today, end

def analyze(rows, start, end):
    total = inc = out = 0
    missed = missed_ok = missed_fail = 0
    waits, talks = [], []
    quality     = Counter()
    inc_agents  = Counter()
    out_agents  = Counter()
    hours       = defaultdict(lambda: {"inc": 0, "out": 0, "missed": 0})
    days        = defaultdict(lambda: {"inc": 0, "out": 0, "missed": 0})
    missed_list = []
    agent_ratings = defaultdict(list)

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

        is_incoming = dept == "Support" and "сход" not in direction
        is_outgoing = "сход" in direction and caller in SUPPORT_NUMBERS

        if not (is_incoming or is_outgoing):
            continue

        result = row[C_RESULT].strip() if len(row) > C_RESULT else ""
        talk   = safe_float(row[C_TALK_DUR]) if len(row) > C_TALK_DUR else 0
        wait   = safe_float(row[C_WAIT_DUR]) if len(row) > C_WAIT_DUR else 0
        q      = int(safe_float(row[C_QUALITY])) if len(row) > C_QUALITY else 0
        cb_raw = row[C_CALLBACK].strip().upper() if len(row) > C_CALLBACK else ""
        hour   = dt.hour
        day    = dt.weekday()  # 0=Mon, 6=Sun

        total += 1
        quality[q] += 1

        if is_incoming:
            inc += 1
            hours[hour]["inc"] += 1
            days[day]["inc"] += 1
            if callee == REDIRECT_NUMBER:
                missed += 1
                hours[hour]["missed"] += 1
                days[day]["missed"] += 1
                cb_status = "✅ Перезвонили" if cb_raw == "TRUE" else "❌ Не перезвонили"
                if cb_raw == "TRUE":
                    missed_ok += 1
                elif cb_raw == "FALSE":
                    missed_fail += 1
                    missed_list.append({
                        "time": dt.strftime("%d.%m %H:%M"),
                        "caller": str(row[C_CALLER]).split(".")[0].strip() if len(row) > C_CALLER else "?",
                        "status": cb_status,
                        "called_back": cb_raw == "TRUE",
                    })
            else:
                if callee in SUPPORT_NUMBERS:
                    inc_agents[callee] += 1
                if result in ANSWERED and talk > 0:
                    waits.append(wait)
        elif is_outgoing:
            out += 1
            hours[hour]["out"] += 1
            days[day]["out"] += 1
            out_agents[caller] += 1

        if talk > 0:
            talks.append(talk)

        # Collect ratings per agent
        if q > 0:
            if is_incoming and callee in SUPPORT_NUMBERS:
                agent_ratings[callee].append(q)
            elif is_outgoing:
                agent_ratings[caller].append(q)

    avg_wait = round(sum(waits) / len(waits)) if waits else 0
    avg_talk = round(sum(talks) / len(talks)) if talks else 0
    cb_pct   = round(missed_ok / missed * 100) if missed else 0
    rated    = sum(v for k, v in quality.items() if k > 0)

    agents_data = []
    all_agents  = set(inc_agents.keys()) | set(out_agents.keys())
    for a in all_agents:
        ratings = agent_ratings.get(a, [])
        avg_r   = round(sum(ratings)/len(ratings), 1) if ratings else None
        agents_data.append({
            "name":         agent_name(a),
            "number":       a,
            "incoming":     inc_agents.get(a, 0),
            "outgoing":     out_agents.get(a, 0),
            "total":        inc_agents.get(a, 0) + out_agents.get(a, 0),
            "avg_rating":   avg_r,
            "rating_count": len(ratings),
        })
    agents_data.sort(key=lambda x: x["total"], reverse=True)

    hours_data = [{"hour": h, **hours[h]} for h in range(8, 20)]

    day_labels = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
    days_data  = [{"day": day_labels[i], "inc": days[i]["inc"], "out": days[i]["out"], "missed": days[i]["missed"]} for i in range(7)]

    # Missed by hour
    missed_hours_data = [{"hour": h, "missed": hours[h]["missed"]} for h in range(8, 20)]

    quality_data = [
        {"score": s, "label": ["","Ужасно","Плохо","Средне","Хорошо","Отлично"][s],
         "count": quality.get(s, 0)}
        for s in range(1, 6) if quality.get(s, 0) > 0
    ]

    return {
        "summary": {
            "total": total, "incoming": inc, "outgoing": out,
            "missed": missed, "missed_ok": missed_ok,
            "missed_fail": missed_fail, "cb_pct": cb_pct,
            "avg_wait": avg_wait, "avg_talk": avg_talk,
            "rated": rated, "total_rated_pool": total,
        },
        "agents":   agents_data,
        "hours":    hours_data,
        "quality":  quality_data,
        "missed_list": missed_list[:50],
        "days":         days_data,
        "missed_hours": missed_hours_data,
    }

# ── Routes ────────────────────────────────

@app.get("/api/report")
def get_report(period: str = "today", start: str = None, end: str = None, x_init_data: str = Header(default="")):
    check_access(x_init_data)
    if start and end:
        try:
            s = datetime.strptime(start, "%Y-%m-%d")
            e = datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        except:
            s, e = period_range(period)
    else:
        s, e = period_range(period)

    rows = fetch_rows()
    return analyze(rows, s, e)

@app.get("/api/compare")
def get_compare(period1: str = "this_week", period2: str = "last_week", x_init_data: str = Header(default="")):
    check_access(x_init_data)
    rows = fetch_rows()
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end   = today.replace(hour=23, minute=59, second=59)

    ranges = {
        "this_week":  (today - timedelta(days=today.weekday()), end),
        "last_week":  (today - timedelta(days=today.weekday()+7),
                       today - timedelta(days=today.weekday()+1)),
        "this_month": (today.replace(day=1), end),
        "last_month": ((today.replace(day=1) - timedelta(days=1)).replace(day=1),
                        today.replace(day=1) - timedelta(days=1)),
    }

    s1, e1 = ranges.get(period1, (today, end))
    s2, e2 = ranges.get(period2, (today, end))

    r1 = analyze(rows, s1, e1)["summary"]
    r2 = analyze(rows, s2, e2)["summary"]
    return {"period1": {"label": period1, "data": r1},
            "period2": {"label": period2, "data": r2}}

app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
