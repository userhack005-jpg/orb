import os
import re
import copy
import json
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

# ================= CONFIGURATION =================
IST = timezone(timedelta(hours=5, minutes=30))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip().rstrip("/")

LOCAL_DB_FILE = "users_db.json"
REDIS_KEY = "orb_multiuser_bot:users_v2"

state_lock = threading.Lock()
webhook_executor = ThreadPoolExecutor(max_workers=4)

users_db = {}
db_loaded = False
redis_status = "not_configured"
last_tv_poll = "not_yet"
monitor_started = False
latest_prices = {}

# ================= UPSTASH REDIS STORAGE (ZERO LOCK DURING NETWORK I/O) =================
def _is_valid_rest_url(url):
    return url.startswith("https://") and ":6379" not in url

def _redis_command(cmd_list):
    global redis_status
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        redis_status = "not_configured (using local JSON)"
        return None
    if not _is_valid_rest_url(UPSTASH_REDIS_REST_URL):
        redis_status = "error: use https:// REST URL (not :6379)"
        return None
    try:
        resp = requests.post(
            UPSTASH_REDIS_REST_URL,
            headers={
                "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
                "Content-Type": "application/json"
            },
            json=cmd_list,
            timeout=6
        )
        if resp.status_code == 200:
            redis_status = "connected"
            return resp.json().get("result")
        redis_status = f"http_{resp.status_code}"
        print(f"[DB] Upstash HTTP {resp.status_code}: {resp.text[:120]}", flush=True)
    except Exception as e:
        redis_status = f"error: {type(e).__name__}"
        print(f"[DB] Upstash error: {e}", flush=True)
    return None

def init_storage():
    global users_db, db_loaded
    loaded = None

    raw = _redis_command(["GET", REDIS_KEY])
    if isinstance(raw, str) and raw.strip():
        try:
            loaded = json.loads(raw)
        except Exception as e:
            print(f"[DB] Redis JSON parse error: {e}", flush=True)

    if loaded is None and os.path.exists(LOCAL_DB_FILE):
        try:
            with open(LOCAL_DB_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except Exception:
            loaded = {}

    with state_lock:
        if isinstance(loaded, dict):
            users_db = loaded
        db_loaded = True
    print(f"[DB] Loaded {len(users_db)} users. Storage: {redis_status}", flush=True)

def persist_users_async():
    """Takes a fast RAM snapshot and saves to Upstash/local without blocking any request."""
    with state_lock:
        snapshot = json.dumps(users_db)

    def _writer(raw_str):
        try:
            tmp = LOCAL_DB_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(raw_str)
            os.replace(tmp, LOCAL_DB_FILE)
        except Exception as e:
            print(f"[DB] Local write error: {e}", flush=True)
        _redis_command(["SET", REDIS_KEY, raw_str])

    threading.Thread(target=_writer, args=(snapshot,), daemon=True).start()

def default_user_profile(chat_id):
    return {
        "chat_id": str(chat_id),
        "symbol": "OANDA:XAUUSD",
        "active": True,
        "max_retests": 4,
        "alerts": {
            "range_marked": True,
            "breakout_close": True,
            "retest_entry": True
        },
        "ranges": [
            {"title": "Asian Open", "time": "01:00"},
            {"title": "London Open", "time": "12:00"}
        ],
        "ui_step": None,
        "ui_temp": {},
        "runtime": {
            "active_instance_id": None,
            "active_range_title": "-",
            "active_range_time": "-",
            "state": "WAITING_FOR_RANGE",
            "range_high": None,
            "range_low": None,
            "retest_count": 0,
            "active_slot": None,
            "last_slot_close_price": None,
            "retest_cooldown_until": 0.0
        }
    }

def _ensure_user_locked(chat_id):
    cid = str(chat_id)
    if cid not in users_db:
        users_db[cid] = default_user_profile(cid)
    else:
        base = default_user_profile(cid)
        for k, v in base.items():
            if k not in users_db[cid]:
                users_db[cid][k] = v
        for rk, rv in base["runtime"].items():
            if rk not in users_db[cid]["runtime"]:
                users_db[cid]["runtime"][rk] = rv
    return users_db[cid]

# ================= TELEGRAM API HELPERS =================
def tg_api(method, payload):
    if not TELEGRAM_BOT_TOKEN:
        print("[TG] Missing TELEGRAM_BOT_TOKEN", flush=True)
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=8)
        return resp.json()
    except Exception as e:
        print(f"[TG] {method} error: {e}", flush=True)
        return None

def send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_api("sendMessage", payload)

def edit_message(chat_id, message_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_api("editMessageText", payload)

def answer_callback(callback_query_id, text=""):
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    tg_api("answerCallbackQuery", payload)

def setup_bot_commands_and_webhook():
    if not TELEGRAM_BOT_TOKEN:
        return
    commands = [
        {"command": "start", "description": "Open Main Menu & Welcome"},
        {"command": "ranges", "description": "Add, Edit, or Delete Range Timings"},
        {"command": "status", "description": "View Live Price & Active Range Status"},
        {"command": "help", "description": "How to use this ORB Trading Bot"}
    ]
    tg_api("setMyCommands", {"commands": commands})
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
        res = tg_api("setWebhook", {"url": webhook_url, "drop_pending_updates": False})
        print(f"[TG] Webhook setup ({webhook_url}): {res}", flush=True)

# ================= TRADINGVIEW BATCH FETCHER =================
TV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
    "Content-Type": "application/json"
}

def _tv_scan_endpoint(endpoint, tickers):
    url = f"https://scanner.tradingview.com/{endpoint}/scan"
    payload = {
        "symbols": {"tickers": tickers},
        "columns": ["close", "bid", "ask", "open|5", "high|5", "low|5", "close|5"]
    }
    resp = requests.post(url, json=payload, headers=TV_HEADERS, timeout=8)
    if resp.status_code != 200 or not resp.text.strip():
        return {}, resp.status_code
    data = resp.json().get("data", [])
    out = {}
    for item in data:
        sym = item.get("s", "").upper()
        d = item.get("d", [])
        if len(d) >= 1:
            close_val = d[0] if len(d) > 0 else None
            bid_val = d[1] if len(d) > 1 else None
            live_tick = bid_val if bid_val is not None else close_val
            if live_tick is not None:
                out[sym] = {
                    "live": live_tick,
                    "m5_open": d[3] if len(d) > 3 and d[3] is not None else live_tick,
                    "m5_high": d[4] if len(d) > 4 and d[4] is not None else live_tick,
                    "m5_low": d[5] if len(d) > 5 and d[5] is not None else live_tick,
                    "m5_close": d[6] if len(d) > 6 and d[6] is not None else live_tick
                }
    return out, 200

def fetch_tv_batch(symbols):
    unique_syms = sorted(list(set(
        s.strip().upper().replace(" ", "").replace("/", "")
        for s in symbols if s and s.strip()
    )))
    if not unique_syms:
        return {}

    out = {}
    remaining = list(unique_syms)

    # CFD & Metals first for XAUUSD / XAGUSD, then global, forex, crypto
    for ep in ("cfd", "metals", "global", "forex", "crypto"):
        if not remaining:
            break
        try:
            res, status = _tv_scan_endpoint(ep, remaining)
            if status != 200:
                print(f"[TV] {ep}/scan HTTP {status}", flush=True)
                continue
            out.update(res)
            remaining = [s for s in remaining if s not in out]
        except Exception as e:
            print(f"[TV] {ep}/scan error: {e}", flush=True)

    for sym in remaining:
        try:
            r = requests.get(
                "https://scanner.tradingview.com/symbol",
                params={
                    "symbol": sym,
                    "fields": "close,bid,ask,open|5,high|5,low|5,close|5"
                },
                headers=TV_HEADERS,
                timeout=8
            )
            if r.status_code == 200 and r.text.strip():
                j = r.json()
                live_tick = j.get("bid") if j.get("bid") is not None else j.get("close")
                if live_tick is not None:
                    out[sym] = {
                        "live": live_tick,
                        "m5_open": j.get("open|5") if j.get("open|5") is not None else live_tick,
                        "m5_high": j.get("high|5") if j.get("high|5") is not None else live_tick,
                        "m5_low": j.get("low|5") if j.get("low|5") is not None else live_tick,
                        "m5_close": j.get("close|5") if j.get("close|5") is not None else live_tick
                    }
        except Exception as e:
            print(f"[TV] /symbol fallback error ({sym}): {e}", flush=True)

    return out

def validate_tv_symbol(symbol):
    sym = symbol.strip().upper().replace(" ", "").replace("/", "")
    res = fetch_tv_batch([sym])
    return sym in res, res.get(sym)
# ================= INLINE KEYBOARD BUILDERS =================
def main_menu_keyboard(user):
    toggle_text = "⏸️ Pause Alerts" if user.get("active", True) else "▶️ Resume Alerts"
    return {
        "inline_keyboard": [
            [{"text": f"📈 Set Symbol ({user['symbol']})", "callback_data": "menu:symbol"}],
            [{"text": f"⏰ Manage Ranges ({len(user['ranges'])})", "callback_data": "menu:ranges"}],
            [{"text": "🔔 Alerts & Max Retests", "callback_data": "menu:alerts"}],
            [{"text": "📊 Live Status", "callback_data": "menu:status"}],
            [{"text": toggle_text, "callback_data": "action:toggle_active"}],
            [{"text": "❓ Help & Guide", "callback_data": "menu:help"}]
        ]
    }

def main_menu_text(user):
    status_badge = "🟢 Active" if user.get("active", True) else "🔴 Paused"
    return (
        f"⚡ <b>ORB Multi-Range Alert Bot</b>\n\n"
        f"Welcome! Track Opening Range Breakouts (5m candle close) and instant live Retest Entries directly on Telegram.\n\n"
        f"<b>Current Setup:</b>\n"
        f"• Status: {status_badge}\n"
        f"• Symbol: <code>{user['symbol']}</code>\n"
        f"• Ranges Configured: <b>{len(user['ranges'])}</b>\n"
        f"• Max Retests / Range: <b>{user['max_retests']}</b>\n\n"
        f"Choose an option below to configure your bot:"
    )

def ranges_menu_view(user):
    ranges = sorted(user.get("ranges", []), key=lambda x: x["time"])
    lines = ["⏰ <b>Manage Sequential Ranges (IST 24-Hr)</b>\n"]
    if not ranges:
        lines.append("<i>No ranges configured yet. Tap below to add your first range.</i>")
    else:
        lines.append("Ranges run sequentially. When the next range time starts, the previous range automatically disables.\n")
        for idx, r in enumerate(ranges):
            lines.append(f"{idx+1}. <b>{r['title']}</b> — <code>{r['time']} IST</code>")

    keyboard = []
    for idx, r in enumerate(ranges):
        keyboard.append([
            {"text": f"✏️ Edit {r['time']} ({r['title'][:14]})", "callback_data": f"range:edit:{idx}"},
            {"text": "🗑️ Delete", "callback_data": f"range:del:{idx}"}
        ])
    keyboard.append([{"text": "➕ Add New Range", "callback_data": "range:add"}])
    keyboard.append([{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}])
    return "\n".join(lines), {"inline_keyboard": keyboard}

def alerts_menu_view(user):
    al = user.get("alerts", {})
    r_icon = "✅" if al.get("range_marked", True) else "❌"
    b_icon = "✅" if al.get("breakout_close", True) else "❌"
    e_icon = "✅" if al.get("retest_entry", True) else "❌"
    mx = user.get("max_retests", 4)

    text = (
        "🔔 <b>Alert Preferences & Max Retests</b>\n\n"
        "Tap any alert below to turn it ON or OFF:\n"
        f"• <b>1. Range Marked:</b> {r_icon}\n"
        f"• <b>2. Breakout Close:</b> {b_icon}\n"
        f"• <b>3. Retest Entry:</b> {e_icon}\n\n"
        f"<b>Max Retests per Range:</b> <code>{mx}</code>"
    )
    keyboard = {
        "inline_keyboard": [
            [{"text": f"{r_icon} 1. Range Marked Alert", "callback_data": "alert:toggle:range_marked"}],
            [{"text": f"{b_icon} 2. Breakout Close Alert", "callback_data": "alert:toggle:breakout_close"}],
            [{"text": f"{e_icon} 3. Retest Entry Alert", "callback_data": "alert:toggle:retest_entry"}],
            [
                {"text": "➖ Decrease", "callback_data": "retest:dec"},
                {"text": f"Max: {mx}", "callback_data": "noop"},
                {"text": "➕ Increase", "callback_data": "retest:inc"}
            ],
            [{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}]
        ]
    }
    return text, keyboard

def status_view(user):
    sym = user["symbol"].upper()
    with state_lock:
        pdata = copy.deepcopy(latest_prices.get(sym))
    if not pdata:
        fetched = fetch_tv_batch([sym])
        pdata = fetched.get(sym)

    rt = user.get("runtime", {})
    live_str = f"<code>{pdata['live']}</code>" if pdata and pdata.get("live") is not None else "<i>Fetching...</i>"
    hl_str = (
        f"<code>{rt['range_high']}</code> / <code>{rt['range_low']}</code>"
        if rt.get("range_high") is not None and rt.get("range_low") is not None
        else "<i>Not marked yet</i>"
    )
    now_str = datetime.now(IST).strftime("%H:%M:%S IST")
    status_badge = "🟢 Active" if user.get("active", True) else "🔴 Paused"

    text = (
        f"📊 <b>Live Bot Status</b>\n"
        f"<i>Updated: {now_str}</i>\n\n"
        f"• Alerts: {status_badge}\n"
        f"• Symbol: <code>{sym}</code>\n"
        f"• Live Price: {live_str}\n"
        f"• Active Range: <b>{rt.get('active_range_title', '-')}</b> ({rt.get('active_range_time', '-')})\n"
        f"• State: <code>{rt.get('state', 'WAITING')}</code>\n"
        f"• Range High / Low: {hl_str}\n"
        f"• Retests Completed: <b>{rt.get('retest_count', 0)} / {user.get('max_retests', 4)}</b>"
    )
    keyboard = {
        "inline_keyboard": [
            [{"text": "🔄 Refresh Status", "callback_data": "menu:status"}],
            [{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}]
        ]
    }
    return text, keyboard

def help_view():
    text = (
        "❓ <b>How This ORB Bot Works</b>\n\n"
        "<b>1. Set Your Symbol</b>\n"
        "Use TradingView format with exchange prefix, e.g.:\n"
        "• Gold: <code>OANDA:XAUUSD</code>\n"
        "• Silver: <code>OANDA:XAGUSD</code>\n"
        "• Bitcoin: <code>BINANCE:BTCUSDT</code>\n"
        "• EUR/USD: <code>FX:EURUSD</code>\n\n"
        "<b>2. Configure Ranges (IST 24-Hr)</b>\n"
        "Add one or more 5-minute range start times (e.g., <code>01:00</code>, <code>12:00</code>, <code>15:05</code>). "
        "The bot marks the 5-minute candle High & Low.\n"
        "• Multiple ranges switch automatically in sequence.\n"
        "• A single range stays active for 24 hours until Max Retests is reached.\n\n"
        "<b>3. Alert Cycle</b>\n"
        "• <b>Range Marked:</b> Sent when the 5m range candle closes.\n"
        "• <b>Breakout Close:</b> Sent when a subsequent 5m candle closes outside the range.\n"
        "• <b>Retest Entry:</b> Sent immediately on live price touch back to the breakout level."
    )
    keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}]]}
    return text, keyboard

# ================= SCHEDULE & ORB ENGINE =================
def get_current_schedule(now, ranges):
    if not ranges:
        return None
    sorted_ranges = sorted(ranges, key=lambda x: x["time"])
    candidates = []
    for day_offset in (-1, 0):
        base_date = (now + timedelta(days=day_offset)).date()
        for r in sorted_ranges:
            hh, mm = map(int, r["time"].split(":"))
            start_dt = datetime(base_date.year, base_date.month, base_date.day, hh, mm, 0, tzinfo=IST)
            if start_dt <= now:
                candidates.append((start_dt, r["title"], r["time"]))
    if not candidates:
        return None
    start_dt, title, time_str = candidates[-1]
    end_dt = start_dt + timedelta(minutes=5)
    instance_id = f"{start_dt.strftime('%Y-%m-%d')}_{time_str}"
    return instance_id, title, time_str, start_dt, end_dt

def process_user_orb_locked(user, data, now):
    persist = False
    alerts_out = []
    ranges = user.get("ranges", [])
    rt = user["runtime"]
    max_retests = int(user.get("max_retests", 4))
    symbol = user["symbol"].upper()

    def queue_alert(msg, alert_type):
        if user.get("alerts", {}).get(alert_type, True):
            alerts_out.append((user["chat_id"], msg))

    sched = get_current_schedule(now, ranges)
    if not sched:
        if rt.get("state") != "NO_RANGES":
            rt["state"] = "NO_RANGES"
            persist = True
        return persist, alerts_out

    instance_id, title, time_str, target_start, target_end = sched

    if rt.get("active_instance_id") != instance_id:
        rt["active_instance_id"] = instance_id
        rt["active_range_title"] = title
        rt["active_range_time"] = time_str
        rt["range_high"] = None
        rt["range_low"] = None
        rt["retest_count"] = 0
        rt["active_slot"] = None
        rt["last_slot_close_price"] = None
        rt["retest_cooldown_until"] = 0.0
        rt["state"] = "MARKING_RANGE" if (target_start <= now < target_end) else "MISSED_WINDOW_WAITING_NEXT"
        persist = True

    state = rt["state"]
    if state in ("DONE_FOR_RANGE", "MISSED_WINDOW_WAITING_NEXT") or not data:
        return persist, alerts_out

    # 1. Mark 5-minute range
    if state == "MARKING_RANGE":
        if now < target_end:
            rt["range_high"] = data["m5_high"]
            rt["range_low"] = data["m5_low"]
            return persist, alerts_out
        else:
            if rt["range_high"] is not None and rt["range_low"] is not None:
                rt["state"] = "SEEKING_INITIAL_BREAK"
                rt["active_slot"] = [now.hour, now.minute // 5]
                rt["last_slot_close_price"] = data["m5_close"]
                queue_alert(
                    f"📌 <b>[{title} | {time_str} IST] Range Marked</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"High: <code>{rt['range_high']}</code>\n"
                    f"Low: <code>{rt['range_low']}</code>",
                    "range_marked"
                )
            else:
                rt["state"] = "MISSED_WINDOW_WAITING_NEXT"
            return True, alerts_out

    current_slot = [now.hour, now.minute // 5]
    active_slot = rt.get("active_slot")
    last_close = rt.get("last_slot_close_price")
    r_high = rt["range_high"]
    r_low = rt["range_low"]

    # 2. 5-Minute Candle Close Detection
    if active_slot is not None and current_slot != active_slot and last_close is not None:
        breakout_triggered = False

        if state == "SEEKING_INITIAL_BREAK":
            if last_close < r_low:
                rt["state"] = "WAIT_SELL_RETEST"
                breakout_triggered = True
                queue_alert(
                    f"🔻 <b>[{title}] Close Below Range Low</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"Close: <code>{last_close}</code> &lt; Low: <code>{r_low}</code>\n"
                    f"Status: Waiting for SELL Retest...",
                    "breakout_close"
                )
            elif last_close > r_high:
                rt["state"] = "WAIT_BUY_RETEST"
                breakout_triggered = True
                queue_alert(
                    f"🔺 <b>[{title}] Close Above Range High</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"Close: <code>{last_close}</code> &gt; High: <code>{r_high}</code>\n"
                    f"Status: Waiting for BUY Retest...",
                    "breakout_close"
                )

        elif state == "IN_SELL_SEEKING_HIGH_BREAK" and last_close > r_high:
            rt["state"] = "WAIT_BUY_RETEST"
            breakout_triggered = True
            queue_alert(
                f"🔄 <b>[{title}] Range High Broken!</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Close: <code>{last_close}</code> &gt; High: <code>{r_high}</code>\n"
                f"Status: Reversal — Waiting for BUY Retest...",
                "breakout_close"
            )

        elif state == "IN_BUY_SEEKING_LOW_BREAK" and last_close < r_low:
            rt["state"] = "WAIT_SELL_RETEST"
            breakout_triggered = True
            queue_alert(
                f"🔄 <b>[{title}] Range Low Broken!</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Close: <code>{last_close}</code> &lt; Low: <code>{r_low}</code>\n"
                f"Status: Reversal — Waiting for SELL Retest...",
                "breakout_close"
            )

        rt["active_slot"] = current_slot
        rt["last_slot_close_price"] = data["m5_close"]
        if breakout_triggered:
            rt["retest_cooldown_until"] = time.time() + 8.0
            persist = True
        return persist, alerts_out

    # Keep slot close updated in RAM only (do NOT write to Redis every 3 seconds)
    rt["active_slot"] = current_slot
    rt["last_slot_close_price"] = data["m5_close"]

    # 3. Immediate Live Retest Entry Check
    if time.time() < rt.get("retest_cooldown_until", 0.0):
        return persist, alerts_out

    state = rt["state"]
    if state == "WAIT_SELL_RETEST":
        wick_touch = (now.second >= 10 and data["m5_high"] is not None and data["m5_high"] >= r_low)
        if (data["live"] is not None and data["live"] >= r_low) or wick_touch:
            rt["retest_count"] = rt.get("retest_count", 0) + 1
            cnt = rt["retest_count"]
            queue_alert(
                f"✅ <b>[{title}] SELL Entry Triggered! ({cnt}/{max_retests})</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Live Price: <code>{data['live']}</code> | Range Low: <code>{r_low}</code>",
                "retest_entry"
            )
            rt["state"] = "DONE_FOR_RANGE" if cnt >= max_retests else "IN_SELL_SEEKING_HIGH_BREAK"
            return True, alerts_out

    elif state == "WAIT_BUY_RETEST":
        wick_touch = (now.second >= 10 and data["m5_low"] is not None and data["m5_low"] <= r_high)
        if (data["live"] is not None and data["live"] <= r_high) or wick_touch:
            rt["retest_count"] = rt.get("retest_count", 0) + 1
            cnt = rt["retest_count"]
            queue_alert(
                f"✅ <b>[{title}] BUY Entry Triggered! ({cnt}/{max_retests})</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Live Price: <code>{data['live']}</code> | Range High: <code>{r_high}</code>",
                "retest_entry"
            )
            rt["state"] = "DONE_FOR_RANGE" if cnt >= max_retests else "IN_BUY_SEEKING_LOW_BREAK"
            return True, alerts_out

    return persist, alerts_out

def monitor_loop():
    global last_tv_poll
    print("🚀 Multi-User ORB Monitoring Engine Started.", flush=True)
    init_storage()
    setup_bot_commands_and_webhook()

    while True:
        try:
            with state_lock:
                unique_symbols = sorted(list(set(
                    u["symbol"].upper()
                    for u in users_db.values()
                    if u.get("active", True) and u.get("ranges")
                )))

            if not unique_symbols:
                time.sleep(5)
                continue

            batch_data = fetch_tv_batch(unique_symbols)
            now = datetime.now(IST)
            last_tv_poll = now.strftime("%H:%M:%S IST")

            should_persist = False
            needs_fast_poll = False
            alerts_to_send = []

            with state_lock:
                if batch_data:
                    latest_prices.update(batch_data)

                for u in users_db.values():
                    if not u.get("active", True) or not u.get("ranges"):
                        continue
                    sym = u["symbol"].upper()
                    data = batch_data.get(sym)
                    p_flag, u_alerts = process_user_orb_locked(u, data, now)
                    if p_flag:
                        should_persist = True
                    if u_alerts:
                        alerts_to_send.extend(u_alerts)
                    if u["runtime"].get("state") in ("MARKING_RANGE", "WAIT_SELL_RETEST", "WAIT_BUY_RETEST"):
                        needs_fast_poll = True

            # Send Telegram alerts & persist OUTSIDE state_lock
            for cid, msg_text in alerts_to_send:
                send_message(cid, msg_text)

            if should_persist:
                persist_users_async()

            time.sleep(3 if needs_fast_poll else 5)
        except Exception as e:
            print(f"[LOOP] Unexpected error: {e}", flush=True)
            time.sleep(5)

def start_monitor_once():
    global monitor_started
    with state_lock:
        if monitor_started:
            return
        monitor_started = True
    threading.Thread(target=monitor_loop, daemon=True).start()

# Start monitor 2s after boot so Gunicorn binds $PORT immediately
threading.Timer(2.0, start_monitor_once).start()

# ================= TELEGRAM WEBHOOK HANDLERS =================
def handle_message(msg):
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()

    with state_lock:
        user = _ensure_user_locked(chat_id)
        if text.startswith("/"):
            user["ui_step"] = None
            user["ui_temp"] = {}
            user_snap = copy.deepcopy(user)
            cmd = text.split()[0].lower()
            step = None
        else:
            step = user.get("ui_step")
            user_snap = copy.deepcopy(user)
            cmd = None

    if cmd:
        persist_users_async()
        if cmd == "/start":
            send_message(chat_id, main_menu_text(user_snap), main_menu_keyboard(user_snap))
        elif cmd == "/ranges":
            t, kb = ranges_menu_view(user_snap)
            send_message(chat_id, t, kb)
        elif cmd == "/status":
            t, kb = status_view(user_snap)
            send_message(chat_id, t, kb)
        elif cmd == "/help":
            t, kb = help_view()
            send_message(chat_id, t, kb)
        else:
            send_message(chat_id, "Unknown command. Use /start to open the Main Menu.")
        return

    if step == "AWAIT_SYMBOL":
        sym = text.upper().replace(" ", "").replace("/", "")     
        if ":" not in sym:
            send_message(
                chat_id,
                "⚠️ Please include the exchange prefix (e.g., <code>OANDA:XAUUSD</code> or <code>BINANCE:BTCUSDT</code>).\nTry again or send /start to cancel."
            )
            return
        ok, pdata = validate_tv_symbol(sym)
        if not ok:
            send_message(
                chat_id,
                f"❌ Symbol <code>{sym}</code> was not found on TradingView.\nPlease check spelling and try again (e.g., <code>OANDA:XAUUSD</code>, <code>BINANCE:BTCUSDT</code>)."
            )
            return
        with state_lock:
            user = _ensure_user_locked(chat_id)
            user["symbol"] = sym
            user["ui_step"] = None
            user["runtime"]["active_instance_id"] = None
            user_snap = copy.deepcopy(user)
        persist_users_async()
        send_message(
            chat_id,
            f"✅ Symbol updated to <code>{sym}</code> (Live: <code>{pdata['live']}</code>)!\n\n" + main_menu_text(user_snap),
            main_menu_keyboard(user_snap)
        )
        return

    elif step == "AWAIT_ADD_TITLE":
        title = text[:28] or "Range"
        with state_lock:
            user = _ensure_user_locked(chat_id)
            user["ui_temp"] = {"title": title}
            user["ui_step"] = "AWAIT_ADD_TIME"
        persist_users_async()
        send_message(
            chat_id,
            f"👍 Title set to <b>{title}</b>.\n\nNow send the <b>5-minute candle start time</b> in 24-hour IST format (<code>HH:MM</code>), e.g., <code>01:00</code>, <code>12:00</code>, or <code>15:05</code>:"
        )
        return

    elif step == "AWAIT_ADD_TIME":
        if not re.match(r"^([01]\d|2[0-3]):[0-5]\d$", text):
            send_message(chat_id, "⚠️ Invalid time format. Please send in 24-hour <code>HH:MM</code> format (e.g., <code>09:15</code> or <code>15:05</code>):")
            return
        with state_lock:
            user = _ensure_user_locked(chat_id)
            title = user.get("ui_temp", {}).get("title", "Range")
            user["ranges"].append({"title": title, "time": text})
            user["ranges"].sort(key=lambda x: x["time"])
            user["ui_step"] = None
            user["ui_temp"] = {}
            user_snap = copy.deepcopy(user)
        persist_users_async()
        t, kb = ranges_menu_view(user_snap)
        send_message(chat_id, f"✅ Range <b>{title} ({text} IST)</b> added!\n\n" + t, kb)
        return

    elif step == "AWAIT_EDIT_TITLE":
        with state_lock:
            user = _ensure_user_locked(chat_id)
            idx = user.get("ui_temp", {}).get("edit_idx")
            if idx is None or idx >= len(user["ranges"]):
                user["ui_step"] = None
                user_snap = copy.deepcopy(user)
                found = False
            else:
                if text != "-":
                    user["ranges"][idx]["title"] = text[:28]
                user["ui_step"] = "AWAIT_EDIT_TIME"
                cur_time = user["ranges"][idx]["time"]
                found = True
        persist_users_async()
        if not found:
            send_message(chat_id, "⚠️ Range not found. Returning to menu.", main_menu_keyboard(user_snap))
        else:
            send_message(
                chat_id,
                f"Now send the new start time in 24-hr IST (<code>HH:MM</code>), or send <code>-</code> to keep <code>{cur_time}</code>:"
            )
        return

    elif step == "AWAIT_EDIT_TIME":
        if text != "-" and not re.match(r"^([01]\d|2[0-3]):[0-5]\d$", text):
            send_message(chat_id, "⚠️ Invalid time format. Send <code>HH:MM</code> (e.g., <code>14:30</code>) or <code>-</code> to keep current:")
            return
        with state_lock:
            user = _ensure_user_locked(chat_id)
            idx = user.get("ui_temp", {}).get("edit_idx")
            if idx is None or idx >= len(user["ranges"]):
                user["ui_step"] = None
                user_snap = copy.deepcopy(user)
                found = False
            else:
                if text != "-":
                    user["ranges"][idx]["time"] = text
                user["ranges"].sort(key=lambda x: x["time"])
                user["ui_step"] = None
                user["ui_temp"] = {}
                user_snap = copy.deepcopy(user)
                found = True
        persist_users_async()
        if not found:
            send_message(chat_id, "⚠️ Range not found. Returning to menu.", main_menu_keyboard(user_snap))
        else:
            t, kb = ranges_menu_view(user_snap)
            send_message(chat_id, "✅ Range updated!\n\n" + t, kb)
        return

    send_message(chat_id, main_menu_text(user_snap), main_menu_keyboard(user_snap))

def handle_callback(cb):
    cb_id = cb["id"]
    chat_id = str(cb["message"]["chat"]["id"])
    msg_id = cb["message"]["message_id"]
    data = cb.get("data", "")

    if data == "noop":
        answer_callback(cb_id)
        return

    should_persist = False
    cb_toast = ""
    out_text = None
    out_kb = None

    with state_lock:
        user = _ensure_user_locked(chat_id)

        if data == "menu:main":
            user["ui_step"] = None
            user["ui_temp"] = {}
            should_persist = True
            user_snap = copy.deepcopy(user)
            out_text, out_kb = main_menu_text(user_snap), main_menu_keyboard(user_snap)

        elif data == "menu:symbol":
            user["ui_step"] = "AWAIT_SYMBOL"
            should_persist = True
            sym = user["symbol"]
            out_text = (
                f"📈 <b>Set TradingView Symbol</b>\n\n"
                f"Current Symbol: <code>{sym}</code>\n\n"
                f"Please reply with your TradingView ticker (with exchange prefix):\n"
                f"• Gold: <code>OANDA:XAUUSD</code>\n"
                f"• Silver: <code>OANDA:XAGUSD</code>\n"
                f"• Bitcoin: <code>BINANCE:BTCUSDT</code>\n"
                f"• EUR/USD: <code>FX:EURUSD</code>"
            )
            out_kb = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "menu:main"}]]}

        elif data == "menu:ranges":
            user["ui_step"] = None
            should_persist = True
            user_snap = copy.deepcopy(user)
            out_text, out_kb = ranges_menu_view(user_snap)

        elif data == "range:add":
            user["ui_step"] = "AWAIT_ADD_TITLE"
            user["ui_temp"] = {}
            should_persist = True
            out_text = "➕ <b>Add New Range</b>\n\nStep 1 of 2: Send a <b>Title</b> for this range (e.g., <code>Asian Open</code>, <code>London Open</code>, <code>NY Session</code>):"
            out_kb = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "menu:ranges"}]]}

        elif data.startswith("range:edit:"):
            idx = int(data.split(":")[2])
            if 0 <= idx < len(user["ranges"]):
                r = user["ranges"][idx]
                user["ui_step"] = "AWAIT_EDIT_TITLE"
                user["ui_temp"] = {"edit_idx": idx}
                should_persist = True
                out_text = (
                    f"✏️ <b>Edit Range: {r['title']} ({r['time']} IST)</b>\n\n"
                    f"Send a new <b>Title</b>, or send <code>-</code> to keep <b>{r['title']}</b>:"
                )
                out_kb = {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "menu:ranges"}]]}
            else:
                cb_toast = "Range not found."

        elif data.startswith("range:del:"):
            idx = int(data.split(":")[2])
            if 0 <= idx < len(user["ranges"]):
                removed = user["ranges"].pop(idx)
                should_persist = True
                user_snap = copy.deepcopy(user)
                out_text, out_kb = ranges_menu_view(user_snap)
                cb_toast = f"Deleted {removed['title']} ({removed['time']})"
            else:
                cb_toast = "Range not found."

        elif data == "menu:alerts":
            user_snap = copy.deepcopy(user)
            out_text, out_kb = alerts_menu_view(user_snap)

        elif data.startswith("alert:toggle:"):
            key = data.split(":")[2]
            if key in ("range_marked", "breakout_close", "retest_entry"):
                user["alerts"][key] = not user["alerts"].get(key, True)
                should_persist = True
                user_snap = copy.deepcopy(user)
                out_text, out_kb = alerts_menu_view(user_snap)
                cb_toast = "Updated!"

        elif data in ("retest:inc", "retest:dec"):
            mx = int(user.get("max_retests", 4))
            mx = min(20, mx + 1) if data == "retest:inc" else max(1, mx - 1)
            user["max_retests"] = mx
            should_persist = True
            user_snap = copy.deepcopy(user)
            out_text, out_kb = alerts_menu_view(user_snap)
            cb_toast = f"Max Retests: {mx}"

        elif data == "menu:status":
            user_snap = copy.deepcopy(user)
            cb_toast = "Status refreshed"

        elif data == "action:toggle_active":
            user["active"] = not user.get("active", True)
            should_persist = True
            user_snap = copy.deepcopy(user)
            out_text, out_kb = main_menu_text(user_snap), main_menu_keyboard(user_snap)
            cb_toast = "Alerts Resumed" if user["active"] else "Alerts Paused"

        elif data == "menu:help":
            out_text, out_kb = help_view()

    if data == "menu:status":
        out_text, out_kb = status_view(user_snap)

    if should_persist:
        persist_users_async()
    if out_text is not None:
        edit_message(chat_id, msg_id, out_text, out_kb)
    answer_callback(cb_id, cb_toast)

# ================= FLASK ROUTES =================
@app.route("/")
def home():
    start_monitor_once()
    with state_lock:
        total_u = len(users_db)
        active_u = sum(1 for u in users_db.values() if u.get("active", True))
        unique_s = len(set(u["symbol"].upper() for u in users_db.values() if u.get("active", True)))
    return jsonify({
        "service": "ORB Multi-User Telegram Bot",
        "status": "running",
        "redis_status": redis_status,
        "db_loaded": db_loaded,
        "total_users": total_u,
        "active_users": active_u,
        "tracked_unique_symbols": unique_s,
        "last_tv_poll": last_tv_poll,
        "server_time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    })

@app.route("/health")
def health():
    return "OK", 200

@app.route("/set_webhook")
def manual_set_webhook():
    if not TELEGRAM_BOT_TOKEN:
        return jsonify({"ok": False, "error": "TELEGRAM_BOT_TOKEN not set"}), 400
    base_url = RENDER_EXTERNAL_URL or request.url_root.rstrip("/").replace("http://", "https://")
    webhook_url = f"{base_url}/webhook"
    res = tg_api("setWebhook", {"url": webhook_url, "drop_pending_updates": False})
    commands = [
        {"command": "start", "description": "Open Main Menu & Welcome"},
        {"command": "ranges", "description": "Add, Edit, or Delete Range Timings"},
        {"command": "status", "description": "View Live Price & Active Range Status"},
        {"command": "help", "description": "How to use this ORB Trading Bot"}
    ]
    tg_api("setMyCommands", {"commands": commands})
    return jsonify({"webhook_url": webhook_url, "telegram_response": res})

@app.route("/webhook", methods=["POST"])
def webhook():
    start_monitor_once()
    update = request.get_json(silent=True) or {}

    def _process(upd):
        try:
            if "message" in upd and "text" in upd["message"]:
                handle_message(upd["message"])
            elif "callback_query" in upd:
                handle_callback(upd["callback_query"])
        except Exception as e:
            print(f"[WEBHOOK] Error: {e}", flush=True)

    webhook_executor.submit(_process, update)
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
